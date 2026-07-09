/**
 * scraper.js — Zoom Scores (Bet9ja) data scraper  v2
 *
 * Polls the Zoom external API and upserts data into Supabase.
 * Run with: node scraper.js
 *
 * Required env vars:
 *   SUPABASE_URL              — https://<project>.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY — service_role secret key
 *
 * Scheduling:
 *   • Results + standings + stats sweep   → every 5 minutes  (slow cycle)
 *   • Live matches                        → every 30 seconds (fast cycle,
 *                                           only when a round is live)
 *
 * Season rotation model:
 *   season_index 0 = current season  (always updated live)
 *   season_index 1 = most recent previous season (read-only archive)
 *   season_index 2 = oldest previous season (deleted on next rollover)
 *
 *   Detection: standings.played drops sharply AND round_id resets to a
 *   low number — both signals must fire together to prevent false triggers.
 *
 * H2H model:
 *   Built entirely from our own matches table (not from API's sparse embed).
 *   Each record is a frozen self-contained snapshot: score, HT score, goal
 *   scorers with minutes, possession, corners.  Max 10 per unique pair
 *   (FIFO — oldest evicted when 11th record would be inserted).
 *   Pair identity = LEAST(teamA, teamB) / GREATEST(teamA, teamB) so
 *   home/away reversal still counts as the same matchup.
 *
 * Possession sweep:
 *   On every slow cycle, sweep the last 20 rounds per competition for
 *   matches missing a match_stats row and try fetching /MatchStatistics
 *   for each.  Keeps retrying across cycles until data arrives or the
 *   match is older than 2 hours (give up after that).
 */

'use strict';

const { createClient } = require('@supabase/supabase-js');
const cron = require('node-cron');

// ── Config ────────────────────────────────────────────────────────────────────

const ZOOM_BASE    = 'https://zoomapi.bet9ja.com/zoomexternalapi';
const CLIENT_ID    = '202';
const ZOOM_HEADERS = {
  clientId:       CLIENT_ID,
  'Content-Type': 'application/json',
};

// How many rounds back the stats sweep covers per competition per cycle.
const STATS_SWEEP_ROUNDS = 20;

// Matches older than this (ms) with no stats are abandoned (no more retries).
const STATS_GIVE_UP_MS = 2 * 60 * 60 * 1000; // 2 hours

// Rollover guard: played must be below this fraction of the previous value.
const ROLLOVER_PLAYED_RATIO = 0.25;   // new avg < 25 % of previous avg
const ROLLOVER_MIN_PREV_PLAYED = 6;   // previous avg must have been at least 6
const ROLLOVER_ROUND_RESET_THRESHOLD = 4; // new round_id must be ≤ this
const ROLLOVER_MIN_PREV_ROUND = 10;   // previous max round_id must have been ≥ this

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('[scraper] SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// ── Rollover tracking (in-memory, initialised from DB on startup) ─────────────

/** competitionId → average played count seen last cycle */
const lastKnownAvgPlayed = new Map();

/** competitionId → highest round_id ever observed across all cycles */
const allTimeMaxRound = new Map();

// (currentCycleLatestRound removed — rollover detection now runs before scrapeResults)

// ── Time helpers ──────────────────────────────────────────────────────────────

/**
 * Parse a Bet9ja round time string "DD-MM-YYYY HH:mm:ss" into an ISO timestamp.
 * Returns null if the string is malformed.
 */
function parseRoundTime(str) {
  if (!str) return null;
  const [datePart, timePart] = str.split(' ');
  if (!datePart || !timePart) return null;
  const [dd, mm, yyyy] = datePart.split('-');
  if (!dd || !mm || !yyyy) return null;
  const ts = new Date(`${yyyy}-${mm}-${dd}T${timePart}Z`);
  return Number.isNaN(ts.getTime()) ? null : ts.toISOString();
}

// ── API helpers ───────────────────────────────────────────────────────────────

async function zoomGet(path) {
  const url = `${ZOOM_BASE}${path}`;
  const res  = await fetch(url, { headers: ZOOM_HEADERS });
  if (!res.ok) throw new Error(`GET ${path} → HTTP ${res.status}`);
  const body = await res.json();
  if (body.status !== 1) {
    const code = body.error?.code;
    // -24142 = match not in progress  -24499 = no data / invalid state
    if (code === -24142 || code === -24499) return null;
    throw new Error(`GET ${path} → API error ${code}: ${body.error?.message}`);
  }
  return body.data;
}

async function zoomPost(path, bodyObj) {
  const url = `${ZOOM_BASE}${path}`;
  const res  = await fetch(url, {
    method:  'POST',
    headers: ZOOM_HEADERS,
    body:    JSON.stringify(bodyObj),
  });
  if (!res.ok) throw new Error(`POST ${path} → HTTP ${res.status}`);
  const body = await res.json();
  if (body.status !== 1) {
    const code = body.error?.code;
    if (code === -24142 || code === -24499) return null;
    throw new Error(`POST ${path} → API error ${code}: ${body.error?.message}`);
  }
  return body.data;
}

// ── API fetch functions ───────────────────────────────────────────────────────

/** Returns { competitions: [{ id, name, emblem, liveRound, competitors }] } */
async function fetchCompetitions() {
  return zoomPost('/Competition/Init', {});
}

/**
 * Fetches ALL rounds for a competition (auto-paginates).
 * Returns an array of round objects: { id, time, matches }.
 */
async function fetchAllResults(competitionId) {
  const limit = 100;
  let offset  = 0;
  const allRounds = [];

  while (true) {
    const data = await zoomGet(
      `/SeasonResult/Results?clientId=${CLIENT_ID}&competitionId=${competitionId}&previous=0&offset=${offset}&limit=${limit}`
    );
    if (!data || !data.rounds || data.rounds.length === 0) break;
    allRounds.push(...data.rounds);
    const total = data.total_rounds ?? data.rounds.length;
    offset += data.rounds.length;
    if (offset >= total || data.rounds.length < limit) break;
  }
  return allRounds;
}

/**
 * Fetches the most recent N rounds for a competition (no pagination).
 * Used for the stats sweep to avoid re-fetching the entire history.
 */
async function fetchRecentRounds(competitionId, limit = STATS_SWEEP_ROUNDS) {
  const data = await zoomGet(
    `/SeasonResult/Results?clientId=${CLIENT_ID}&competitionId=${competitionId}&previous=0&offset=0&limit=${limit}`
  );
  return data?.rounds ?? [];
}

/** Returns standings array for a competition. */
async function fetchStandings(competitionId) {
  return zoomGet(
    `/SeasonResult/Statistics?clientId=${CLIENT_ID}&competitionId=${competitionId}`
  );
}

/** Returns live round data; null if no round is currently live. */
async function fetchLiveResults(competitionId) {
  return zoomGet(
    `/SeasonResult/LiveResults?clientId=${CLIENT_ID}&competitionId=${competitionId}`
  );
}

/** Returns live match info (clock, score, events); null if not in progress. */
async function fetchMatchInfo(matchId) {
  return zoomGet(
    `/Match/MatchInfo?clientId=${CLIENT_ID}&matchId=${matchId}`
  );
}

/**
 * Returns full match statistics: possession timeline, shots, probabilities.
 * Available for completed matches for a window after full-time.
 */
async function fetchMatchStats(matchId) {
  return zoomGet(
    `/MatchStatistics/MatchStatistics?clientId=${CLIENT_ID}&matchId=${matchId}`
  );
}

// ── Supabase upsert helpers ───────────────────────────────────────────────────

async function upsertLeague(league) {
  const { error } = await supabase
    .from('leagues')
    .upsert({
      id:         league.id,
      name:       league.name,
      emblem:     league.emblem ?? null,
      live_round: league.liveRound ?? null,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'id' });
  if (error) throw new Error(`upsertLeague(${league.id}): ${error.message}`);
}

async function upsertTeam(team) {
  const { error } = await supabase
    .from('teams')
    .upsert({
      id:         team.id,
      name:       team.name,
      short_name: team.shortName ?? null,
      emblem:     team.emblem ?? null,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'id' });
  if (error) throw new Error(`upsertTeam(${team.id}): ${error.message}`);
}

async function upsertLeagueTeam(leagueId, teamId) {
  const { error } = await supabase
    .from('league_teams')
    .upsert(
      { league_id: leagueId, team_id: teamId },
      { onConflict: 'league_id,team_id', ignoreDuplicates: true }
    );
  if (error) throw new Error(`upsertLeagueTeam(${leagueId},${teamId}): ${error.message}`);
}

/**
 * Upsert a completed match row.
 * IMPORTANT: season_index is intentionally NOT included in the row object.
 *   - New inserts:  DB DEFAULT (0) is used → current season ✓
 *   - Existing rows: season_index is preserved as-is on conflict update ✓
 * This prevents re-scraping from resetting archived season_index values.
 */
async function upsertMatch(match, competitionId, roundId, roundTimeStr) {
  const { error } = await supabase
    .from('matches')
    .upsert({
      match_id:            match.matchId,
      competition_id:      competitionId,
      round_id:            roundId,
      round_time:          parseRoundTime(roundTimeStr),
      home_team_id:        match.homeTeam,
      away_team_id:        match.awayTeam,
      home_score:          match.homeScore ?? 0,
      away_score:          match.awayScore ?? 0,
      ht_home_score:       match.halfTimeHomeScore ?? 0,
      ht_away_score:       match.halfTimeAwayScore ?? 0,
      home_corners:        match.homeCornerScore ?? 0,
      away_corners:        match.awayCornerScore ?? 0,
      ht_home_corners:     match.halfTimeHomeCornerScore ?? 0,
      ht_away_corners:     match.halfTimeAwayCornerScore ?? 0,
      corner_minutes_home: match.minuteCornerHome ?? null,
      corner_minutes_away: match.minuteCornerAway ?? null,
      status:              'completed',
      updated_at:          new Date().toISOString(),
    }, { onConflict: 'match_id' });
  if (error) throw new Error(`upsertMatch(${match.matchId}): ${error.message}`);
}

/**
 * Upsert goal events from goalscorers objects.
 * Format: { "ScorerName": [minute, minute, ...] }  or []  (no goals).
 */
async function upsertGoalEvents(matchId, goalscorers, side) {
  if (!goalscorers || Array.isArray(goalscorers)) return;

  const rows = [];
  for (const [scorer, minutes] of Object.entries(goalscorers)) {
    for (const minute of minutes) {
      rows.push({ match_id: matchId, team_side: side, scorer_name: scorer, minute });
    }
  }
  if (rows.length === 0) return;

  const { error } = await supabase
    .from('goal_events')
    .upsert(rows, {
      onConflict:       'match_id,team_side,scorer_name,minute',
      ignoreDuplicates: true,
    });
  if (error) throw new Error(`upsertGoalEvents(${matchId}): ${error.message}`);
}

async function upsertLiveMatch(liveData, competitionId) {
  const { error } = await supabase
    .from('live_matches')
    .upsert({
      match_id:       liveData.matchId,
      competition_id: competitionId,
      home_team_id:   liveData.homeTeam ?? liveData.homeTeamId ?? null,
      away_team_id:   liveData.awayTeam ?? liveData.awayTeamId ?? null,
      home_score:     liveData.homeScore ?? 0,
      away_score:     liveData.awayScore ?? 0,
      ht_home_score:  liveData.halfTimeHomeScore ?? liveData.htHomeScore ?? null,
      ht_away_score:  liveData.halfTimeAwayScore ?? liveData.htAwayScore ?? null,
      match_minute:   liveData.minute ?? liveData.matchMinute ?? null,
      match_status:   liveData.status ?? 'live',
      raw_data:       liveData,
      updated_at:     new Date().toISOString(),
    }, { onConflict: 'match_id' });
  if (error) throw new Error(`upsertLiveMatch(${liveData.matchId}): ${error.message}`);
}

async function removeLiveMatch(matchId) {
  const { error } = await supabase
    .from('live_matches')
    .delete()
    .eq('match_id', matchId);
  if (error) throw new Error(`removeLiveMatch(${matchId}): ${error.message}`);
}

async function upsertStanding(standing, competitionId) {
  const { error } = await supabase
    .from('standings')
    .upsert({
      competition_id: competitionId,
      team_id:        standing.competitorId,
      position:       standing.position,
      points:         standing.points,
      played:         standing.played,
      wins:           standing.wins,
      draws:          standing.draws,
      losses:         standing.loses,  // API field is "loses" (not "losses")
      goals_for:      standing.goalsFor,
      goals_against:  standing.goalsAgainst,
      form:           standing.form ?? [],
      updated_at:     new Date().toISOString(),
    }, { onConflict: 'competition_id,team_id' });
  if (error) throw new Error(`upsertStanding(${standing.competitorId}): ${error.message}`);
}

// upsertStandingHistory removed — archival is now handled atomically inside
// the perform_season_rollover() Postgres stored procedure.

/**
 * Upsert match statistics (possession, win probabilities, shots).
 * Returns the final possession object (home/away %) or null.
 */
async function upsertMatchStats(matchId, stats, homeTeamId, awayTeamId) {
  const { matchDetails, winningProbabilities, matchTimelineInfo } = stats;

  const effectiveHomeId = homeTeamId ?? matchDetails?.homeTeamId ?? null;
  const effectiveAwayId = awayTeamId ?? matchDetails?.awayTeamId ?? null;

  const poss     = matchTimelineInfo?.ballPossession ?? {};
  const ftPoss   = poss['FULLTIME'] ?? poss[Object.keys(poss).at(-1)] ?? null;
  const possHome = ftPoss?.home ?? null;
  const possAway = ftPoss?.away ?? null;

  const shotsInfo  = matchTimelineInfo?.shotsInfo ?? {};
  const totalShots = Object.values(shotsInfo).reduce((a, b) => a + b, 0) || null;

  const { error } = await supabase
    .from('match_stats')
    .upsert({
      match_id:                matchId,
      home_team_id:            effectiveHomeId,
      away_team_id:            effectiveAwayId,
      possession_home:         possHome,
      possession_away:         possAway,
      win_prob_home:           winningProbabilities?.home ?? null,
      win_prob_away:           winningProbabilities?.away ?? null,
      win_prob_draw:           winningProbabilities?.draw ?? null,
      total_shots:             totalShots,
      shots_info_raw:          Object.keys(shotsInfo).length > 0 ? shotsInfo : null,
      possession_timeline_raw: Object.keys(poss).length > 0 ? poss : null,
      snapshot_at:             new Date().toISOString(),
    }, { onConflict: 'match_id' });
  if (error) throw new Error(`upsertMatchStats(${matchId}): ${error.message}`);

  return { possHome, possAway };
}

// ── H2H helpers ───────────────────────────────────────────────────────────────

/**
 * Build and upsert a self-contained H2H record for a match.
 * Called after we have stats for the match so possession is included.
 *
 * @param {object} matchRow   - Row from /Results (has matchId, homeTeam, awayTeam,
 *                              scores, corners, goalscorers, etc.)
 * @param {string} roundTimeStr - Round time string from API ("DD-MM-YYYY HH:mm:ss")
 * @param {object|null} poss  - { possHome, possAway } from upsertMatchStats, or null
 */
async function buildAndUpsertH2H(matchRow, roundTimeStr, poss) {
  const homeId    = matchRow.homeTeam;
  const awayId    = matchRow.awayTeam;
  const startTime = parseRoundTime(roundTimeStr);

  if (!startTime) {
    console.warn(`[h2h] Cannot build H2H for match ${matchRow.matchId}: no parseable round time.`);
    return;
  }

  const goalscorersHome = Array.isArray(matchRow.goalscorersHome)
    ? null
    : (matchRow.goalscorersHome ?? null);
  const goalscorersAway = Array.isArray(matchRow.goalscorersAway)
    ? null
    : (matchRow.goalscorersAway ?? null);

  const { error } = await supabase
    .from('head_to_head')
    .upsert({
      home_team_id:        homeId,
      away_team_id:        awayId,
      match_start_time:    startTime,
      home_score:          matchRow.homeScore   ?? 0,
      away_score:          matchRow.awayScore   ?? 0,
      ht_home_score:       matchRow.halfTimeHomeScore ?? 0,
      ht_away_score:       matchRow.halfTimeAwayScore ?? 0,
      goalscorers_home:    goalscorersHome,
      goalscorers_away:    goalscorersAway,
      possession_home:     poss?.possHome ?? null,
      possession_away:     poss?.possAway ?? null,
      home_corners:        matchRow.homeCornerScore  ?? null,
      away_corners:        matchRow.awayCornerScore  ?? null,
      corner_minutes_home: matchRow.minuteCornerHome ?? null,
      corner_minutes_away: matchRow.minuteCornerAway ?? null,
      reference_match_id:  matchRow.matchId,
    }, { onConflict: 'home_team_id,away_team_id,match_start_time' });

  if (error) throw new Error(`buildH2H(match=${matchRow.matchId}): ${error.message}`);

  // Enforce FIFO max-10 for this pair (regardless of who was home).
  await pruneH2H(homeId, awayId);
}

/**
 * Enforce max 10 H2H records per matchup pair.
 * Pair identity: LEAST(a,b) / GREATEST(a,b) — counts both home/away orderings.
 * Deletes the oldest record(s) when the pair exceeds 10.
 */
async function pruneH2H(teamAId, teamBId) {
  // Fetch all records for this pair, newest first.
  const { data: records, error } = await supabase
    .from('head_to_head')
    .select('id, match_start_time')
    .or(
      `and(home_team_id.eq.${teamAId},away_team_id.eq.${teamBId}),` +
      `and(home_team_id.eq.${teamBId},away_team_id.eq.${teamAId})`
    )
    .order('match_start_time', { ascending: false });

  if (error) {
    console.error(`[h2h] pruneH2H(${teamAId},${teamBId}): ${error.message}`);
    return;
  }
  if (!records || records.length <= 10) return;

  const toDelete = records.slice(10).map(r => r.id);
  const { error: delErr } = await supabase
    .from('head_to_head')
    .delete()
    .in('id', toDelete);
  if (delErr) console.error(`[h2h] prune delete error: ${delErr.message}`);
  else console.log(`[h2h] Pruned ${toDelete.length} old record(s) for pair (${teamAId}, ${teamBId}).`);
}

// ── Season rollover ───────────────────────────────────────────────────────────

/**
 * On startup, read existing standings and max round_id from the DB to
 * initialise the rollover tracking maps.  This ensures a scraper restart
 * doesn't reset the baseline and cause a missed-rollover or false-positive.
 */
async function initRolloverTracking() {
  console.log('[scraper] Initialising rollover tracking from DB...');

  // Average played per competition from the live standings table
  const { data: standings } = await supabase
    .from('standings')
    .select('competition_id, played');

  if (standings?.length) {
    const byComp = {};
    for (const row of standings) {
      if (!byComp[row.competition_id]) byComp[row.competition_id] = [];
      byComp[row.competition_id].push(row.played);
    }
    for (const [compId, values] of Object.entries(byComp)) {
      const avg = values.reduce((s, v) => s + v, 0) / values.length;
      lastKnownAvgPlayed.set(Number(compId), avg);
    }
  }

  // Max round_id per competition from the matches table (current season only)
  const { data: rounds } = await supabase
    .from('matches')
    .select('competition_id, round_id')
    .eq('season_index', 0)
    .order('round_id', { ascending: false });

  if (rounds?.length) {
    for (const row of rounds) {
      const cur = allTimeMaxRound.get(row.competition_id) ?? 0;
      if (row.round_id > cur) allTimeMaxRound.set(row.competition_id, row.round_id);
    }
  }

  console.log(`[scraper] Rollover tracking ready for ${allTimeMaxRound.size} competition(s).`);
}

/**
 * Determine whether a season rollover has occurred for a competition.
 * Both signals must fire simultaneously to avoid false positives.
 *
 * @param {number} compId
 * @param {number} newAvgPlayed   — average played from current standings response
 * @param {number} latestRoundId  — round_id of the most recent round in results
 * @returns {boolean}
 */
function detectSeasonRollover(compId, newAvgPlayed, latestRoundId) {
  const prevAvgPlayed = lastKnownAvgPlayed.get(compId);
  const prevMaxRound  = allTimeMaxRound.get(compId) ?? 0;

  if (prevAvgPlayed === undefined) return false; // first ever reading — no baseline yet

  const playedDropped =
    prevAvgPlayed >= ROLLOVER_MIN_PREV_PLAYED &&
    newAvgPlayed  <  prevAvgPlayed * ROLLOVER_PLAYED_RATIO;

  const roundReset =
    prevMaxRound     >= ROLLOVER_MIN_PREV_ROUND &&
    latestRoundId    <= ROLLOVER_ROUND_RESET_THRESHOLD;

  if (playedDropped && roundReset) {
    console.log(
      `[rollover] Competition ${compId}: played ${prevAvgPlayed.toFixed(1)} → ${newAvgPlayed.toFixed(1)}, ` +
      `round ${prevMaxRound} → ${latestRoundId}. Rolling over.`
    );
    return true;
  }
  return false;
}

/**
 * Execute a season rollover for one competition via the Postgres stored
 * procedure `perform_season_rollover(p_comp_id)`.
 *
 * The procedure runs in a single transaction and:
 *   1. Deletes oldest previous season (season_index=2) matches + standings_history
 *   2. Promotes recent previous (1) → oldest (2) for both tables
 *   3. Archives the CURRENT live standings table → season_index=1
 *      (reads the DB's standings, which still hold the old season's final table
 *       because we call this BEFORE syncStandings writes new data)
 *   4. Promotes current season matches (0) → recent previous (1)
 *      (called BEFORE scrapeResults writes new-season matches)
 *
 * After the RPC returns, the caller resets in-memory tracking so the
 * next cycle builds a clean baseline for the new season.
 */
async function executeSeasonRollover(compId) {
  console.log(`[rollover] Calling perform_season_rollover RPC for comp ${compId}...`);
  const { error } = await supabase.rpc('perform_season_rollover', { p_comp_id: compId });
  if (error) throw new Error(`rollover RPC(${compId}): ${error.message}`);
  console.log(`[rollover] Season rollover complete for competition ${compId}.`);
}

/**
 * Rollover pre-flight: runs at the START of each slow cycle, BEFORE any new
 * results or standings are written to the DB.
 *
 * For each competition it:
 *   1. Peeks the latest round_id from the API (single-round fetch, cheap)
 *   2. Fetches current standings from the API (to compute newAvgPlayed)
 *   3. Checks both rollover signals (played drop + round reset)
 *   4. If a rollover is detected: calls executeSeasonRollover (atomic RPC),
 *      then resets tracking so the new season starts with a clean baseline
 *   5. If no rollover: updates in-memory baselines for the next cycle
 *
 * By running before scrapeResults and syncStandings:
 *   - The archived standings come from the DB's old-season final table ✓
 *   - New-season matches (season_index=0) are inserted only after promotion ✓
 */
async function checkRolloversBeforeScrape(competitions) {
  for (const comp of competitions) {
    try {
      // Lightweight peek — one round is enough to see the current round_id.
      const peekRounds    = await fetchRecentRounds(comp.id, 1);
      const latestRoundId = peekRounds[0]?.id ?? 0;

      // Fetch standings from the API (not from DB) to get the fresh avgPlayed.
      const standingRows = await fetchStandings(comp.id);
      if (!standingRows || standingRows.length === 0) {
        // Still update allTimeMaxRound even if standings are unavailable.
        const prevMax = allTimeMaxRound.get(comp.id) ?? 0;
        if (latestRoundId > prevMax) allTimeMaxRound.set(comp.id, latestRoundId);
        continue;
      }

      const newAvgPlayed = standingRows.reduce((s, r) => s + r.played, 0) / standingRows.length;

      if (detectSeasonRollover(comp.id, newAvgPlayed, latestRoundId)) {
        // Execute the atomic rollover stored procedure BEFORE any new data is written.
        await executeSeasonRollover(comp.id);
        // Reset tracking — new season starts accumulating a fresh baseline.
        lastKnownAvgPlayed.delete(comp.id);
        allTimeMaxRound.set(comp.id, 0);
      } else {
        // Normal cycle — update baselines for next cycle's detection.
        lastKnownAvgPlayed.set(comp.id, newAvgPlayed);
        const prevMax = allTimeMaxRound.get(comp.id) ?? 0;
        if (latestRoundId > prevMax) allTimeMaxRound.set(comp.id, latestRoundId);
      }
    } catch (err) {
      console.error(`[scraper] Rollover check error (comp ${comp.id}): ${err.message}`);
    }
  }
}

// ── Scrape workflows ──────────────────────────────────────────────────────────

/** Sync leagues and teams from /Competition/Init. */
async function syncLeaguesAndTeams() {
  console.log('[scraper] Syncing leagues and teams...');
  const data = await fetchCompetitions();
  if (!data) { console.warn('[scraper] No competition data.'); return []; }

  const competitions = data.competitions ?? [];
  for (const comp of competitions) {
    await upsertLeague(comp);
    for (const team of comp.competitors ?? []) {
      await upsertTeam(team);
      await upsertLeagueTeam(comp.id, team.id);
    }
  }
  console.log(`[scraper] Synced ${competitions.length} leagues.`);
  return competitions;
}

/** Scrape completed match results for all competitions. */
async function scrapeResults(competitions) {
  let totalMatches = 0;
  let totalGoals   = 0;

  for (const comp of competitions) {
    try {
      const rounds = await fetchAllResults(comp.id);
      for (const round of rounds) {
        for (const match of round.matches ?? []) {
          await upsertMatch(match, comp.id, round.id, round.time);
          await upsertGoalEvents(match.matchId, match.goalscorersHome, 'home');
          await upsertGoalEvents(match.matchId, match.goalscorersAway, 'away');
          totalMatches++;
          totalGoals += (match.homeScore ?? 0) + (match.awayScore ?? 0);
        }
      }

      // Keep allTimeMaxRound up to date so initRolloverTracking (on restart) and
      // checkRolloversBeforeScrape (next cycle peek) both have the highest known
      // round_id.  rounds[0] is the most recent (API returns newest-first).
      if (rounds.length > 0) {
        const latestRoundId = rounds[0].id;
        const prevMax       = allTimeMaxRound.get(comp.id) ?? 0;
        if (latestRoundId > prevMax) allTimeMaxRound.set(comp.id, latestRoundId);
      }

      console.log(`[scraper] ${comp.name}: ${rounds.length} rounds scraped.`);
    } catch (err) {
      console.error(`[scraper] Results error (comp ${comp.id}): ${err.message}`);
    }
  }
  console.log(`[scraper] Results done — ${totalMatches} matches, ${totalGoals} goals total.`);
}

/**
 * Sync standings for all competitions — pure upsert, no rollover logic.
 * Rollover detection and execution happens in checkRolloversBeforeScrape()
 * which runs at the start of the slow cycle, before any writes.
 */
async function syncStandings(competitions) {
  let totalRows = 0;

  for (const comp of competitions) {
    try {
      const rows = await fetchStandings(comp.id);
      if (!rows || rows.length === 0) continue;

      for (const row of rows) {
        await upsertStanding(row, comp.id);
        totalRows++;
      }
      console.log(`[scraper] ${comp.name}: ${rows.length} standing rows upserted.`);
    } catch (err) {
      console.error(`[scraper] Standings error (comp ${comp.id}): ${err.message}`);
    }
  }
  console.log(`[scraper] Standings done — ${totalRows} rows total.`);
}

/**
 * Stats sweep: find ALL current-season matches from the last STATS_SWEEP_ROUNDS
 * rounds that are still missing a match_stats row, and fetch /MatchStatistics
 * for each.  This replaces the old "only check the most recent round" approach.
 *
 * After successfully saving stats for a match, we also build/update the H2H
 * record for that matchup (which requires possession to be available).
 *
 * Matches older than STATS_GIVE_UP_MS that still lack stats are skipped —
 * the MatchStatistics window has closed and the data is unrecoverable.
 */
async function sweepMissingStats(competitions) {
  const cutoff    = new Date(Date.now() - STATS_GIVE_UP_MS).toISOString();
  let statsCount  = 0;
  let h2hCount    = 0;

  for (const comp of competitions) {
    try {
      // Fetch recent rounds from the API (no DB dependency — avoids stale cache).
      const rounds = await fetchRecentRounds(comp.id, STATS_SWEEP_ROUNDS);
      if (rounds.length === 0) continue;

      // Collect all match IDs in these rounds.
      const matchIds = rounds.flatMap(r => (r.matches ?? []).map(m => m.matchId));
      if (matchIds.length === 0) continue;

      // Batch-check which of these already have stats in our DB.
      const { data: existingStats, error: checkErr } = await supabase
        .from('match_stats')
        .select('match_id')
        .in('match_id', matchIds);
      if (checkErr) throw checkErr;

      const haveStats = new Set((existingStats ?? []).map(r => r.match_id));

      // Also check which matches we actually have stored (some rounds may not
      // be saved yet if the initial scrape is still in progress).
      const { data: storedMatches, error: matchErr } = await supabase
        .from('matches')
        .select('match_id, round_time')
        .in('match_id', matchIds)
        .eq('season_index', 0);
      if (matchErr) throw matchErr;

      const storedMatchMap = new Map((storedMatches ?? []).map(m => [m.match_id, m]));

      // Process each missing-stats match in round order (oldest first so we
      // don't overload the API on first run).
      for (const round of [...rounds].reverse()) {
        for (const match of round.matches ?? []) {
          if (haveStats.has(match.matchId)) continue;

          // Skip if the match is not in our DB yet (results scrape still pending)
          // or if it's too old for the stats window.
          const stored = storedMatchMap.get(match.matchId);
          if (!stored) continue;
          if (stored.round_time && stored.round_time < cutoff) continue;

          try {
            const stats = await fetchMatchStats(match.matchId);
            if (!stats) continue; // API returned "no data" — try again next cycle

            // Save match stats and capture the final possession numbers.
            const poss = await upsertMatchStats(
              match.matchId,
              stats,
              match.homeTeam,
              match.awayTeam
            );
            statsCount++;

            // Build the self-contained H2H snapshot now that possession is available.
            await buildAndUpsertH2H(match, round.time, poss);
            h2hCount++;
          } catch (err) {
            console.error(
              `[scraper] Stats/H2H error (match ${match.matchId}): ${err.message}`
            );
          }
        }
      }
    } catch (err) {
      console.error(`[scraper] Stats sweep error (comp ${comp.id}): ${err.message}`);
    }
  }

  if (statsCount > 0) {
    console.log(`[scraper] Stats sweep: ${statsCount} match(es) updated, ${h2hCount} H2H record(s) built.`);
  }
}

/** Scrape live matches for all competitions that have a live round. */
async function scrapeLive(competitions) {
  const liveComps = competitions.filter(c => c.liveRound != null && c.liveRound > 0);

  let liveCount      = 0;
  const seenMatchIds = new Set();

  for (const comp of liveComps) {
    try {
      const liveData = await fetchLiveResults(comp.id);
      if (!liveData) continue;

      const liveMatches = Array.isArray(liveData)
        ? liveData
        : (liveData.matches ?? liveData.rounds?.flatMap(r => r.matches) ?? []);

      for (const lm of liveMatches) {
        seenMatchIds.add(lm.matchId);
        await upsertLiveMatch(lm, comp.id);

        // Overlay detailed match info (game clock, granular events) when available.
        try {
          const info = await fetchMatchInfo(lm.matchId);
          if (info) await upsertLiveMatch({ ...lm, ...info }, comp.id);
        } catch (_) { /* match may have ended between the two calls */ }

        liveCount++;
      }
    } catch (err) {
      console.error(`[scraper] Live error (comp ${comp.id}): ${err.message}`);
    }
  }

  // Always clean up stale live_matches rows, even when no competition is live.
  try {
    const { data: staleLive } = await supabase
      .from('live_matches')
      .select('match_id');

    for (const row of staleLive ?? []) {
      if (!seenMatchIds.has(row.match_id)) {
        await removeLiveMatch(row.match_id);
        console.log(`[scraper] Removed stale live match ${row.match_id}.`);
      }
    }
  } catch (err) {
    console.error(`[scraper] Live cleanup error: ${err.message}`);
  }

  if (liveCount > 0) {
    console.log(`[scraper] Live: ${liveCount} match(es) updated at ${new Date().toISOString()}`);
  }
}

// ── Main orchestration ────────────────────────────────────────────────────────

/** Shared competition list — refreshed by the slow cycle. */
let cachedCompetitions = [];

/** In-flight guards prevent concurrent cron ticks from overlapping. */
let slowInFlight = false;
let fastInFlight = false;

async function runSlowCycle() {
  if (slowInFlight) {
    console.warn('[scraper][slow] Previous cycle still running — skipping this tick.');
    return;
  }
  slowInFlight = true;
  const label = '[scraper][slow]';
  console.log(`\n${label} === Slow cycle start ${new Date().toISOString()} ===`);
  try {
    cachedCompetitions = await syncLeaguesAndTeams();
    await checkRolloversBeforeScrape(cachedCompetitions); // detect + execute BEFORE any writes
    await scrapeResults(cachedCompetitions);              // new-season matches go in as season_index=0
    await syncStandings(cachedCompetitions);              // new-season standings upserted
    await sweepMissingStats(cachedCompetitions);          // possession + H2H
    console.log(`${label} === Slow cycle complete ===`);
  } catch (err) {
    console.error(`${label} Unhandled error: ${err.message}`);
  } finally {
    slowInFlight = false;
  }
}

async function runFastCycle() {
  if (cachedCompetitions.length === 0) return; // wait for slow cycle
  if (fastInFlight) return;
  fastInFlight = true;
  try {
    // Re-fetch liveRound flags without the overhead of a full slow cycle.
    const fresh = await fetchCompetitions();
    if (fresh?.competitions) cachedCompetitions = fresh.competitions;
    await scrapeLive(cachedCompetitions);
  } catch (err) {
    console.error(`[scraper][fast] Error: ${err.message}`);
  } finally {
    fastInFlight = false;
  }
}

async function main() {
  console.log('[scraper] Starting Zoom Scores scraper v2...');
  console.log(`[scraper] Supabase URL: ${SUPABASE_URL}`);

  // Initialise rollover tracking from existing DB data before the first cycle.
  await initRolloverTracking();

  // Run the full slow cycle immediately on startup.
  await runSlowCycle();

  // Every 5 minutes: results, standings (+ rollover detection), stats sweep, H2H.
  cron.schedule('*/5 * * * *', () => {
    runSlowCycle().catch(e => console.error('[scraper][slow] Cron error:', e.message));
  });

  // Every 30 seconds: live matches (fires only when a round is active).
  cron.schedule('*/30 * * * * *', () => {
    runFastCycle().catch(e => console.error('[scraper][fast] Cron error:', e.message));
  }, { scheduled: true, timezone: 'UTC' });

  console.log('[scraper] Cron jobs scheduled. Running...');
}

main().catch(err => {
  console.error('[scraper] Fatal:', err);
  process.exit(1);
});
