/**
 * index.js — Zoom Scores API + Scraper  (merged, single-service v3)
 *
 * Combines what used to be two separate Render services (a Web Service
 * running index.js, and a paid Background Worker running scraper.js)
 * into one free-tier-compatible Web Service. Background Workers aren't
 * available on Render's free tier, so the scraper's cron jobs now run
 * inside this same process, alongside the Express API.
 *
 * A self-ping cron job hits this service's own public /healthz URL every
 * few minutes. Render's free tier spins a Web Service down after ~15
 * minutes without INBOUND HTTP traffic — internal cron activity alone
 * does NOT count, since it never touches Render's public edge/proxy.
 * The self-ping specifically requests the public URL (not localhost) so
 * it registers as real inbound traffic and the 15-minute timer keeps
 * resetting.
 *
 * Run with: node index.js
 *
 * Required env vars:
 *   SUPABASE_URL              — https://<project>.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY — service_role secret key
 *   DELETE_API_KEY            — arbitrary secret to authorise DELETE routes
 *   PORT                      — (optional) defaults to 3000; Render sets this automatically
 *   PUBLIC_URL                — (optional) only needed if RENDER_EXTERNAL_URL
 *                                isn't set for you (e.g. running locally, or on
 *                                a host other than Render). On Render itself,
 *                                RENDER_EXTERNAL_URL is injected automatically —
 *                                you don't need to set PUBLIC_URL there.
 *
 * Scheduling (all cron jobs run inside this one process):
 *   • Results + standings + stats sweep (scraper)  → every 5 minutes
 *   • Live matches (scraper)                       → every 30 seconds
 *   • Self-ping (keep-alive)                       → every 8 minutes
 *
 * Season rotation model:
 *   season_index 0 = current season  (always updated live)
 *   season_index 1 = most recent previous season (read-only archive)
 *   season_index 2 = oldest previous season (deleted on next rollover)
 *   Detection: standings.played drops sharply AND round_id resets to a
 *   low number — both signals must fire together to prevent false triggers.
 *
 * H2H model:
 *   Built entirely from our own matches table (not from API's sparse embed).
 *   Each record is a frozen self-contained snapshot: score, HT score, goal
 *   scorers with minutes, possession, corners. Max 10 per unique pair
 *   (FIFO — oldest evicted when 11th record would be inserted).
 *
 * Possession sweep:
 *   Every slow cycle, sweeps the last 20 rounds per competition for matches
 *   missing a match_stats row and retries /MatchStatistics for each, giving
 *   up after 2 hours (the post-full-time window has closed by then).
 *
 * Routes:
 *   GET  /healthz
 *   GET  /leagues
 *   GET  /teams[?leagueId=]
 *   GET  /matches[?leagueId=&roundId=&season=&limit=&offset=]
 *   GET  /matches/:matchId
 *   GET  /matches/:matchId/goals
 *   GET  /matches/:matchId/stats
 *   GET  /live[?leagueId=]
 *   GET  /standings[?leagueId=]
 *   GET  /standings/history[?leagueId=&season=]
 *   GET  /head-to-head[?homeTeamId=&awayTeamId=]
 *   DELETE /matches/:matchId        (requires X-Api-Key)
 *   DELETE /live/:matchId           (requires X-Api-Key)
 *   DELETE /standings/:leagueId     (requires X-Api-Key)
 */

'use strict';

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const cron = require('node-cron');

// ══════════════════════════════════════════════════════════════════════════
// ── Config ───────────────────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const DELETE_KEY   = process.env.DELETE_API_KEY;
const PORT         = parseInt(process.env.PORT ?? '3000', 10);

// Render auto-injects RENDER_EXTERNAL_URL for web services (confirmed in
// Render's own docs). PUBLIC_URL is a manual fallback for non-Render hosts
// or local runs — self-ping just does nothing if neither is set.
const SELF_PING_URL = process.env.RENDER_EXTERNAL_URL ?? process.env.PUBLIC_URL ?? null;

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

// Rollover guard thresholds — both signals must fire together.
const ROLLOVER_PLAYED_RATIO          = 0.25; // new avg < 25% of previous avg
const ROLLOVER_MIN_PREV_PLAYED       = 6;    // previous avg must have been at least 6
const ROLLOVER_ROUND_RESET_THRESHOLD = 4;    // new round_id must be ≤ this
const ROLLOVER_MIN_PREV_ROUND        = 10;   // previous max round_id must have been ≥ this

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('[server] SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.');
  process.exit(1);
}
if (!DELETE_KEY) {
  console.warn('[server] DELETE_API_KEY not set — DELETE routes are disabled.');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: rollover tracking (in-memory, initialised from DB on startup) ─
// ══════════════════════════════════════════════════════════════════════════

/** competitionId → average played count seen last cycle */
const lastKnownAvgPlayed = new Map();

/** competitionId → highest round_id ever observed across all cycles */
const allTimeMaxRound = new Map();

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: time helpers ───────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

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

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: Zoom API helpers ────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

async function zoomGet(path) {
  const url = `${ZOOM_BASE}${path}`;
  const res = await fetch(url, { headers: ZOOM_HEADERS });
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
  const res = await fetch(url, {
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
  return zoomGet(`/Match/MatchInfo?clientId=${CLIENT_ID}&matchId=${matchId}`);
}

/**
 * Returns full match statistics: possession timeline, shots, probabilities.
 * Available for completed matches for a window after full-time.
 */
async function fetchMatchStats(matchId) {
  return zoomGet(`/MatchStatistics/MatchStatistics?clientId=${CLIENT_ID}&matchId=${matchId}`);
}

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: Supabase upsert helpers ─────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

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
  const { error } = await supabase.from('live_matches').delete().eq('match_id', matchId);
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
      losses:         standing.loses, // API field is "loses" (not "losses")
      goals_for:      standing.goalsFor,
      goals_against:  standing.goalsAgainst,
      form:           standing.form ?? [],
      updated_at:     new Date().toISOString(),
    }, { onConflict: 'competition_id,team_id' });
  if (error) throw new Error(`upsertStanding(${standing.competitorId}): ${error.message}`);
}

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

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: H2H helpers ─────────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

/**
 * Build and upsert a self-contained H2H record for a match, called after
 * stats land so possession is included. Each record is a frozen snapshot
 * (score, HT score, goals, possession, corners) — not a pointer to the
 * match row, so it survives that match being deleted by season rotation.
 */
async function buildAndUpsertH2H(matchRow, roundTimeStr, poss) {
  const homeId    = matchRow.homeTeam;
  const awayId    = matchRow.awayTeam;
  const startTime = parseRoundTime(roundTimeStr);

  if (!startTime) {
    console.warn(`[h2h] Cannot build H2H for match ${matchRow.matchId}: no parseable round time.`);
    return;
  }

  const goalscorersHome = Array.isArray(matchRow.goalscorersHome) ? null : (matchRow.goalscorersHome ?? null);
  const goalscorersAway = Array.isArray(matchRow.goalscorersAway) ? null : (matchRow.goalscorersAway ?? null);

  const { error } = await supabase
    .from('head_to_head')
    .upsert({
      home_team_id:        homeId,
      away_team_id:        awayId,
      match_start_time:    startTime,
      home_score:          matchRow.homeScore ?? 0,
      away_score:          matchRow.awayScore ?? 0,
      ht_home_score:       matchRow.halfTimeHomeScore ?? 0,
      ht_away_score:       matchRow.halfTimeAwayScore ?? 0,
      goalscorers_home:    goalscorersHome,
      goalscorers_away:    goalscorersAway,
      possession_home:     poss?.possHome ?? null,
      possession_away:     poss?.possAway ?? null,
      home_corners:        matchRow.homeCornerScore ?? null,
      away_corners:        matchRow.awayCornerScore ?? null,
      corner_minutes_home: matchRow.minuteCornerHome ?? null,
      corner_minutes_away: matchRow.minuteCornerAway ?? null,
      reference_match_id:  matchRow.matchId,
    }, { onConflict: 'home_team_id,away_team_id,match_start_time' });

  if (error) throw new Error(`buildH2H(match=${matchRow.matchId}): ${error.message}`);

  await pruneH2H(homeId, awayId);
}

/**
 * Enforce max 10 H2H records per matchup pair (home/away-agnostic).
 * Deletes the oldest record(s) when the pair exceeds 10.
 */
async function pruneH2H(teamAId, teamBId) {
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
  const { error: delErr } = await supabase.from('head_to_head').delete().in('id', toDelete);
  if (delErr) console.error(`[h2h] prune delete error: ${delErr.message}`);
  else console.log(`[h2h] Pruned ${toDelete.length} old record(s) for pair (${teamAId}, ${teamBId}).`);
}

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: season rollover ─────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

/**
 * On startup, read existing standings and max round_id from the DB to
 * initialise rollover tracking. Ensures a process restart doesn't reset
 * the baseline and cause a missed-rollover or false-positive.
 */
async function initRolloverTracking() {
  console.log('[scraper] Initialising rollover tracking from DB...');

  const { data: standings } = await supabase.from('standings').select('competition_id, played');
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

/** Both signals (played drop + round reset) must fire together to avoid false positives. */
function detectSeasonRollover(compId, newAvgPlayed, latestRoundId) {
  const prevAvgPlayed = lastKnownAvgPlayed.get(compId);
  const prevMaxRound  = allTimeMaxRound.get(compId) ?? 0;

  if (prevAvgPlayed === undefined) return false; // first ever reading — no baseline yet

  const playedDropped =
    prevAvgPlayed >= ROLLOVER_MIN_PREV_PLAYED &&
    newAvgPlayed  <  prevAvgPlayed * ROLLOVER_PLAYED_RATIO;

  const roundReset =
    prevMaxRound  >= ROLLOVER_MIN_PREV_ROUND &&
    latestRoundId <= ROLLOVER_ROUND_RESET_THRESHOLD;

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
 * Execute a season rollover via the atomic Postgres stored procedure
 * `perform_season_rollover(p_comp_id)` — single transaction, can't partially fail.
 */
async function executeSeasonRollover(compId) {
  console.log(`[rollover] Calling perform_season_rollover RPC for comp ${compId}...`);
  const { error } = await supabase.rpc('perform_season_rollover', { p_comp_id: compId });
  if (error) throw new Error(`rollover RPC(${compId}): ${error.message}`);
  console.log(`[rollover] Season rollover complete for competition ${compId}.`);
}

/**
 * Rollover pre-flight: runs at the START of each slow cycle, BEFORE any new
 * results/standings are written, so the archived standings come from the
 * DB's old-season final table and new-season matches land only after promotion.
 */
async function checkRolloversBeforeScrape(competitions) {
  for (const comp of competitions) {
    try {
      const peekRounds    = await fetchRecentRounds(comp.id, 1);
      const latestRoundId = peekRounds[0]?.id ?? 0;

      const standingRows = await fetchStandings(comp.id);
      if (!standingRows || standingRows.length === 0) {
        const prevMax = allTimeMaxRound.get(comp.id) ?? 0;
        if (latestRoundId > prevMax) allTimeMaxRound.set(comp.id, latestRoundId);
        continue;
      }

      const newAvgPlayed = standingRows.reduce((s, r) => s + r.played, 0) / standingRows.length;

      if (detectSeasonRollover(comp.id, newAvgPlayed, latestRoundId)) {
        await executeSeasonRollover(comp.id);
        lastKnownAvgPlayed.delete(comp.id);
        allTimeMaxRound.set(comp.id, 0);
      } else {
        lastKnownAvgPlayed.set(comp.id, newAvgPlayed);
        const prevMax = allTimeMaxRound.get(comp.id) ?? 0;
        if (latestRoundId > prevMax) allTimeMaxRound.set(comp.id, latestRoundId);
      }
    } catch (err) {
      console.error(`[scraper] Rollover check error (comp ${comp.id}): ${err.message}`);
    }
  }
}

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: scrape workflows ────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

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

/** Sync standings for all competitions — pure upsert, no rollover logic (handled separately). */
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
 * Sweep the last STATS_SWEEP_ROUNDS rounds per competition for matches still
 * missing a match_stats row, and fetch /MatchStatistics for each. Builds the
 * H2H record immediately once stats land. Matches older than STATS_GIVE_UP_MS
 * are skipped — the post-full-time window has closed and the data is gone.
 */
async function sweepMissingStats(competitions) {
  const cutoff   = new Date(Date.now() - STATS_GIVE_UP_MS).toISOString();
  let statsCount = 0;
  let h2hCount   = 0;

  for (const comp of competitions) {
    try {
      const rounds = await fetchRecentRounds(comp.id, STATS_SWEEP_ROUNDS);
      if (rounds.length === 0) continue;

      const matchIds = rounds.flatMap(r => (r.matches ?? []).map(m => m.matchId));
      if (matchIds.length === 0) continue;

      const { data: existingStats, error: checkErr } = await supabase
        .from('match_stats')
        .select('match_id')
        .in('match_id', matchIds);
      if (checkErr) throw checkErr;
      const haveStats = new Set((existingStats ?? []).map(r => r.match_id));

      const { data: storedMatches, error: matchErr } = await supabase
        .from('matches')
        .select('match_id, round_time')
        .in('match_id', matchIds)
        .eq('season_index', 0);
      if (matchErr) throw matchErr;
      const storedMatchMap = new Map((storedMatches ?? []).map(m => [m.match_id, m]));

      for (const round of [...rounds].reverse()) {
        for (const match of round.matches ?? []) {
          if (haveStats.has(match.matchId)) continue;

          const stored = storedMatchMap.get(match.matchId);
          if (!stored) continue;
          if (stored.round_time && stored.round_time < cutoff) continue;

          try {
            const stats = await fetchMatchStats(match.matchId);
            if (!stats) continue; // window not open yet — try again next cycle

            const poss = await upsertMatchStats(match.matchId, stats, match.homeTeam, match.awayTeam);
            statsCount++;

            await buildAndUpsertH2H(match, round.time, poss);
            h2hCount++;
          } catch (err) {
            console.error(`[scraper] Stats/H2H error (match ${match.matchId}): ${err.message}`);
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

  try {
    const { data: staleLive } = await supabase.from('live_matches').select('match_id');
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

// ══════════════════════════════════════════════════════════════════════════
// ── Scraper: orchestration ───────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

let cachedCompetitions = [];
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
    await checkRolloversBeforeScrape(cachedCompetitions);
    await scrapeResults(cachedCompetitions);
    await syncStandings(cachedCompetitions);
    await sweepMissingStats(cachedCompetitions);
    console.log(`${label} === Slow cycle complete ===`);
  } catch (err) {
    console.error(`${label} Unhandled error: ${err.message}`);
  } finally {
    slowInFlight = false;
  }
}

async function runFastCycle() {
  if (cachedCompetitions.length === 0) return;
  if (fastInFlight) return;
  fastInFlight = true;
  try {
    const fresh = await fetchCompetitions();
    if (fresh?.competitions) cachedCompetitions = fresh.competitions;
    await scrapeLive(cachedCompetitions);
  } catch (err) {
    console.error(`[scraper][fast] Error: ${err.message}`);
  } finally {
    fastInFlight = false;
  }
}

/**
 * Self-ping: requests this service's own public /healthz URL. Must use the
 * PUBLIC url (not localhost) so the request actually passes through Render's
 * edge/proxy and counts as inbound traffic for spin-down purposes.
 */
async function selfPing() {
  if (!SELF_PING_URL) return; // no public URL known (local dev, non-Render host) — nothing to do
  try {
    const res = await fetch(`${SELF_PING_URL}/healthz`);
    console.log(`[keepalive] Self-ping ${res.status} at ${new Date().toISOString()}`);
  } catch (err) {
    console.error(`[keepalive] Self-ping failed: ${err.message}`);
  }
}

// ══════════════════════════════════════════════════════════════════════════
// ── Express setup ────────────────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

const app = express();
app.use(express.json());
app.set('json spaces', 2);

function asyncHandler(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function requireApiKey(req, res, next) {
  if (!DELETE_KEY) {
    return res.status(403).json({ error: 'DELETE_API_KEY not configured on server.' });
  }
  const key = req.headers['x-api-key'] ?? req.query.api_key;
  if (key !== DELETE_KEY) {
    return res.status(401).json({ error: 'Invalid or missing API key.' });
  }
  next();
}

function parseIntParam(val, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (val === undefined || val === null || val === '') return null;
  const n = parseInt(val, 10);
  if (!Number.isFinite(n) || n < min || n > max) return null;
  return n;
}

function badRequest(res, msg) {
  return res.status(400).json({ error: msg });
}

function send(res, data, error, status = 200) {
  if (error) {
    console.error('[server] Supabase error:', error.message);
    return res.status(500).json({ error: error.message });
  }
  res.status(status).json(data);
}

// ── Routes: health ─────────────────────────────────────────────────────────────

app.get('/healthz', (_req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

// ── Routes: leagues ────────────────────────────────────────────────────────────

app.get('/leagues', asyncHandler(async (_req, res) => {
  const { data, error } = await supabase.from('leagues').select('*').order('id');
  send(res, data, error);
}));

// ── Routes: teams ──────────────────────────────────────────────────────────────

app.get('/teams', asyncHandler(async (req, res) => {
  let query = supabase.from('teams').select('*').order('name');

  if (req.query.leagueId) {
    const leagueId = parseIntParam(req.query.leagueId, 1);
    if (leagueId === null) return badRequest(res, 'leagueId must be a positive integer.');
    const { data: lt, error: ltErr } = await supabase
      .from('league_teams')
      .select('team_id')
      .eq('league_id', leagueId);
    if (ltErr) return send(res, null, ltErr);
    query = query.in('id', lt.map(r => r.team_id));
  }

  const { data, error } = await query;
  send(res, data, error);
}));

// ── Routes: matches ────────────────────────────────────────────────────────────

app.get('/matches', asyncHandler(async (req, res) => {
  const limit  = Math.min(parseIntParam(req.query.limit, 1, 500) ?? 50, 500);
  const offset = parseIntParam(req.query.offset, 0) ?? 0;

  let season = 0;
  if (req.query.season !== undefined) {
    season = parseIntParam(req.query.season, 0, 2);
    if (season === null) return badRequest(res, 'season must be 0 (current), 1 (recent previous), or 2 (oldest previous).');
  }

  let query = supabase
    .from('matches')
    .select(
      `*,
       home_team:teams!matches_home_team_id_fkey(id, name, short_name),
       away_team:teams!matches_away_team_id_fkey(id, name, short_name),
       league:leagues(id, name)`,
      { count: 'exact' }
    )
    .eq('season_index', season)
    .order('round_time', { ascending: false })
    .range(offset, offset + limit - 1);

  if (req.query.leagueId) {
    const leagueId = parseIntParam(req.query.leagueId, 1);
    if (leagueId === null) return badRequest(res, 'leagueId must be a positive integer.');
    query = query.eq('competition_id', leagueId);
  }

  if (req.query.roundId) {
    const roundId = parseIntParam(req.query.roundId, 1);
    if (roundId === null) return badRequest(res, 'roundId must be a positive integer.');
    query = query.eq('round_id', roundId);
  }

  const { data, error, count } = await query;
  send(res, { total: count, season, offset, limit, matches: data }, error);
}));

app.get('/matches/:matchId', asyncHandler(async (req, res) => {
  const matchId = parseIntParam(req.params.matchId, 1);
  if (matchId === null) return badRequest(res, 'matchId must be a positive integer.');

  const { data, error } = await supabase
    .from('matches')
    .select(
      `*,
       home_team:teams!matches_home_team_id_fkey(id, name, short_name, emblem),
       away_team:teams!matches_away_team_id_fkey(id, name, short_name, emblem),
       league:leagues(id, name),
       goal_events(*),
       match_stats(*)`
    )
    .eq('match_id', matchId)
    .single();

  if (error?.code === 'PGRST116') return res.status(404).json({ error: 'Match not found.' });
  send(res, data, error);
}));

app.get('/matches/:matchId/goals', asyncHandler(async (req, res) => {
  const matchId = parseIntParam(req.params.matchId, 1);
  if (matchId === null) return badRequest(res, 'matchId must be a positive integer.');

  const { data, error } = await supabase
    .from('goal_events')
    .select('*')
    .eq('match_id', matchId)
    .order('minute');
  send(res, data, error);
}));

app.get('/matches/:matchId/stats', asyncHandler(async (req, res) => {
  const matchId = parseIntParam(req.params.matchId, 1);
  if (matchId === null) return badRequest(res, 'matchId must be a positive integer.');

  const { data, error } = await supabase
    .from('match_stats')
    .select('*')
    .eq('match_id', matchId)
    .single();

  if (error?.code === 'PGRST116') return res.status(404).json({ error: 'Stats not found for this match.' });
  send(res, data, error);
}));

// ── Routes: live ───────────────────────────────────────────────────────────────

app.get('/live', asyncHandler(async (req, res) => {
  let query = supabase
    .from('live_matches')
    .select(
      `*,
       home_team:teams!live_matches_home_team_id_fkey(id, name, short_name),
       away_team:teams!live_matches_away_team_id_fkey(id, name, short_name),
       league:leagues(id, name)`
    )
    .order('updated_at', { ascending: false });

  if (req.query.leagueId) {
    const leagueId = parseIntParam(req.query.leagueId, 1);
    if (leagueId === null) return badRequest(res, 'leagueId must be a positive integer.');
    query = query.eq('competition_id', leagueId);
  }

  const { data, error } = await query;
  send(res, { count: data?.length ?? 0, live_matches: data }, error);
}));

// ── Routes: standings ──────────────────────────────────────────────────────────

app.get('/standings', asyncHandler(async (req, res) => {
  let query = supabase
    .from('standings')
    .select(`*, team:teams(id, name, short_name, emblem), league:leagues(id, name)`)
    .order('competition_id')
    .order('position');

  if (req.query.leagueId) {
    const leagueId = parseIntParam(req.query.leagueId, 1);
    if (leagueId === null) return badRequest(res, 'leagueId must be a positive integer.');
    query = query.eq('competition_id', leagueId);
  }

  const { data, error } = await query;
  send(res, data, error);
}));

app.get('/standings/history', asyncHandler(async (req, res) => {
  let season = 1;
  if (req.query.season !== undefined) {
    season = parseIntParam(req.query.season, 1, 2);
    if (season === null) return badRequest(res, 'season must be 1 (recent previous) or 2 (oldest previous).');
  }

  let query = supabase
    .from('standings_history')
    .select(`*, team:teams(id, name, short_name, emblem), league:leagues(id, name)`)
    .eq('season_index', season)
    .order('competition_id')
    .order('position');

  if (req.query.leagueId) {
    const leagueId = parseIntParam(req.query.leagueId, 1);
    if (leagueId === null) return badRequest(res, 'leagueId must be a positive integer.');
    query = query.eq('competition_id', leagueId);
  }

  const { data, error } = await query;
  send(res, { season, records: data }, error);
}));

// ── Routes: head-to-head ───────────────────────────────────────────────────────

app.get('/head-to-head', asyncHandler(async (req, res) => {
  const rawHome = req.query.homeTeamId;
  const rawAway = req.query.awayTeamId;

  let query = supabase
    .from('head_to_head')
    .select(
      `*,
       home_team:teams!head_to_head_home_team_id_fkey(id, name, short_name),
       away_team:teams!head_to_head_away_team_id_fkey(id, name, short_name)`
    )
    .order('match_start_time', { ascending: false });

  if (rawHome && rawAway) {
    const homeTeamId = parseIntParam(rawHome, 1);
    const awayTeamId = parseIntParam(rawAway, 1);
    if (homeTeamId === null || awayTeamId === null) {
      return badRequest(res, 'homeTeamId and awayTeamId must be positive integers.');
    }
    query = query.or(
      `and(home_team_id.eq.${homeTeamId},away_team_id.eq.${awayTeamId}),` +
      `and(home_team_id.eq.${awayTeamId},away_team_id.eq.${homeTeamId})`
    );
  } else if (rawHome || rawAway) {
    const teamId = parseIntParam(rawHome ?? rawAway, 1);
    if (teamId === null) return badRequest(res, 'homeTeamId/awayTeamId must be a positive integer.');
    query = query.or(`home_team_id.eq.${teamId},away_team_id.eq.${teamId}`);
  }

  const { data, error } = await query;
  send(res, { count: data?.length ?? 0, records: data }, error);
}));

// ── Routes: DELETE (protected) ─────────────────────────────────────────────────

app.delete('/matches/:matchId', requireApiKey, asyncHandler(async (req, res) => {
  const matchId = parseIntParam(req.params.matchId, 1);
  if (matchId === null) return badRequest(res, 'matchId must be a positive integer.');

  const { error } = await supabase.from('matches').delete().eq('match_id', matchId);
  if (error) return send(res, null, error);
  res.status(200).json({ deleted: true, match_id: matchId });
}));

app.delete('/live/:matchId', requireApiKey, asyncHandler(async (req, res) => {
  const matchId = parseIntParam(req.params.matchId, 1);
  if (matchId === null) return badRequest(res, 'matchId must be a positive integer.');

  const { error } = await supabase.from('live_matches').delete().eq('match_id', matchId);
  if (error) return send(res, null, error);
  res.status(200).json({ deleted: true, match_id: matchId });
}));

app.delete('/standings/:leagueId', requireApiKey, asyncHandler(async (req, res) => {
  const leagueId = parseIntParam(req.params.leagueId, 1);
  if (leagueId === null) return badRequest(res, 'leagueId must be a positive integer.');

  const { error, count } = await supabase.from('standings').delete().eq('competition_id', leagueId);
  if (error) return send(res, null, error);
  res.status(200).json({ deleted: true, league_id: leagueId, rows_deleted: count });
}));

// ── Error handler ──────────────────────────────────────────────────────────────

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('[server] Unhandled error:', err);
  res.status(500).json({ error: err.message ?? 'Internal server error' });
});

// ══════════════════════════════════════════════════════════════════════════
// ── Start ────────────────────────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════

// Bind the port FIRST, before kicking off the scraper's initial cycle. The
// first full historical fetch across 9 leagues can take a while — if we
// awaited it before app.listen(), Render's deploy health check could time
// out waiting for something to answer on $PORT. Express starts serving
// immediately; the scraper's startup work happens in the background after.
app.listen(PORT, () => {
  console.log(`[server] Zoom Scores API + Scraper listening on port ${PORT}`);
  console.log(`[server] Supabase: ${SUPABASE_URL}`);
  console.log(`[server] DELETE routes: ${DELETE_KEY ? 'ENABLED' : 'DISABLED (no DELETE_API_KEY)'}`);
  console.log(`[server] Self-ping target: ${SELF_PING_URL ?? 'NONE (spin-down protection inactive — set PUBLIC_URL if not on Render)'}`);

  // Kick off the scraper (not awaited here — runs in the background).
  (async () => {
    console.log('[scraper] Starting Zoom Scores scraper...');
    await initRolloverTracking();
    await runSlowCycle();

    // Every 5 minutes: results, standings (+ rollover detection), stats sweep, H2H.
    cron.schedule('*/5 * * * *', () => {
      runSlowCycle().catch(e => console.error('[scraper][slow] Cron error:', e.message));
    });

    // Every 30 seconds: live matches (fires only when a round is active).
    cron.schedule('*/30 * * * * *', () => {
      runFastCycle().catch(e => console.error('[scraper][fast] Cron error:', e.message));
    }, { scheduled: true, timezone: 'UTC' });

    // Every 8 minutes: self-ping to prevent free-tier spin-down (well under
    // Render's 15-minute idle threshold, independent of the cycles above).
    cron.schedule('*/8 * * * *', () => {
      selfPing().catch(e => console.error('[keepalive] Cron error:', e.message));
    });

    console.log('[scraper] Cron jobs scheduled. Running...');
  })().catch(err => console.error('[scraper] Fatal init error:', err));
});
