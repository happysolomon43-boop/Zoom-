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
 *   season_index 1 = most recent previous season
 *   season_index 2 = oldest previous season
 *   The scraper fetches all three directly from the Zoom API (previous=0/1/2)
 *   and writes season_index explicitly on every cycle — no dependency on the
 *   rollover RPC. The rollover RPC remains as a fallback but is not required.
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
 * previous=0 = current season, previous=1 = last season, previous=2 = oldest season.
 */
async function fetchAllResults(competitionId, previous = 0) {
  const limit = 100;
  let offset  = 0;
  const allRounds = [];

  while (true) {
    let data;
    try {
      data = await zoomGet(
        `/SeasonResult/Results?clientId=${CLIENT_ID}&competitionId=${competitionId}&previous=${previous}&offset=${offset}&limit=${limit}`
      );
    } catch (err) {
      // status=-1 means the API has no data for this previous value (season not available).
      // Treat as empty rather than an error so the caller loop stays clean.
      if (/status\s*-?1/i.test(err.message) || /no.?data/i.test(err.message)) {
        if (offset === 0) console.info(`[scraper] comp ${competitionId} previous=${previous}: no season data available.`);
        break;
      }
      throw err; // real network/parse error — propagate normally
    }
    if (!data || !data.rounds || data.rounds.length === 0) break;
    allRounds.push(...data.rounds);
    offset += data.rounds.length;
    // FIX: removed `data.total_rounds ?? data.rounds.length` fallback.
    // When the API omits total_rounds it fell back to the page size (100),
    // so offset (100) >= total (100) broke after the first page — silently
    // capping every league at 100 rounds. After ~16 hours Premier Turbo
    // crosses 100 rounds; new rounds live on page 2+ and were never fetched.
    if (data.rounds.length < limit) break;
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

/** Returns standings array for a competition (current season). */
async function fetchStandings(competitionId) {
  return zoomGet(
    `/SeasonResult/Statistics?clientId=${CLIENT_ID}&competitionId=${competitionId}`
  );
}

/**
 * Returns standings for a previous season (previous=1 → last, previous=2 → oldest).
 * Falls back to null/empty if the Statistics endpoint ignores the param.
 */
async function fetchStandingsForSeason(competitionId, previous) {
  try {
    return await zoomGet(
      `/SeasonResult/Statistics?clientId=${CLIENT_ID}&competitionId=${competitionId}&previous=${previous}`
    );
  } catch (err) {
    if (/status\s*-?1/i.test(err.message) || /no.?data/i.test(err.message)) return null;
    throw err;
  }
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
 * seasonIndex maps directly to the Zoom API "previous" param:
 *   0 = current season, 1 = last season, 2 = oldest season.
 * season_index is always written explicitly so historical rows land in the
 * right bucket regardless of DB defaults or rollover state.
 */
async function upsertMatch(match, competitionId, roundId, roundTimeStr, seasonIndex = 0) {
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
      season_index:        seasonIndex,
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
 * Upsert a historical standings row into standings_history.
 * seasonIndex: 1 = last season, 2 = oldest season.
 *
 * Requires a unique constraint on (competition_id, team_id, season_index).
 * If missing, run once in Supabase SQL editor:
 *   ALTER TABLE standings_history
 *     ADD CONSTRAINT standings_history_comp_team_season_key
 *     UNIQUE (competition_id, team_id, season_index);
 */
async function upsertStandingHistory(standing, competitionId, seasonIndex) {
  const { error } = await supabase
    .from('standings_history')
    .upsert({
      competition_id: competitionId,
      team_id:        standing.competitorId,
      season_index:   seasonIndex,
      position:       standing.position,
      points:         standing.points,
      played:         standing.played,
      wins:           standing.wins,
      draws:          standing.draws,
      losses:         standing.loses,
      goals_for:      standing.goalsFor,
      goals_against:  standing.goalsAgainst,
      form:           standing.form ?? [],
    }, { onConflict: 'competition_id,team_id,season_index' });
  if (error) throw new Error(`upsertStandingHistory(${standing.competitorId}, s${seasonIndex}): ${error.message}`);
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

/**
 * Archive all season data for one competition from one season slot to another.
 *
 * Called when a season rollover is detected: the data currently stored as
 * season_index=1 (Previous) must be moved to season_index=2 (Oldest) before
 * it gets overwritten by the incoming new previous season.
 *
 * Steps:
 *   1. Fetch and save the fresh previous=1 standings from the API into
 *      standings_history(1) — this gives us the most up-to-date snapshot
 *      of Season B's final table before it slides away from the API.
 *   2. Delete any existing season_index=toSeason rows (clean slate).
 *   3. Move season_index=fromSeason rows → season_index=toSeason.
 *
 * match_id values are unique across all seasons in virtual football, so
 * UPDATE … SET season_index=2 never causes a conflict on the matches table.
 * standings_history requires deleting toSeason rows first because of the
 * UNIQUE(competition_id, team_id, season_index) constraint.
 */
async function archiveSeasonForComp(competitionId, compName, fromSeason, toSeason) {
  // NOTE: syncStandings already ran this cycle and saved the latest previous=1
  // standings into standings_history(1). No need to snapshot again here —
  // doing so risks re-fetching after the API has already flipped to the new season.

  // Move matches fromSeason → toSeason ────────────────────────────────────
  const { error: delMatchErr } = await supabase
    .from('matches')
    .delete()
    .eq('competition_id', competitionId)
    .eq('season_index', toSeason);
  if (delMatchErr) throw new Error(`archiveSeasonForComp del matches: ${delMatchErr.message}`);

  const { error: updMatchErr } = await supabase
    .from('matches')
    .update({ season_index: toSeason, updated_at: new Date().toISOString() })
    .eq('competition_id', competitionId)
    .eq('season_index', fromSeason);
  if (updMatchErr) throw new Error(`archiveSeasonForComp upd matches: ${updMatchErr.message}`);

  // Step 2 + 3: Move standings_history fromSeason → toSeason ───────────────
  const { error: delStErr } = await supabase
    .from('standings_history')
    .delete()
    .eq('competition_id', competitionId)
    .eq('season_index', toSeason);
  if (delStErr) throw new Error(`archiveSeasonForComp del standings_history: ${delStErr.message}`);

  const { error: updStErr } = await supabase
    .from('standings_history')
    .update({ season_index: toSeason })
    .eq('competition_id', competitionId)
    .eq('season_index', fromSeason);
  if (updStErr) throw new Error(`archiveSeasonForComp upd standings_history: ${updStErr.message}`);

  console.log(`[archive] ${compName}: archived season ${fromSeason} → ${toSeason}.`);
}

/**
 * Scrape completed match results for all competitions.
 *
 * Season model (Zoom API only exposes 2 seasons — previous=0 and previous=1):
 *   season_index 0 = current season       ← previous=0 from API, refreshed every cycle
 *   season_index 1 = previous season      ← previous=1 from API
 *   season_index 2 = oldest season (Oldest tab) ← NOT on the API; lives only in our DB.
 *                    Populated by archiving season_index=1 when a new season starts.
 *
 * Rollover detection (per competition, no in-process state needed):
 *   Sample up to 20 match IDs from the incoming previous=1 response.
 *   If NONE of them exist in our DB as season_index=1 for this competition,
 *   the API has rolled to a new season. We archive the old 1→2 before writing.
 *
 * This approach is fully self-healing on every scrape cycle: process restarts,
 * partial failures, and missed cycles all recover automatically.
 */
/**
 * FIX: recentOnly=true (routine 5-min cycles) fetches only the last
 * STATS_SWEEP_ROUNDS rounds per league instead of ALL historical rounds.
 * Full history (recentOnly=false) runs once at startup and is self-healing
 * for cold starts. Re-fetching thousands of rounds every 5 min was making
 * slow cycles take many minutes and was the second cause of stale data.
 */
async function scrapeResults(competitions, recentOnly = false) {
  let totalMatches = 0;
  let totalGoals   = 0;

  for (const comp of competitions) {
    try {
      // ── Previous season (previous=1 → season_index 1) ─────────────────
      // Skip on routine cycles — previous season data doesn't change often.
      const prevRounds = recentOnly ? [] : await fetchAllResults(comp.id, 1);

      if (prevRounds.length > 0) {
        // Rollover detection: sample incoming match IDs against the DB.
        const sampleIds = prevRounds
          .flatMap(r => (r.matches ?? []).map(m => m.matchId))
          .slice(0, 20);

        // Two-part rollover check:
        // (a) None of the incoming previous=1 match IDs exist in DB season_index=1.
        // (b) DB season_index=1 already has substantial data (≥ 50 matches).
        //     This prevents a false trigger on first run / cold start / partial
        //     prior cycle — all of which would leave season_index=1 empty, causing
        //     the sample to return 0 matches even though nothing rolled over.
        //     A false trigger would delete the existing season_index=2 (Oldest) data.
        const { data: existing, error: chkErr } = await supabase
          .from('matches')
          .select('match_id')
          .in('match_id', sampleIds)
          .eq('competition_id', comp.id)
          .eq('season_index', 1)
          .limit(1);

        if (chkErr) throw chkErr;

        const { count: s1Count, error: cntErr } = await supabase
          .from('matches')
          .select('match_id', { count: 'exact', head: true })
          .eq('competition_id', comp.id)
          .eq('season_index', 1);

        if (cntErr) throw cntErr;

        const noOverlap      = sampleIds.length > 0 && (existing ?? []).length === 0;
        const hasEnoughData  = (s1Count ?? 0) >= 50; // a real full season has many more
        const isRollover     = noOverlap && hasEnoughData;

        if (isRollover) {
          // The API's previous=1 is a brand-new season — the old previous
          // season is no longer accessible via the API after this cycle.
          // Archive season_index=1 → season_index=2 NOW before overwriting.
          console.log(`[scraper] Season rollover detected for ${comp.name} — archiving 1 → 2.`);
          await archiveSeasonForComp(comp.id, comp.name, 1, 2);
        }

        // Write incoming previous=1 data as season_index=1.
        // Goal events are included — the Zoom API returns goalscorer data for
        // previous=1 too, and upsertGoalEvents uses ignoreDuplicates:true so
        // re-inserting already-known events is a harmless no-op.
        // This ensures leagues that were never rolled over (first run of this fix)
        // get full scorer+minute data for all their season-1 matches.
        for (const round of prevRounds) {
          for (const match of round.matches ?? []) {
            await upsertMatch(match, comp.id, round.id, round.time, 1);
            await upsertGoalEvents(match.matchId, match.goalscorersHome, 'home');
            await upsertGoalEvents(match.matchId, match.goalscorersAway, 'away');
            totalMatches++;
            totalGoals += (match.homeScore ?? 0) + (match.awayScore ?? 0);
          }
        }
        console.log(`[scraper] ${comp.name} season 1: ${prevRounds.length} rounds scraped.`);
      }

      // ── Current season (previous=0 → season_index 0) ──────────────────
      const currRounds = recentOnly
        ? await fetchRecentRounds(comp.id, STATS_SWEEP_ROUNDS)
        : await fetchAllResults(comp.id, 0);

      for (const round of currRounds) {
        for (const match of round.matches ?? []) {
          await upsertMatch(match, comp.id, round.id, round.time, 0);
          await upsertGoalEvents(match.matchId, match.goalscorersHome, 'home');
          await upsertGoalEvents(match.matchId, match.goalscorersAway, 'away');

          // Build H2H immediately so records are saved even when the stats
          // window closes before sweepMissingStats runs.
          await buildAndUpsertH2H(match, round.time, null).catch(err =>
            console.error(`[h2h] scrapeResults error (match ${match.matchId}): ${err.message}`)
          );

          totalMatches++;
          totalGoals += (match.homeScore ?? 0) + (match.awayScore ?? 0);
        }
      }

      if (currRounds.length > 0) {
        const latestRoundId = currRounds[0].id;
        const prevMax       = allTimeMaxRound.get(comp.id) ?? 0;
        if (latestRoundId > prevMax) allTimeMaxRound.set(comp.id, latestRoundId);
        console.log(`[scraper] ${comp.name} season 0: ${currRounds.length} rounds scraped.`);
      }

    } catch (err) {
      console.error(`[scraper] Results error (comp ${comp.id}): ${err.message}`);
    }
  }

  console.log(`[scraper] Results done — ${totalMatches} matches, ${totalGoals} goals total.`);
}

/**
 * Sync standings for all competitions.
 *
 * Current season  → `standings` table (live, always overwritten).
 * Previous season → `standings_history` season_index=1.
 *
 * NOTE: syncStandings runs BEFORE scrapeResults in runSlowCycle so that the
 * standings for the old season are captured into standings_history(1) BEFORE
 * scrapeResults potentially archives them to standings_history(2).
 *
 * Guard: if the Statistics endpoint ignores `previous` and returns current-
 * season data, the played totals will be close. Skip the write if they're
 * within 5% of each other to prevent data corruption.
 */
async function syncStandings(competitions) {
  let totalRows = 0;

  for (const comp of competitions) {
    // ── Current season → standings table ────────────────────────────────
    // Kept in its own try/catch so a transient failure or empty response
    // for the current season never blocks the history write below.
    // We track whether the fetch succeeded so the similarity guard below
    // can be skipped safely when currPlayedTotal is unreliable.
    let currPlayedTotal = 0;
    let currStandingsFetched = false;
    try {
      const currRows = await fetchStandings(comp.id);
      if (currRows && currRows.length > 0) {
        currPlayedTotal       = currRows.reduce((s, r) => s + (r.played ?? 0), 0);
        currStandingsFetched  = true;
        for (const row of currRows) {
          await upsertStanding(row, comp.id);
          totalRows++;
        }
      }
    } catch (err) {
      console.error(`[scraper] Standings error (comp ${comp.id}): ${err.message}`);
    }

    // ── Previous season → standings_history(1) ───────────────────────────
    // Runs independently of the block above so leagues whose new season
    // has just started (empty current standings) still get their history.
    //
    // syncStandings intentionally runs BEFORE scrapeResults each cycle so
    // standings_history(1) is captured before a potential 1→2 archive on
    // season rollover. The cold-start trade-off: history is one slow cycle
    // behind the first match write — acceptable, since the next 5-min tick
    // will populate it once scrapeResults has seeded season_index=1 matches.
    try {
      const prevRows = await fetchStandingsForSeason(comp.id, 1);
      if (!prevRows || prevRows.length === 0) continue;

      const prevPlayedTotal = prevRows.reduce((s, r) => s + (r.played ?? 0), 0);

      // Guard: skip if the API returned identical data to the current season.
      // Some league APIs (including Zoom-branded variants) ignore the
      // `previous` parameter and always return current-season data — writing
      // that into standings_history would corrupt the Previous season tab.
      if (currStandingsFetched && prevPlayedTotal > 0 && currPlayedTotal > 0 &&
          Math.abs(prevPlayedTotal - currPlayedTotal) / currPlayedTotal < 0.05) {

        // Query both history and confirmed season-1 match count in parallel.
        const [histResult, s1Result] = await Promise.all([
          supabase
            .from('standings_history')
            .select('competition_id', { count: 'exact', head: true })
            .eq('competition_id', comp.id)
            .eq('season_index', 1),
          supabase
            .from('matches')
            .select('match_id', { count: 'exact', head: true })
            .eq('competition_id', comp.id)
            .eq('season_index', 1),
        ]);

        const histCount   = histResult.count ?? 0;
        const s1MatchCount = s1Result.count  ?? 0;

        if (histCount > 0) {
          // History already populated and played totals look like same-season
          // data — preserving existing history, skipping overwrite.
          console.warn(
            `[scraper] ${comp.name} previous standings look same as current ` +
            `(played ${prevPlayedTotal} vs ${currPlayedTotal}) — skipping history write.`
          );
          continue;
        }

        if (s1MatchCount < 50) {
          // No history yet AND not enough season_index=1 matches to confirm a
          // real previous season. scrapeResults will write the matches this
          // cycle; the next slow cycle will have the evidence needed.
          console.info(
            `[scraper] ${comp.name} season 1 history deferred — ` +
            `${s1MatchCount} season-1 match(es) in DB, need ≥50 to confirm previous season.`
          );
          continue;
        }

        // History is empty BUT ≥50 season_index=1 matches in the DB confirm
        // a real previous season. Safe to backfill standings_history(1).
        console.info(
          `[scraper] ${comp.name} backfilling season 1 standings ` +
          `(confirmed by ${s1MatchCount} season-1 matches).`
        );
      }

      for (const row of prevRows) {
        await upsertStandingHistory(row, comp.id, 1);
        totalRows++;
      }
      console.log(`[scraper] ${comp.name} season 1 standings: ${prevRows.length} rows saved to history.`);
    } catch (err) {
      console.warn(`[scraper] ${comp.name} previous standings skipped: ${err.message}`);
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

/**
 * Scrape live matches for all competitions.
 *
 * IMPORTANT: this intentionally does NOT pre-filter competitions using the
 * `liveRound` field from /Competition/Init. That field comes from the same
 * infrequently-refreshed competitions list used for league/team sync, and
 * there's no guarantee it's updated on the same cadence as a round actually
 * going live. Gating on it silently skipped competitions whenever the field
 * was stale or unset, which was making the /live endpoint (and the app's
 * Live tab) look empty even while rounds were genuinely in progress.
 * fetchLiveResults() already returns null when nothing is live for a given
 * competition — that's the real source of truth — so every competition is
 * checked every fast cycle and the null/empty result is what determines
 * "nothing live," not a possibly-stale flag.
 */
async function scrapeLive(competitions) {
  let liveCount   = 0;
  let fetchErrors = 0;   // tracks API errors (not null/empty — those mean "nothing live")
  const seenMatchIds = new Set();

  for (const comp of competitions) {
    try {
      const liveData = await fetchLiveResults(comp.id);
      if (!liveData) continue;

      const liveMatches = Array.isArray(liveData)
        ? liveData
        : (liveData.matches ?? liveData.rounds?.flatMap(r => r.matches ?? []) ?? []);

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
      fetchErrors++;
      console.error(`[scraper] Live error (comp ${comp.id}): ${err.message}`);
    }
  }

  // Only purge stale rows when every competition fetch succeeded (or returned null = nothing live).
  // If ANY fetch threw an error we can't tell which matches are truly stale, so we leave them.
  if (fetchErrors === 0) {
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
  } else {
    console.warn(`[scraper] Live cleanup skipped — ${fetchErrors} fetch error(s). Stale rows preserved.`);
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

async function runSlowCycle(recentOnly = true) {
  if (slowInFlight) {
    console.warn('[scraper][slow] Previous cycle still running — skipping this tick.');
    return;
  }
  slowInFlight = true;
  const label = '[scraper][slow]';
  console.log(`\n${label} === Slow cycle start ${new Date().toISOString()} (recentOnly=${recentOnly}) ===`);
  try {
    cachedCompetitions = await syncLeaguesAndTeams();
    // syncStandings runs FIRST so standings_history(1) is populated before
    // scrapeResults may archive it to standings_history(2) on rollover.
    await syncStandings(cachedCompetitions);
    await scrapeResults(cachedCompetitions, recentOnly);
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

// CORS: this API is read-open by design (no auth on GET routes — see header
// comment), so allowing any origin matches the existing security model.
// DELETE routes stay protected by X-Api-Key regardless of origin.
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Api-Key');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

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
       league:leagues(id, name),
       goal_events(team_side, minute)`,
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

    // Register all recurring jobs FIRST, unconditionally — so that if the
    // initial immediate run below fails (e.g. Supabase briefly unreachable
    // during cold start), the scraper still retries automatically on its
    // next scheduled tick instead of staying dead until the next redeploy.

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

    console.log('[scraper] Cron jobs scheduled.');

    // Now attempt an immediate first run, isolated in its own try/catch.
    // If this fails, the cron jobs above are already registered and will
    // retry on their normal schedule — no permanent dead-end.
    try {
      await initRolloverTracking();
      await runSlowCycle(false); // false = full history fetch on startup
      console.log('[scraper] Initial run complete. Running on schedule...');
    } catch (err) {
      console.error(`[scraper] Initial run failed (will retry on next 5-min tick): ${err.message}`);
    }
  })();
});
