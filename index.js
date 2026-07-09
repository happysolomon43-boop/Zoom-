/**
 * index.js — Zoom Scores REST API server  v2
 *
 * Serves data from Supabase tables populated by scraper.js.
 * Run with: node index.js
 *
 * Required env vars:
 *   SUPABASE_URL              — https://<project>.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY — service_role secret key
 *   DELETE_API_KEY            — arbitrary secret to authorise DELETE routes
 *   PORT                      — (optional) defaults to 3000
 *
 * Routes:
 *   GET  /healthz
 *
 *   GET  /leagues
 *   GET  /teams[?leagueId=]
 *
 *   GET  /matches[?leagueId=&roundId=&season=&limit=&offset=]
 *   GET  /matches/:matchId
 *   GET  /matches/:matchId/goals
 *   GET  /matches/:matchId/stats
 *
 *   GET  /live[?leagueId=]
 *
 *   GET  /standings[?leagueId=]
 *   GET  /standings/history[?leagueId=&season=]   (season: 1=recent prev, 2=oldest prev)
 *
 *   GET  /head-to-head[?homeTeamId=&awayTeamId=]
 *
 *   DELETE /matches/:matchId        (requires X-Api-Key)
 *   DELETE /live/:matchId           (requires X-Api-Key)
 *   DELETE /standings/:leagueId     (requires X-Api-Key)
 */

'use strict';

const express = require('express');
const { createClient } = require('@supabase/supabase-js');

// ── Config ─────────────────────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const DELETE_KEY   = process.env.DELETE_API_KEY;
const PORT         = parseInt(process.env.PORT ?? '3000', 10);

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

// ── Express setup ──────────────────────────────────────────────────────────────

const app = express();
app.use(express.json());
app.set('json spaces', 2);

// ── Middleware ─────────────────────────────────────────────────────────────────

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

/**
 * Parse and validate an integer query/path parameter.
 * Returns the integer, or null if the value is missing, non-numeric,
 * or outside the optional [min, max] range.
 */
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
  const { data, error } = await supabase
    .from('leagues')
    .select('*')
    .order('id');
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

/**
 * GET /matches
 *
 * Query params:
 *   leagueId  — filter by competition_id
 *   roundId   — filter by round_id
 *   season    — 0 (current, default), 1 (recent previous), 2 (oldest previous)
 *   limit     — page size, 1–500, default 50
 *   offset    — page offset, default 0
 */
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

  if (error?.code === 'PGRST116') {
    return res.status(404).json({ error: 'Stats not found for this match.' });
  }
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

/**
 * GET /standings
 * Returns the current season standings table.
 * Optional: ?leagueId= to filter by competition.
 */
app.get('/standings', asyncHandler(async (req, res) => {
  let query = supabase
    .from('standings')
    .select(
      `*,
       team:teams(id, name, short_name, emblem),
       league:leagues(id, name)`
    )
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

/**
 * GET /standings/history
 * Returns archived standings from previous seasons.
 *
 * Query params:
 *   season   — 1 (most recent previous) or 2 (oldest previous). Default: 1.
 *   leagueId — filter by competition.
 */
app.get('/standings/history', asyncHandler(async (req, res) => {
  let season = 1;
  if (req.query.season !== undefined) {
    season = parseIntParam(req.query.season, 1, 2);
    if (season === null) return badRequest(res, 'season must be 1 (recent previous) or 2 (oldest previous).');
  }

  let query = supabase
    .from('standings_history')
    .select(
      `*,
       team:teams(id, name, short_name, emblem),
       league:leagues(id, name)`
    )
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

/**
 * GET /head-to-head
 *
 * Query params (all optional — omit for all records):
 *   homeTeamId  — if both homeTeamId and awayTeamId provided, exact directional match
 *   awayTeamId  — if only one of them provided, returns all meetings involving that team
 *
 * Returns up to 10 records per matchup pair, newest first.
 * Each record is a frozen self-contained snapshot: score, HT score, goal scorers
 * with exact minutes, possession, and corner data.
 */
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
    // Both supplied — show all meetings between the pair (both orderings).
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
    // One supplied — show all meetings where this team appears as either side.
    const teamId = parseIntParam(rawHome ?? rawAway, 1);
    if (teamId === null) {
      return badRequest(res, 'homeTeamId/awayTeamId must be a positive integer.');
    }
    query = query.or(`home_team_id.eq.${teamId},away_team_id.eq.${teamId}`);
  }

  const { data, error } = await query;
  send(res, { count: data?.length ?? 0, records: data }, error);
}));

// ── Routes: DELETE (protected) ─────────────────────────────────────────────────

app.delete('/matches/:matchId', requireApiKey, asyncHandler(async (req, res) => {
  const matchId = parseIntParam(req.params.matchId, 1);
  if (matchId === null) return badRequest(res, 'matchId must be a positive integer.');

  const { error } = await supabase
    .from('matches')
    .delete()
    .eq('match_id', matchId);

  if (error) return send(res, null, error);
  res.status(200).json({ deleted: true, match_id: matchId });
}));

app.delete('/live/:matchId', requireApiKey, asyncHandler(async (req, res) => {
  const matchId = parseIntParam(req.params.matchId, 1);
  if (matchId === null) return badRequest(res, 'matchId must be a positive integer.');

  const { error } = await supabase
    .from('live_matches')
    .delete()
    .eq('match_id', matchId);

  if (error) return send(res, null, error);
  res.status(200).json({ deleted: true, match_id: matchId });
}));

app.delete('/standings/:leagueId', requireApiKey, asyncHandler(async (req, res) => {
  const leagueId = parseIntParam(req.params.leagueId, 1);
  if (leagueId === null) return badRequest(res, 'leagueId must be a positive integer.');

  const { error, count } = await supabase
    .from('standings')
    .delete()
    .eq('competition_id', leagueId);

  if (error) return send(res, null, error);
  res.status(200).json({ deleted: true, league_id: leagueId, rows_deleted: count });
}));

// ── Error handler ──────────────────────────────────────────────────────────────

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('[server] Unhandled error:', err);
  res.status(500).json({ error: err.message ?? 'Internal server error' });
});

// ── Start ──────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`[server] Zoom Scores API v2 listening on port ${PORT}`);
  console.log(`[server] Supabase: ${SUPABASE_URL}`);
  console.log(`[server] DELETE routes: ${DELETE_KEY ? 'ENABLED' : 'DISABLED (no DELETE_API_KEY)'}`);
});
