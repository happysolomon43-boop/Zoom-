-- ============================================================
-- Zoom Scores (Bet9ja) — Supabase / PostgreSQL Schema  v2
--
-- Reflects the full schema after migration.sql has been applied.
-- Run this once on a fresh database; on an existing database run
-- migration.sql instead (it is idempotent and won't clobber data).
-- ============================================================

-- ── Helpers ─────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION attach_updated_at(tbl TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format(
    'CREATE OR REPLACE TRIGGER trg_%I_updated_at
     BEFORE UPDATE ON %I
     FOR EACH ROW EXECUTE FUNCTION set_updated_at()',
    tbl, tbl
  );
END;
$$;


-- ── leagues ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS leagues (
  id          INT          PRIMARY KEY,
  name        TEXT         NOT NULL,
  emblem      TEXT,
  live_round  INT,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

SELECT attach_updated_at('leagues');


-- ── teams ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS teams (
  id          INT          PRIMARY KEY,
  name        TEXT         NOT NULL,
  short_name  TEXT,
  emblem      TEXT,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

SELECT attach_updated_at('teams');


-- ── league_teams ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS league_teams (
  league_id   INT  NOT NULL REFERENCES leagues(id) ON DELETE CASCADE,
  team_id     INT  NOT NULL REFERENCES teams(id)   ON DELETE CASCADE,
  PRIMARY KEY (league_id, team_id)
);


-- ── matches ──────────────────────────────────────────────────
-- season_index:
--   0 = current season  (auto-updating, default for new inserts)
--   1 = most recent previous season (read-only archive)
--   2 = oldest previous season (deleted on next rollover)

CREATE TABLE IF NOT EXISTS matches (
  match_id              BIGINT       PRIMARY KEY,
  competition_id        INT          NOT NULL REFERENCES leagues(id),
  round_id              INT          NOT NULL,
  round_time            TIMESTAMPTZ,
  home_team_id          INT          NOT NULL REFERENCES teams(id),
  away_team_id          INT          NOT NULL REFERENCES teams(id),
  home_score            INT          NOT NULL DEFAULT 0,
  away_score            INT          NOT NULL DEFAULT 0,
  ht_home_score         INT          NOT NULL DEFAULT 0,
  ht_away_score         INT          NOT NULL DEFAULT 0,
  home_corners          INT                   DEFAULT 0,
  away_corners          INT                   DEFAULT 0,
  ht_home_corners       INT                   DEFAULT 0,
  ht_away_corners       INT                   DEFAULT 0,
  corner_minutes_home   JSONB,
  corner_minutes_away   JSONB,
  status                TEXT         NOT NULL DEFAULT 'completed',
  season_index          INT          NOT NULL DEFAULT 0,  -- ← v2
  created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

SELECT attach_updated_at('matches');

CREATE INDEX IF NOT EXISTS idx_matches_competition ON matches(competition_id);
CREATE INDEX IF NOT EXISTS idx_matches_home_team   ON matches(home_team_id);
CREATE INDEX IF NOT EXISTS idx_matches_away_team   ON matches(away_team_id);
CREATE INDEX IF NOT EXISTS idx_matches_round       ON matches(competition_id, round_id);
CREATE INDEX IF NOT EXISTS idx_matches_season      ON matches(competition_id, season_index);  -- ← v2


-- ── goal_events ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS goal_events (
  id           BIGSERIAL    PRIMARY KEY,
  match_id     BIGINT       NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
  team_side    TEXT         NOT NULL CHECK (team_side IN ('home','away')),
  scorer_name  TEXT         NOT NULL,
  minute       INT          NOT NULL,
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

  UNIQUE (match_id, team_side, scorer_name, minute)
);

CREATE INDEX IF NOT EXISTS idx_goal_events_match ON goal_events(match_id);


-- ── live_matches ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS live_matches (
  match_id        BIGINT       PRIMARY KEY,
  competition_id  INT          NOT NULL REFERENCES leagues(id),
  home_team_id    INT          REFERENCES teams(id),
  away_team_id    INT          REFERENCES teams(id),
  home_score      INT          NOT NULL DEFAULT 0,
  away_score      INT          NOT NULL DEFAULT 0,
  ht_home_score   INT,
  ht_away_score   INT,
  match_minute    INT,
  match_status    TEXT,
  raw_data        JSONB,
  updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

SELECT attach_updated_at('live_matches');

CREATE INDEX IF NOT EXISTS idx_live_matches_competition ON live_matches(competition_id);


-- ── standings ────────────────────────────────────────────────
-- Always reflects the CURRENT season. Archived seasons live in
-- standings_history.

CREATE TABLE IF NOT EXISTS standings (
  id              BIGSERIAL    PRIMARY KEY,
  competition_id  INT          NOT NULL REFERENCES leagues(id),
  team_id         INT          NOT NULL REFERENCES teams(id),
  position        INT          NOT NULL,
  points          INT          NOT NULL DEFAULT 0,
  played          INT          NOT NULL DEFAULT 0,
  wins            INT          NOT NULL DEFAULT 0,
  draws           INT          NOT NULL DEFAULT 0,
  losses          INT          NOT NULL DEFAULT 0,
  goals_for       INT          NOT NULL DEFAULT 0,
  goals_against   INT          NOT NULL DEFAULT 0,
  goal_diff       INT          GENERATED ALWAYS AS (goals_for - goals_against) STORED,
  form            TEXT[],
  updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

  UNIQUE (competition_id, team_id)
);

SELECT attach_updated_at('standings');

CREATE INDEX IF NOT EXISTS idx_standings_competition ON standings(competition_id, position);


-- ── standings_history ─────────────────────────────────────────
-- Snapshot taken immediately before a season rollover fires.
-- season_index 1 = most recent previous season.
-- season_index 2 = oldest previous season.
-- Both are deleted+replaced on the next rollover; nothing older is kept.

CREATE TABLE IF NOT EXISTS standings_history (
  id              BIGSERIAL    PRIMARY KEY,
  season_index    INT          NOT NULL CHECK (season_index IN (1, 2)),
  competition_id  INT          NOT NULL REFERENCES leagues(id) ON DELETE CASCADE,
  team_id         INT          NOT NULL REFERENCES teams(id)   ON DELETE CASCADE,
  position        INT          NOT NULL,
  points          INT          NOT NULL DEFAULT 0,
  played          INT          NOT NULL DEFAULT 0,
  wins            INT          NOT NULL DEFAULT 0,
  draws           INT          NOT NULL DEFAULT 0,
  losses          INT          NOT NULL DEFAULT 0,
  goals_for       INT          NOT NULL DEFAULT 0,
  goals_against   INT          NOT NULL DEFAULT 0,
  goal_diff       INT          GENERATED ALWAYS AS (goals_for - goals_against) STORED,
  form            TEXT[],
  archived_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

  UNIQUE (season_index, competition_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_standings_history_comp
  ON standings_history(competition_id, season_index, position);


-- ── match_stats ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS match_stats (
  id                      BIGSERIAL    PRIMARY KEY,
  match_id                BIGINT       NOT NULL UNIQUE REFERENCES matches(match_id) ON DELETE CASCADE,
  home_team_id            INT          REFERENCES teams(id),
  away_team_id            INT          REFERENCES teams(id),
  possession_home         NUMERIC(5,2),
  possession_away         NUMERIC(5,2),
  win_prob_home           NUMERIC(5,4),
  win_prob_away           NUMERIC(5,4),
  win_prob_draw           NUMERIC(5,4),
  total_shots             INT,
  shots_info_raw          JSONB,
  possession_timeline_raw JSONB,
  snapshot_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_match_stats_match ON match_stats(match_id);


-- ── head_to_head ─────────────────────────────────────────────
-- Self-contained frozen snapshots — survives season pruning
-- because it copies all relevant data rather than referencing
-- the match row.  Max 10 records per unique team pair (FIFO).
--
-- Pair identity for FIFO pruning:
--   LEAST(home_team_id, away_team_id) / GREATEST(home_team_id, away_team_id)
-- The scraper enforces this in application code after each insert.

CREATE TABLE IF NOT EXISTS head_to_head (
  id                  BIGSERIAL    PRIMARY KEY,
  -- Match identity
  home_team_id        INT          NOT NULL REFERENCES teams(id),
  away_team_id        INT          NOT NULL REFERENCES teams(id),
  match_start_time    TIMESTAMPTZ  NOT NULL,
  -- Scores (frozen at full-time)
  home_score          INT          NOT NULL DEFAULT 0,
  away_score          INT          NOT NULL DEFAULT 0,
  ht_home_score       INT,                             -- ← v2
  ht_away_score       INT,                             -- ← v2
  -- Goal scorers with exact minutes { "Name": [min, min, ...] }
  goalscorers_home    JSONB,
  goalscorers_away    JSONB,
  -- Possession at full-time (%)                       -- ← v2
  possession_home     NUMERIC(5,2),
  possession_away     NUMERIC(5,2),
  -- Corner totals + exact corner minutes              -- ← v2
  home_corners        INT,
  away_corners        INT,
  corner_minutes_home JSONB,
  corner_minutes_away JSONB,
  -- Back-reference (nullable: survives match deletion) -- ← v2 changed to SET NULL
  reference_match_id  BIGINT       REFERENCES matches(match_id) ON DELETE SET NULL,
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

  UNIQUE (home_team_id, away_team_id, match_start_time)
);

CREATE INDEX IF NOT EXISTS idx_h2h_teams ON head_to_head(home_team_id, away_team_id);
CREATE INDEX IF NOT EXISTS idx_h2h_ref   ON head_to_head(reference_match_id);


-- ── Done ─────────────────────────────────────────────────────
SELECT 'Schema v2 applied successfully.' AS status;
