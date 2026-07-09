-- ============================================================
-- Zoom Scores — Schema Migration v2
-- Season rotation, H2H self-contained snapshots, possession sweep
--
-- Safe to re-run: IF NOT EXISTS / IF EXISTS guards throughout.
-- Apply via:
--   PGPASSWORD=$SUPABASE_DB_PASSWORD psql \
--     "postgresql://postgres.odeuukgdgdherissskgw@aws-0-eu-central-1.pooler.supabase.com:5432/postgres?sslmode=require" \
--     -f migration.sql
-- ============================================================


-- ── 1. season_index on matches ────────────────────────────────
-- 0 = current season (default, always inserted as 0)
-- 1 = most recent previous season
-- 2 = oldest previous season (gets deleted on next rollover)

ALTER TABLE matches ADD COLUMN IF NOT EXISTS season_index INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_matches_season
  ON matches(competition_id, season_index);


-- ── 2. standings_history ──────────────────────────────────────
-- Snapshot of the standings table taken right before a season
-- rolls over.  season_index 1 = recent previous, 2 = oldest.
-- The live `standings` table always reflects the current season.

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


-- ── 3. head_to_head self-contained columns ────────────────────
-- H2H records are now frozen snapshots that survive season pruning.
-- We add HT scores, possession, and corner data so the record is
-- complete without needing the original match row.

ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS ht_home_score       INT;
ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS ht_away_score       INT;
ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS possession_home     NUMERIC(5,2);
ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS possession_away     NUMERIC(5,2);
ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS home_corners        INT;
ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS away_corners        INT;
ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS corner_minutes_home JSONB;
ALTER TABLE head_to_head ADD COLUMN IF NOT EXISTS corner_minutes_away JSONB;


-- ── 4. Fix reference_match_id FK to ON DELETE SET NULL ────────
-- Without this, deleting old matches during season pruning would
-- fail with a foreign key violation.

ALTER TABLE head_to_head
  DROP CONSTRAINT IF EXISTS head_to_head_reference_match_id_fkey;

ALTER TABLE head_to_head
  ADD CONSTRAINT head_to_head_reference_match_id_fkey
  FOREIGN KEY (reference_match_id)
  REFERENCES matches(match_id)
  ON DELETE SET NULL;


-- ── Done ──────────────────────────────────────────────────────
SELECT 'Migration v2 applied successfully.' AS status;


-- ── 5. perform_season_rollover() stored procedure ─────────────
-- Atomic single-transaction rollover. Called by the scraper via
-- supabase.rpc('perform_season_rollover', { p_comp_id }).
--
-- Execution order (all in one transaction):
--   1. DELETE oldest previous (season_index=2) matches + standings_history
--   2. UPDATE matches + standings_history: 1 → 2
--   3. INSERT current standings → standings_history as season_index=1
--   4. UPDATE matches: 0 → 1  (called BEFORE new season matches are inserted)

CREATE OR REPLACE FUNCTION perform_season_rollover(p_comp_id INT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM matches           WHERE competition_id = p_comp_id AND season_index = 2;
  DELETE FROM standings_history WHERE competition_id = p_comp_id AND season_index = 2;

  UPDATE matches           SET season_index = 2 WHERE competition_id = p_comp_id AND season_index = 1;
  UPDATE standings_history SET season_index = 2 WHERE competition_id = p_comp_id AND season_index = 1;

  INSERT INTO standings_history
    (season_index, competition_id, team_id, position, points, played,
     wins, draws, losses, goals_for, goals_against, form, archived_at)
  SELECT
    1, competition_id, team_id, position, points, played,
    wins, draws, losses, goals_for, goals_against, form, NOW()
  FROM standings
  WHERE competition_id = p_comp_id
  ON CONFLICT (season_index, competition_id, team_id) DO UPDATE SET
    position      = EXCLUDED.position,
    points        = EXCLUDED.points,
    played        = EXCLUDED.played,
    wins          = EXCLUDED.wins,
    draws         = EXCLUDED.draws,
    losses        = EXCLUDED.losses,
    goals_for     = EXCLUDED.goals_for,
    goals_against = EXCLUDED.goals_against,
    form          = EXCLUDED.form,
    archived_at   = EXCLUDED.archived_at;

  UPDATE matches SET season_index = 1
    WHERE competition_id = p_comp_id AND season_index = 0;
END;
$$;
