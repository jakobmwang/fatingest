-- Milestone dates instead of phase values; the verdict and the attempts in columns of their own;
-- per-position meta on files_chunks. Additive and idempotent: run once against a live database
-- created before this date (docker exec -i fatingest-db psql -U fatingest -d fatingest < this file).
-- initdb/01-schema.sql already carries the result for new databases.
BEGIN;
ALTER TABLE files ALTER COLUMN due_at DROP NOT NULL;
ALTER TABLE files ALTER COLUMN due_at DROP DEFAULT;
ALTER TABLE files ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE files ADD COLUMN IF NOT EXISTS failed_at  TIMESTAMPTZ;
ALTER TABLE files ADD COLUMN IF NOT EXISTS attempts   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE files ADD COLUMN IF NOT EXISTS error      TEXT;
ALTER TABLE files_chunks ADD COLUMN IF NOT EXISTS meta JSONB NOT NULL DEFAULT '{}';
DROP INDEX IF EXISTS idx_files_due;
CREATE INDEX IF NOT EXISTS idx_files_due ON files(due_at) WHERE parsed_at IS NULL AND failed_at IS NULL;
-- What used to be phases in meta.kind becomes dates and columns.
UPDATE files SET failed_at = COALESCE(parsed_at, now()), parsed_at = NULL WHERE meta->>'kind' = 'parse_failed';
UPDATE files SET error = meta->>'error' WHERE meta ? 'error';
UPDATE files SET attempts = (meta->>'attempts')::int WHERE meta ? 'attempts';
UPDATE files SET meta = meta - 'kind' WHERE meta->>'kind' IN ('pending', 'unparseable', 'parse_failed');
UPDATE files SET meta = meta - 'error' - 'attempts' WHERE meta ? 'error' OR meta ? 'attempts';
-- Members of archives were never delivered on their own: they have no queue position.
UPDATE files SET due_at = NULL WHERE NOT (meta ? 'source') AND parsed_at IS NOT NULL;
COMMIT;
