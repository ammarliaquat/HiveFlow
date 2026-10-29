-- ============================================================================
-- Migration 007: Add target_tag to batch_log and batch_summary
-- ============================================================================
-- Adds target_tag column to metadata.batch_log and metadata.batch_summary
-- so that audit logs explicitly record which target environment (e.g. land_n vs land_test)
-- each batch run wrote to.
-- ============================================================================

BEGIN;

ALTER TABLE metadata.batch_log
    ADD COLUMN IF NOT EXISTS target_tag VARCHAR(100);

ALTER TABLE metadata.batch_summary
    ADD COLUMN IF NOT EXISTS target_tag VARCHAR(100);

COMMENT ON COLUMN metadata.batch_log.target_tag IS
    'databases.ini section name for target DB connection (e.g. "land_n" or "land_test") used during the run.';

COMMENT ON COLUMN metadata.batch_summary.target_tag IS
    'databases.ini section name for target DB connection (e.g. "land_n" or "land_test") used during the run.';

COMMIT;
