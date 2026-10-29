-- Migration 025: Add Data Profiling and Business Key Discovery Metadata Fields
-- Purpose: Track key discovery source (PRIMARY_KEY, PROFILED, HEURISTIC), confidence score, and duplicate count.

ALTER TABLE metadata.db_tables
    ADD COLUMN IF NOT EXISTS key_source VARCHAR(50) DEFAULT 'PRIMARY_KEY',
    ADD COLUMN IF NOT EXISTS key_confidence INT DEFAULT 100,
    ADD COLUMN IF NOT EXISTS duplicate_count BIGINT DEFAULT 0;

ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS key_source VARCHAR(50) DEFAULT 'PRIMARY_KEY',
    ADD COLUMN IF NOT EXISTS key_confidence INT DEFAULT 100,
    ADD COLUMN IF NOT EXISTS duplicate_count BIGINT DEFAULT 0;

COMMENT ON COLUMN metadata.db_tables_etl.key_source IS 'Origin of columns_unique_key: PRIMARY_KEY, PROFILED, PROFILED_COMPOSITE, PROFILED_LOWEST_DUP, HEURISTIC, NONE.';
COMMENT ON COLUMN metadata.db_tables_etl.key_confidence IS 'Confidence percentage (0-100) in the discovered unique key.';
COMMENT ON COLUMN metadata.db_tables_etl.duplicate_count IS 'Count of duplicate rows discovered on candidate key during data profiling.';
