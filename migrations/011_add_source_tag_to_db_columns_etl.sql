-- ==============================================================================
-- FILE: migrations/011_add_source_tag_to_db_columns_etl.sql
-- PURPOSE: Add source_tag and source_schema directly to db_columns_etl
--          so column metadata can be queried/validated without joining tables.
-- ==============================================================================

ALTER TABLE metadata.db_columns_etl
    ADD COLUMN IF NOT EXISTS source_tag    VARCHAR(100),
    ADD COLUMN IF NOT EXISTS source_schema VARCHAR(200);

-- Backfill source_tag and source_schema from parent table db_tables_etl
UPDATE metadata.db_columns_etl c
SET source_tag    = t.source_tag,
    source_schema = t.source_schema
FROM metadata.db_tables_etl t
WHERE c.table_id = t.table_id
  AND (c.source_tag IS NULL OR c.source_schema IS NULL);

-- Create composite index on (source_tag, source_table, is_active) for fast queries
CREATE INDEX IF NOT EXISTS idx_etl_cols_tag_table
    ON metadata.db_columns_etl (source_tag, source_table, is_active);

COMMENT ON COLUMN metadata.db_columns_etl.source_tag    IS 'Inherited from db_tables_etl.source_tag for direct column lookup';
COMMENT ON COLUMN metadata.db_columns_etl.source_schema IS 'Inherited from db_tables_etl.source_schema for direct column lookup';
