-- Tier-2 execution control supports one independent subscription per source table and target system.
-- Existing Tier-2 rows, policies, CDC cursors, and run evidence remain bound to their existing table_id.

BEGIN;

ALTER TABLE metadata.db_tables_etl
    DROP CONSTRAINT IF EXISTS uq_db_tables_etl_source;

ALTER TABLE metadata.db_tables_etl
    ADD CONSTRAINT uq_db_tables_etl_source_target
    UNIQUE (source_tag, source_schema, source_table, target_tag);

DROP INDEX IF EXISTS metadata.uq_db_tables_etl_source_master;

CREATE UNIQUE INDEX IF NOT EXISTS uq_db_tables_etl_source_master_target
    ON metadata.db_tables_etl (source_table_master_key, target_tag)
    WHERE source_table_master_key IS NOT NULL
      AND target_tag IS NOT NULL;

COMMIT;
