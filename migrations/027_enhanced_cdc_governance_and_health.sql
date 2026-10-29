-- Migration 027: Add cdc_provider, cdc_health_details JSON, and support PENDING_APPROVAL cdc_strategy
-- Purpose: Implement computed confidence scoring, distinct ratio variation checks, provider plugin tags, and rich health diagnostics.

ALTER TABLE metadata.db_tables
    ADD COLUMN IF NOT EXISTS cdc_provider VARCHAR(50) DEFAULT 'TIMESTAMP',
    ADD COLUMN IF NOT EXISTS cdc_health_details TEXT DEFAULT '{}';

ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS cdc_provider VARCHAR(50) DEFAULT 'TIMESTAMP',
    ADD COLUMN IF NOT EXISTS cdc_health_details TEXT DEFAULT '{}';

COMMENT ON COLUMN metadata.db_tables_etl.cdc_provider IS 'Specific CDC Plugin Provider: TIMESTAMP, INSERT_ONLY, ORA_ROWSCN, FLASHBACK, SQLSERVER_CDC, SQLSERVER_CT, POSTGRES_WAL, MYSQL_BINLOG, NONE.';
COMMENT ON COLUMN metadata.db_tables_etl.cdc_health_details IS 'JSON payload containing diagnostic health checks (datatype, null_pct, variation_pct, recent_activity, unique_key_valid).';
