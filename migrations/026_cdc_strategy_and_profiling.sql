-- Migration 026: Add CDC Strategy, Profiling Source, Confidence, Health, and Null Row Tracking
-- Purpose: Support TIMESTAMP, INSERT_ONLY, SCN, LOG_BASED, and FULL_LOAD CDC strategies with confidence scoring.

ALTER TABLE metadata.db_tables
    ADD COLUMN IF NOT EXISTS cdc_strategy VARCHAR(50) DEFAULT 'TIMESTAMP',
    ADD COLUMN IF NOT EXISTS cdc_source VARCHAR(50) DEFAULT 'PROFILED',
    ADD COLUMN IF NOT EXISTS cdc_confidence INT DEFAULT 95,
    ADD COLUMN IF NOT EXISTS cdc_health VARCHAR(20) DEFAULT 'OK',
    ADD COLUMN IF NOT EXISTS cdc_null_rows BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cdc_profile_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS cdc_strategy VARCHAR(50) DEFAULT 'TIMESTAMP',
    ADD COLUMN IF NOT EXISTS cdc_source VARCHAR(50) DEFAULT 'PROFILED',
    ADD COLUMN IF NOT EXISTS cdc_confidence INT DEFAULT 95,
    ADD COLUMN IF NOT EXISTS cdc_health VARCHAR(20) DEFAULT 'OK',
    ADD COLUMN IF NOT EXISTS cdc_null_rows BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cdc_profile_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

COMMENT ON COLUMN metadata.db_tables_etl.cdc_strategy IS 'CDC Execution Strategy: TIMESTAMP, INSERT_ONLY, SCN, LOG_BASED, FULL_LOAD.';
COMMENT ON COLUMN metadata.db_tables_etl.cdc_source IS 'Origin of cdc_columns: METADATA, NATIVE, PROFILED, HEURISTIC, NONE.';
COMMENT ON COLUMN metadata.db_tables_etl.cdc_confidence IS 'Confidence percentage (0-100) in the discovered CDC watermark strategy.';
COMMENT ON COLUMN metadata.db_tables_etl.cdc_health IS 'Health status of CDC column: OK, WARNING, FAIL.';
COMMENT ON COLUMN metadata.db_tables_etl.cdc_null_rows IS 'Count of NULL rows detected on the CDC column in the source table.';
COMMENT ON COLUMN metadata.db_tables_etl.cdc_profile_date IS 'Timestamp of the last data profiling analysis for CDC strategy.';
