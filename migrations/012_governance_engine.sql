-- ==============================================================================
-- Migration: 012_governance_engine.sql
-- Purpose: Governed policy execution columns in metadata.db_tables_etl
-- ==============================================================================

ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS approved_cdc_column    VARCHAR(200),
    ADD COLUMN IF NOT EXISTS approved_cdc_type      VARCHAR(50),  -- 'TIMESTAMP', 'SEQUENCE', 'SCN', 'LOG_BASED', 'NONE'
    ADD COLUMN IF NOT EXISTS governance_status      VARCHAR(30)   DEFAULT 'PENDING', -- 'PENDING', 'APPROVED', 'OVERRIDDEN'
    ADD COLUMN IF NOT EXISTS approved_by            VARCHAR(100),
    ADD COLUMN IF NOT EXISTS approved_at            TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_etl_governance_status 
    ON metadata.db_tables_etl (governance_status, is_active);

COMMENT ON COLUMN metadata.db_tables_etl.governance_status IS 'Policy status: PENDING (new discovery), APPROVED (steward verified), OVERRIDDEN (custom policy).';
COMMENT ON COLUMN metadata.db_tables_etl.approved_cdc_column IS 'Governed CDC watermark column approved by Data Steward. Source of truth for execution when APPROVED/OVERRIDDEN.';
