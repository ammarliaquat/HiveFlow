-- migrations/015_schema_lifecycle.sql

-- 1. Schema Drift Alerts
CREATE TABLE IF NOT EXISTS metadata.schema_drift_alerts (
    alert_id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    table_id INTEGER REFERENCES metadata.db_tables_etl(table_id),
    deployment_correlation_id VARCHAR(100),
    column_name VARCHAR(100),
    drift_type VARCHAR(50), -- NEW_COLUMN, DROPPED_COLUMN, TYPE_CHANGED, RENAME_DETECTED
    old_canonical_type VARCHAR(50),
    new_canonical_type VARCHAR(50),
    severity VARCHAR(20), -- INFO, WARNING, CRITICAL
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, APPROVED, REJECTED, AUTO_PROMOTED
    impact_summary JSONB,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_schema_drift_table_id ON metadata.schema_drift_alerts(table_id);
CREATE INDEX IF NOT EXISTS idx_schema_drift_status ON metadata.schema_drift_alerts(status);

-- 2. Promotion Policies
CREATE TABLE IF NOT EXISTS metadata.promotion_policies (
    policy_id SERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES metadata.db_tables_etl(table_id),
    policy VARCHAR(20) DEFAULT 'MANUAL', -- AUTO, MANUAL, BLOCK
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_promo_policy_table ON metadata.promotion_policies(table_id);

-- 3. Versioning existing metadata.db_columns_etl
ALTER TABLE metadata.db_columns_etl ADD COLUMN IF NOT EXISTS schema_version INTEGER DEFAULT 1;
ALTER TABLE metadata.db_columns_etl ADD COLUMN IF NOT EXISTS effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE metadata.db_columns_etl ADD COLUMN IF NOT EXISTS retired_at TIMESTAMP;

-- Ensure an index supports querying active columns
CREATE INDEX IF NOT EXISTS idx_columns_etl_active ON metadata.db_columns_etl(table_id, retired_at) WHERE retired_at IS NULL;
