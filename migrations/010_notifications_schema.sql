-- migrations/010_notifications_schema.sql
-- Phase B: Enterprise Notification Center & Actionable Alerts Schema
-- Stores derived notification events, cross-run incident correlation, and lifecycle audit tracking.

CREATE TABLE IF NOT EXISTS metadata.notifications (
    notification_id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Categorization & Structured Code
    notification_type VARCHAR(50) NOT NULL,      -- 'PIPELINE', 'CONNECTION', 'DISCOVERY', 'QUALITY', 'SYSTEM', 'SECURITY', 'VALIDATION'
    event_code VARCHAR(100) NOT NULL,            -- 'PIPELINE_LOAD_FAILED', 'CONNECTION_FAILED', 'QUALITY_CHECK_FAILED', 'SCHEMA_DRIFT', 'DISCOVERY_FAILED'
    created_by_component VARCHAR(100) NOT NULL,  -- 'PIPELINE', 'DISCOVERY', 'CONNECTION', 'QUALITY', 'GUI', 'SYSTEM'
    severity VARCHAR(20) NOT NULL,               -- 'WARNING', 'ERROR', 'CRITICAL' (No INFO stored)
    title VARCHAR(200) NOT NULL,
    message TEXT NOT NULL,
    
    -- Cross-Run Correlation & Deduplication
    correlation_key VARCHAR(150),                -- e.g. 'connection_failure:msopforg_opforg' or 'load_failed:msopforg_opforg.Membership'
    affected_count INTEGER DEFAULT 1,            -- Number of times or tables aggregated into this open incident
    
    -- Direct Context & Log Links
    run_id UUID,
    batch_log_id BIGINT,                         -- Direct link to metadata.batch_log.id for instant inspection
    summary_id BIGINT,                           -- Direct link to metadata.batch_summary.id
    source_tag VARCHAR(100),
    source_table VARCHAR(200),
    layer VARCHAR(50),                           -- 'Bronze', 'Silver', 'Gold', 'System'
    status VARCHAR(50) DEFAULT 'NEW',            -- 'NEW', 'ACKNOWLEDGED', 'RESOLVED'
    
    -- Incident Lifecycle Audit Trail
    acknowledged BOOLEAN DEFAULT false,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP,
    resolved BOOLEAN DEFAULT false,
    resolved_by VARCHAR(100),
    resolved_at TIMESTAMP,
    
    -- Email Tracking
    email_sent BOOLEAN DEFAULT false,
    email_sent_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_notifications_status ON metadata.notifications(status, severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_correlation ON metadata.notifications(correlation_key, status) WHERE correlation_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_notifications_type_sev ON metadata.notifications(notification_type, severity);
CREATE INDEX IF NOT EXISTS idx_notifications_run ON metadata.notifications(run_id);

COMMENT ON TABLE metadata.notifications IS 'Derived operational notification events and actionable alerts for ETL monitoring.';
COMMENT ON COLUMN metadata.notifications.correlation_key IS 'Unique incident key grouping related failures across runs until resolved.';
COMMENT ON COLUMN metadata.notifications.status IS 'Incident lifecycle status: NEW -> ACKNOWLEDGED -> RESOLVED.';
