-- ============================================================================
-- Migration 006: Metadata Schema Version
-- ============================================================================
-- Adds metadata.schema_version to track database schema version compatibility.
-- ============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS metadata.schema_version (
    version      VARCHAR(20) PRIMARY KEY,
    applied_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description  TEXT
);

COMMENT ON TABLE metadata.schema_version IS
    'Tracks applied schema version for pipeline pre-flight health checks.';

INSERT INTO metadata.schema_version (version, description)
VALUES ('1.0.0', 'Medallion ETL Pipeline Canonical Schema v1.0.0')
ON CONFLICT (version) DO NOTHING;

COMMIT;
