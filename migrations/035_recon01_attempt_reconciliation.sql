-- ============================================================
-- FILE: migrations/035_recon01_attempt_reconciliation.sql
-- PURPOSE: RECON-01: Physical Strategy-Aware Reconciliation & Attempt Persistence Manifest
-- ============================================================

-- Phase A: Create Attempt Persistence Manifest Ledger
CREATE TABLE IF NOT EXISTS metadata.attempt_persistence_manifest (
    manifest_id                 UUID PRIMARY KEY,
    run_id                      UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                    INTEGER NOT NULL,
    attempt_number              INTEGER NOT NULL,
    policy_version_id           UUID NOT NULL,
    window_id                   UUID NOT NULL REFERENCES metadata.cdc_attempt_window(window_id) ON DELETE RESTRICT,
    storage_layer               VARCHAR(50) NOT NULL,
    storage_object_identity     VARCHAR(255) NOT NULL,
    expected_row_count          BIGINT NOT NULL,
    expected_key_fingerprint    VARCHAR(64) NOT NULL,
    expected_content_fingerprint VARCHAR(64),
    schema_fingerprint          VARCHAR(64) NOT NULL,
    manifest_fingerprint        VARCHAR(64) NOT NULL,
    created_at                  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT unq_attempt_manifest UNIQUE (run_id, table_id, attempt_number, storage_layer)
);

CREATE INDEX IF NOT EXISTS idx_manifest_lookup 
ON metadata.attempt_persistence_manifest (run_id, table_id, attempt_number);

CREATE INDEX IF NOT EXISTS idx_manifest_policy_ver 
ON metadata.attempt_persistence_manifest (table_id, policy_version_id);


-- Phase B: Create CDC Attempt Reconciliation Ledger
CREATE TABLE IF NOT EXISTS metadata.cdc_attempt_reconciliation (
    reconciliation_id           UUID PRIMARY KEY,
    manifest_id                 UUID NOT NULL REFERENCES metadata.attempt_persistence_manifest(manifest_id) ON DELETE RESTRICT,
    run_id                      UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                    INTEGER NOT NULL,
    attempt_number              INTEGER NOT NULL,
    policy_version_id           UUID NOT NULL,
    window_id                   UUID NOT NULL REFERENCES metadata.cdc_attempt_window(window_id) ON DELETE RESTRICT,
    reconciliation_strategy     VARCHAR(50) NOT NULL,
    storage_layer               VARCHAR(50) NOT NULL,
    is_commit_authoritative     BOOLEAN DEFAULT FALSE NOT NULL,
    expected_row_count          BIGINT NOT NULL,
    physical_row_count          BIGINT NOT NULL,
    row_count_delta             BIGINT NOT NULL,
    missing_key_count           BIGINT DEFAULT 0 NOT NULL,
    unexpected_key_count        BIGINT DEFAULT 0 NOT NULL,
    duplicate_key_count         BIGINT DEFAULT 0 NOT NULL,
    null_key_count              BIGINT DEFAULT 0 NOT NULL,
    expected_key_fingerprint    VARCHAR(64) NOT NULL,
    physical_key_fingerprint    VARCHAR(64) NOT NULL,
    expected_content_fingerprint VARCHAR(64),
    physical_content_fingerprint VARCHAR(64),
    outcome                     VARCHAR(30) NOT NULL,
    failure_code                VARCHAR(100),
    details                     JSONB,
    reconciliation_fingerprint  VARCHAR(64) NOT NULL,
    reconciled_at               TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT chk_recon_outcome CHECK (
        outcome IN ('CONVERGED', 'REJECTED', 'EMPTY_VERIFIED')
    )
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_lookup 
ON metadata.cdc_attempt_reconciliation (run_id, table_id, attempt_number);

CREATE INDEX IF NOT EXISTS idx_reconciliation_policy_ver 
ON metadata.cdc_attempt_reconciliation (table_id, policy_version_id);

CREATE INDEX IF NOT EXISTS idx_reconciliation_outcome 
ON metadata.cdc_attempt_reconciliation (table_id, outcome, is_commit_authoritative);
