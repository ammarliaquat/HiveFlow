-- ============================================================
-- FILE: migrations/036_obs01_atomic_cursor_commit.sql
-- PURPOSE: OBS-01: Atomic Cursor Advancement, Terminal Success & Immutable Commit Ledger
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.cdc_commit_event (
    commit_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_log_id                BIGINT NOT NULL REFERENCES metadata.batch_log(id) ON DELETE RESTRICT,
    run_id                      UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                    INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    attempt_number              INTEGER NOT NULL,
    policy_version_id           UUID NOT NULL REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    reconciliation_id           UUID NOT NULL REFERENCES metadata.cdc_attempt_reconciliation(reconciliation_id) ON DELETE RESTRICT,
    manifest_id                 UUID NOT NULL REFERENCES metadata.attempt_persistence_manifest(manifest_id) ON DELETE RESTRICT,
    window_id                   UUID NOT NULL REFERENCES metadata.cdc_attempt_window(window_id) ON DELETE RESTRICT,
    previous_cursor             JSONB,
    committed_cursor            JSONB NOT NULL,
    previous_state_version      BIGINT,
    new_state_version           BIGINT NOT NULL,
    policy_fingerprint          VARCHAR(64) NOT NULL,
    window_fingerprint          VARCHAR(64) NOT NULL,
    manifest_fingerprint        VARCHAR(64) NOT NULL,
    reconciliation_fingerprint  VARCHAR(64) NOT NULL,
    commit_mode                 VARCHAR(30) NOT NULL,
    committed_at                TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT unq_cdc_commit_batch_log UNIQUE (batch_log_id),
    CONSTRAINT unq_cdc_commit_attempt UNIQUE (run_id, table_id, attempt_number),
    CONSTRAINT chk_commit_mode CHECK (commit_mode IN ('BOOTSTRAP', 'INCREMENTAL', 'EMPTY'))
);

CREATE INDEX IF NOT EXISTS idx_cdc_commit_lookup 
ON metadata.cdc_commit_event (table_id, policy_version_id);

CREATE INDEX IF NOT EXISTS idx_cdc_commit_run 
ON metadata.cdc_commit_event (run_id, table_id, attempt_number);
