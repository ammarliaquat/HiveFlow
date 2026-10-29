-- Extend strategy check constraint on metadata.table_cdc_policy_candidate
ALTER TABLE metadata.table_cdc_policy_candidate
DROP CONSTRAINT IF EXISTS table_cdc_policy_candidate_strategy_check;

ALTER TABLE metadata.table_cdc_policy_candidate
ADD CONSTRAINT table_cdc_policy_candidate_strategy_check
CHECK (strategy IN ('SEQUENCE_HIGH_WATERMARK', 'TIMESTAMP_HIGH_WATERMARK', 'INSERT_TIMESTAMP', 'FULL_SNAPSHOT', 'FULL_SNAPSHOT_CONSISTENT', 'LOG_BASED'));

-- Allow NULL window_id for cursorless full snapshot attempts
ALTER TABLE metadata.attempt_persistence_manifest ALTER COLUMN window_id DROP NOT NULL;
ALTER TABLE metadata.cdc_attempt_reconciliation ALTER COLUMN window_id DROP NOT NULL;

CREATE TABLE IF NOT EXISTS metadata.source_snapshot_attempt (
    attempt_id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                      UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                    INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    attempt_number              INTEGER NOT NULL,
    policy_version_id           UUID NOT NULL REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    source_engine               VARCHAR(50) NOT NULL,
    storage_engine              VARCHAR(50) NOT NULL,
    isolation_level             VARCHAR(50) NOT NULL,
    status                      VARCHAR(30) NOT NULL CHECK (status IN ('ACTIVE', 'CAPTURED', 'FAILED')),
    failure_code                VARCHAR(100),
    started_at                  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at                TIMESTAMPTZ,
    CONSTRAINT unq_source_snapshot_attempt UNIQUE (run_id, table_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS metadata.source_snapshot_manifest (
    snapshot_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    attempt_id                  UUID NOT NULL REFERENCES metadata.source_snapshot_attempt(attempt_id) ON DELETE RESTRICT,
    run_id                      UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                    INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    attempt_number              INTEGER NOT NULL,
    policy_version_id           UUID NOT NULL REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    source_schema_fingerprint   VARCHAR(64) NOT NULL,
    extracted_row_count         BIGINT NOT NULL,
    snapshot_fingerprint        VARCHAR(64) NOT NULL,
    captured_at                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT unq_source_snapshot_manifest UNIQUE (run_id, table_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS metadata.snapshot_commit_event (
    commit_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_log_id                BIGINT NOT NULL UNIQUE REFERENCES metadata.batch_log(id) ON DELETE RESTRICT,
    run_id                      UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                    INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    attempt_number              INTEGER NOT NULL,
    policy_version_id           UUID NOT NULL REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    snapshot_id                 UUID NOT NULL REFERENCES metadata.source_snapshot_manifest(snapshot_id) ON DELETE RESTRICT,
    manifest_id                 UUID NOT NULL REFERENCES metadata.attempt_persistence_manifest(manifest_id) ON DELETE RESTRICT,
    reconciliation_id           UUID NOT NULL REFERENCES metadata.cdc_attempt_reconciliation(reconciliation_id) ON DELETE RESTRICT,
    publication_id              UUID NOT NULL REFERENCES metadata.gold_publication_manifest(publication_id) ON DELETE RESTRICT,
    published_row_count         BIGINT NOT NULL,
    committed_at                TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_snapshot_attempt_table
ON metadata.source_snapshot_attempt (table_id, policy_version_id);

CREATE INDEX IF NOT EXISTS idx_source_snapshot_manifest_run
ON metadata.source_snapshot_manifest (run_id, table_id, attempt_number);

CREATE INDEX IF NOT EXISTS idx_snapshot_commit_event_table
ON metadata.snapshot_commit_event (table_id, policy_version_id);
