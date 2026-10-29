-- ============================================================
-- FILE: migrations/034_cdc01_bounded_cursor_state.sql
-- PURPOSE: CDC-01: Explicit CDC Candidate Authority, Committed Cursor State, and Attempt Window
-- ============================================================

-- Phase A: Explicit Candidate CDC Policy Authority
CREATE TABLE IF NOT EXISTS metadata.table_cdc_policy_candidate (
    table_id                        INTEGER PRIMARY KEY REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    policy_schema_version           INTEGER DEFAULT 1 NOT NULL,
    strategy                        VARCHAR(50) NOT NULL CHECK (strategy IN ('SEQUENCE_HIGH_WATERMARK', 'TIMESTAMP_HIGH_WATERMARK', 'INSERT_TIMESTAMP', 'FULL_SNAPSHOT', 'LOG_BASED')),
    mutation_mode                   VARCHAR(20) NOT NULL CHECK (mutation_mode IN ('APPEND_ONLY', 'MUTABLE', 'UNKNOWN')),
    cursor_finality_mode            VARCHAR(30) NOT NULL CHECK (cursor_finality_mode IN ('PREFIX_FINAL', 'UNSAFE_OR_UNKNOWN')),
    cursor_columns                  JSONB NOT NULL,
    cursor_types                    JSONB NOT NULL,
    tie_breaker_columns             JSONB,
    candidate_revision              BIGINT DEFAULT 1 NOT NULL,
    updated_at                      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Phase B: Committed CDC State Ledger (Keyed by table_id and policy_version_id)
CREATE TABLE IF NOT EXISTS metadata.table_cdc_state (
    table_id                        INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    policy_version_id               UUID NOT NULL REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    cdc_policy_fingerprint          VARCHAR(64) NOT NULL,
    committed_cursor                JSONB,
    committed_run_id                UUID REFERENCES metadata.gui_job_registry(run_id),
    committed_attempt_number        INTEGER,
    committed_window_fingerprint    VARCHAR(64),
    committed_at                    TIMESTAMPTZ,
    state_version                   BIGINT DEFAULT 1 NOT NULL,
    updated_at                      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_table_cdc_state PRIMARY KEY (table_id, policy_version_id)
);

CREATE INDEX IF NOT EXISTS idx_table_cdc_state_policy 
ON metadata.table_cdc_state (policy_version_id);

-- Phase C: Durable Attempt-Scoped Execution Window Ledger
CREATE TABLE IF NOT EXISTS metadata.cdc_attempt_window (
    window_id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                          UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                        INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    attempt_number                  INTEGER NOT NULL,
    policy_version_id               UUID NOT NULL REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    cdc_policy_fingerprint          VARCHAR(64) NOT NULL,
    window_fingerprint              VARCHAR(64) NOT NULL,
    lower_cursor                    JSONB,
    upper_cursor                    JSONB NOT NULL,
    captured_at                     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT uq_cdc_attempt_window UNIQUE (run_id, table_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_cdc_attempt_window_lookup 
ON metadata.cdc_attempt_window (table_id, run_id);
