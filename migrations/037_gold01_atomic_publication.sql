-- ============================================================
-- FILE: migrations/037_gold01_atomic_publication.sql
-- PURPOSE: GOLD-ATOMIC-01: Safe Atomic Publication of Full-Snapshot Gold State
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.gold_publication_manifest (
    publication_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                      UUID NOT NULL REFERENCES metadata.gui_job_registry(run_id) ON DELETE RESTRICT,
    table_id                    INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    attempt_number              INTEGER NOT NULL,
    policy_version_id           UUID NOT NULL REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    staging_object_identity     VARCHAR(255) NOT NULL,
    target_object_identity      VARCHAR(255) NOT NULL,
    publication_strategy        VARCHAR(50) NOT NULL CHECK (publication_strategy IN ('TRANSACTIONAL_SWAP', 'TRANSACTIONAL_REPLACE', 'VERSIONED_POINTER_SWAP', 'DELTA_ACID_OVERWRITE')),
    reconciliation_id           UUID NOT NULL REFERENCES metadata.cdc_attempt_reconciliation(reconciliation_id) ON DELETE RESTRICT,
    manifest_id                 UUID NOT NULL REFERENCES metadata.attempt_persistence_manifest(manifest_id) ON DELETE RESTRICT,
    published_row_count         BIGINT NOT NULL,
    publication_fingerprint     VARCHAR(64) NOT NULL,
    status                      VARCHAR(30) NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'ROLLED_BACK', 'FAILED')),
    published_at                TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT unq_gold_publication_attempt UNIQUE (run_id, table_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_gold_publication_table 
ON metadata.gold_publication_manifest (table_id, policy_version_id);

CREATE INDEX IF NOT EXISTS idx_gold_publication_run 
ON metadata.gold_publication_manifest (run_id, table_id, attempt_number);
