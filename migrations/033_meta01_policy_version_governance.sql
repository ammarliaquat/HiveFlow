-- ============================================================
-- FILE: migrations/033_meta01_policy_version_governance.sql
-- PURPOSE: META-01: Table Policy Versioning, Promotion Ledger, and Provenance Governance
-- ============================================================

-- Phase A: Create Table Policy Version Ledger (Immutable Snapshots)
CREATE TABLE IF NOT EXISTS metadata.table_policy_version (
    policy_version_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id                        INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    version_number                  BIGINT NOT NULL,
    policy_schema_version           INTEGER DEFAULT 1 NOT NULL,
    contract_payload                JSONB NOT NULL,
    source_key_fingerprint          VARCHAR(64) NOT NULL,
    mapping_fingerprint             VARCHAR(64) NOT NULL,
    target_key_fingerprint          VARCHAR(64) NOT NULL,
    contract_fingerprint            VARCHAR(64) NOT NULL,
    policy_fingerprint              VARCHAR(64) NOT NULL,
    promoted_at                     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    promoted_by_type                VARCHAR(20) NOT NULL CHECK (promoted_by_type IN ('USER', 'SYSTEM', 'SCHEDULER')),
    promoted_by_id                  VARCHAR(100) NOT NULL,
    promotion_reason                TEXT,
    supersedes_version_id           UUID REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    CONSTRAINT uq_table_version UNIQUE (table_id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_table_policy_version_lookup 
ON metadata.table_policy_version (table_id, version_number DESC);

-- Phase B: Create Policy Promotion Audit Ledger
CREATE TABLE IF NOT EXISTS metadata.policy_promotion_audit (
    promotion_request_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_id                        INTEGER NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE RESTRICT,
    expected_candidate_fingerprint  VARCHAR(64) NOT NULL,
    outcome                         VARCHAR(20) NOT NULL CHECK (outcome IN ('ACCEPTED', 'REJECTED')),
    policy_version_id               UUID REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT,
    failure_code                    VARCHAR(50),
    actor_type                      VARCHAR(20) NOT NULL CHECK (actor_type IN ('USER', 'SYSTEM', 'SCHEDULER')),
    actor_id                        VARCHAR(100) NOT NULL,
    reason                          TEXT,
    details                         JSONB,
    requested_at                    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at                    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_policy_promotion_audit_table 
ON metadata.policy_promotion_audit (table_id, requested_at DESC);

-- Phase C: Extend metadata.db_tables_etl with Single Authoritative Current Pointer
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'metadata' AND table_name = 'db_tables_etl' AND column_name = 'current_policy_version_id'
    ) THEN
        ALTER TABLE metadata.db_tables_etl 
        ADD COLUMN current_policy_version_id UUID REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT;
    END IF;
END $$;

-- Phase D: Extend metadata.batch_log with Policy Provenance
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'metadata' AND table_name = 'batch_log' AND column_name = 'policy_version_id'
    ) THEN
        ALTER TABLE metadata.batch_log 
        ADD COLUMN policy_version_id UUID REFERENCES metadata.table_policy_version(policy_version_id) ON DELETE RESTRICT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'metadata' AND table_name = 'batch_log' AND column_name = 'policy_fingerprint'
    ) THEN
        ALTER TABLE metadata.batch_log 
        ADD COLUMN policy_fingerprint VARCHAR(64);
    END IF;
END $$;
