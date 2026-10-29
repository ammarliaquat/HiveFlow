-- migrations/042_job_scope_claim_fencing.sql
-- GOLD-ATOMIC-01 / JOB-01: Authoritative Table-Attempt Lease Fencing and Target Schema Fingerprint.

ALTER TABLE metadata.job_scope_claim
    ADD COLUMN IF NOT EXISTS attempt_number INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS worker_attempt_number INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS owner_instance_id VARCHAR(100) DEFAULT 'worker-local',
    ADD COLUMN IF NOT EXISTS fencing_token VARCHAR(128),
    ADD COLUMN IF NOT EXISTS lease_revision BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_heartbeat TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS status VARCHAR(30) DEFAULT 'ACTIVE';

ALTER TABLE metadata.job_scope_claim
    DROP CONSTRAINT IF EXISTS job_scope_claim_status_check;

ALTER TABLE metadata.job_scope_claim
    ADD CONSTRAINT job_scope_claim_status_check
    CHECK (status IN ('ACTIVE', 'PUBLISHING', 'RELEASED', 'REVOKED', 'EXPIRED'));

ALTER TABLE metadata.gold_publication_manifest
    ADD COLUMN IF NOT EXISTS fencing_token VARCHAR(128),
    ADD COLUMN IF NOT EXISTS fencing_revision BIGINT,
    ADD COLUMN IF NOT EXISTS target_schema_fingerprint VARCHAR(64);
