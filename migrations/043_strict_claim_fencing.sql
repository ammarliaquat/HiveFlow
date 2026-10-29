-- migrations/043_strict_claim_fencing.sql
-- JOB-01 / GOLD-ATOMIC-01: Authoritative Table-Level Claim Fencing, Fencing Epochs, and Lineage Constraint Hardening.

-- 1. Normalize legacy claim rows: if released_at is set, status should be 'RELEASED'; otherwise 'REVOKED'
UPDATE metadata.job_scope_claim
SET status = 'RELEASED'
WHERE released_at IS NOT NULL;

UPDATE metadata.job_scope_claim
SET status = 'REVOKED', released_at = CURRENT_TIMESTAMP
WHERE released_at IS NULL;

-- Normalize legacy publication manifests lacking certified target evidence
UPDATE metadata.gold_publication_manifest
SET status = 'ROLLED_BACK'
WHERE status = 'PUBLISHED' AND (target_evidence_fingerprint IS NULL OR target_schema_fingerprint IS NULL);

-- 2. Drop old multi-action index if present
DROP INDEX IF EXISTS metadata.uq_active_job_scope_claim;

-- 3. Add authoritative fencing columns
ALTER TABLE metadata.job_scope_claim
    ADD COLUMN IF NOT EXISTS fencing_token_digest VARCHAR(64),
    ADD COLUMN IF NOT EXISTS fencing_epoch BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS heartbeat_version BIGINT NOT NULL DEFAULT 1;

-- 4. Create per-table active claim exclusivity unique index
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_table_scope_claim
ON metadata.job_scope_claim (source_table_id)
WHERE status IN ('ACTIVE', 'PUBLISHING', 'RESERVED') AND released_at IS NULL;

-- 5. Status check constraints
ALTER TABLE metadata.job_scope_claim
    DROP CONSTRAINT IF EXISTS job_scope_claim_status_check,
    DROP CONSTRAINT IF EXISTS chk_job_scope_claim_status;

ALTER TABLE metadata.job_scope_claim
    ADD CONSTRAINT chk_job_scope_claim_status
    CHECK (status IN ('RESERVED', 'ACTIVE', 'PUBLISHING', 'RELEASED', 'REVOKED', 'EXPIRED', 'RECOVERY_REQUIRED'));

-- 6. Conditional constraint enforcing non-null token digest, epoch, and lease on active/publishing claims
ALTER TABLE metadata.job_scope_claim
    DROP CONSTRAINT IF EXISTS chk_job_scope_claim_active_fencing;

ALTER TABLE metadata.job_scope_claim
    ADD CONSTRAINT chk_job_scope_claim_active_fencing
    CHECK (
        status NOT IN ('ACTIVE', 'PUBLISHING') OR (
            fencing_token_digest IS NOT NULL AND
            length(fencing_token_digest) = 64 AND
            owner_instance_id IS NOT NULL AND
            attempt_number > 0 AND
            worker_attempt_number > 0 AND
            fencing_epoch > 0 AND
            lease_expires_at IS NOT NULL AND
            released_at IS NULL
        )
    );

-- 7. Gold publication manifest enhancements
ALTER TABLE metadata.gold_publication_manifest
    ADD COLUMN IF NOT EXISTS claim_id BIGINT REFERENCES metadata.job_scope_claim(claim_id),
    ADD COLUMN IF NOT EXISTS fencing_epoch BIGINT,
    ADD COLUMN IF NOT EXISTS worker_attempt_number INTEGER,
    ADD COLUMN IF NOT EXISTS owner_instance_id VARCHAR(100),
    ADD COLUMN IF NOT EXISTS fencing_token_digest VARCHAR(64);

ALTER TABLE metadata.gold_publication_manifest
    DROP CONSTRAINT IF EXISTS chk_gold_publication_status;

ALTER TABLE metadata.gold_publication_manifest
    ADD CONSTRAINT chk_gold_publication_status
    CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'ROLLED_BACK', 'FAILED', 'RECOVERY_REQUIRED'));

ALTER TABLE metadata.gold_publication_manifest
    DROP CONSTRAINT IF EXISTS chk_gold_publication_evidence_published;

ALTER TABLE metadata.gold_publication_manifest
    ADD CONSTRAINT chk_gold_publication_evidence_published
    CHECK (
        status != 'PUBLISHED' OR (
            target_evidence_fingerprint IS NOT NULL AND
            target_schema_fingerprint IS NOT NULL
        )
    );
