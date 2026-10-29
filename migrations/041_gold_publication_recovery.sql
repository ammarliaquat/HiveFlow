-- migrations/041_gold_publication_recovery.sql
-- GOLD-ATOMIC-01: Cross-database recoverable publication, cleanup tracking, and CAS revision fencing.

ALTER TABLE metadata.gold_publication_manifest
    ADD COLUMN IF NOT EXISTS staging_evidence_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS target_evidence_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS target_commit_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cleanup_status VARCHAR(30) DEFAULT 'PENDING' CHECK (cleanup_status IN ('PENDING', 'COMPLETED', 'FAILED', 'NOT_REQUIRED')),
    ADD COLUMN IF NOT EXISTS cleanup_error TEXT,
    ADD COLUMN IF NOT EXISTS state_revision INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_recovery_at TIMESTAMPTZ;

ALTER TABLE metadata.gold_publication_manifest
    DROP CONSTRAINT IF EXISTS gold_publication_manifest_status_check;

ALTER TABLE metadata.gold_publication_manifest
    ADD CONSTRAINT gold_publication_manifest_status_check
    CHECK (status IN ('PENDING', 'PUBLISHED', 'ROLLED_BACK', 'FAILED', 'RECOVERY_REQUIRED'));
