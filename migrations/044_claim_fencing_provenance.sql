-- ============================================================
-- FILE: migrations/044_claim_fencing_provenance.sql
-- PURPOSE: Plaintext Token Sanitization, Publication Deadline, Recovery Exclusivity, and 7-Tuple Evidence Ledger
-- ============================================================

-- Phase A: Sanitize metadata.job_scope_claim & Enforce Publication Deadline and Recovery Exclusivity
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'metadata' AND table_name = 'job_scope_claim' AND column_name = 'fencing_token'
    ) THEN
        ALTER TABLE metadata.job_scope_claim DROP COLUMN fencing_token;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'metadata' AND table_name = 'job_scope_claim' AND column_name = 'publication_deadline'
    ) THEN
        ALTER TABLE metadata.job_scope_claim ADD COLUMN publication_deadline TIMESTAMPTZ NULL;
    END IF;
END $$;

-- Drop old partial index on job_scope_claim and re-create extending exclusivity to RECOVERY_REQUIRED
DROP INDEX IF EXISTS metadata.uq_active_table_scope_claim;

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_table_scope_claim
ON metadata.job_scope_claim (source_table_id)
WHERE status IN ('RESERVED', 'ACTIVE', 'PUBLISHING', 'RECOVERY_REQUIRED') AND released_at IS NULL;

-- Consolidate job_scope_claim check constraints
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = 'metadata'
          AND table_name = 'job_scope_claim'
          AND constraint_type = 'CHECK'
          AND (constraint_name LIKE '%digest%' OR constraint_name LIKE '%lifecycle%' OR constraint_name LIKE '%status%')
    ) LOOP
        EXECUTE 'ALTER TABLE metadata.job_scope_claim DROP CONSTRAINT IF EXISTS ' || quote_ident(r.constraint_name);
    END LOOP;
END $$;

-- Backfill publication_deadline for any existing PUBLISHING claims
UPDATE metadata.job_scope_claim
SET publication_deadline = CURRENT_TIMESTAMP + INTERVAL '1 hour'
WHERE status = 'PUBLISHING' AND publication_deadline IS NULL;

ALTER TABLE metadata.job_scope_claim
ADD CONSTRAINT chk_job_scope_claim_lifecycle
CHECK (
    (status = 'RESERVED' AND fencing_token_digest IS NULL AND publication_deadline IS NULL)
    OR (status = 'ACTIVE' AND fencing_token_digest ~ '^[a-f0-9]{64}$' AND released_at IS NULL)
    OR (status = 'PUBLISHING' AND fencing_token_digest ~ '^[a-f0-9]{64}$' AND publication_deadline IS NOT NULL AND released_at IS NULL)
    OR (status = 'RECOVERY_REQUIRED' AND fencing_token_digest ~ '^[a-f0-9]{64}$' AND released_at IS NULL)
    OR (status IN ('RELEASED', 'REVOKED', 'EXPIRED') AND released_at IS NOT NULL)
);

-- Phase B: Sanitize metadata.gold_publication_manifest & Consolidate Status Constraints
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'metadata' AND table_name = 'gold_publication_manifest' AND column_name = 'fencing_token'
    ) THEN
        ALTER TABLE metadata.gold_publication_manifest DROP COLUMN fencing_token;
    END IF;
END $$;

-- Drop all old publication status constraints if present
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = 'metadata'
          AND table_name = 'gold_publication_manifest'
          AND constraint_type = 'CHECK'
          AND constraint_name LIKE '%status%'
    ) LOOP
        EXECUTE 'ALTER TABLE metadata.gold_publication_manifest DROP CONSTRAINT IF EXISTS ' || quote_ident(r.constraint_name);
    END LOOP;
END $$;

-- Add consolidated Gold publication status constraint
ALTER TABLE metadata.gold_publication_manifest
ADD CONSTRAINT chk_gold_publication_status
CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'ROLLED_BACK', 'FAILED', 'RECOVERY_REQUIRED'));

-- Add evidence audit and codec columns if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'metadata' AND table_name = 'gold_publication_manifest' AND column_name = 'evidence_codec_version'
    ) THEN
        ALTER TABLE metadata.gold_publication_manifest
        ADD COLUMN evidence_codec_version VARCHAR(20) DEFAULT 'canon_v1' NOT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'metadata' AND table_name = 'gold_publication_manifest' AND column_name = 'target_null_key_count'
    ) THEN
        ALTER TABLE metadata.gold_publication_manifest
        ADD COLUMN target_null_key_count BIGINT DEFAULT 0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'metadata' AND table_name = 'gold_publication_manifest' AND column_name = 'target_duplicate_key_count'
    ) THEN
        ALTER TABLE metadata.gold_publication_manifest
        ADD COLUMN target_duplicate_key_count BIGINT DEFAULT 0;
    END IF;
END $$;
