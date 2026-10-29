-- migrations/045_recovery_and_frozen_policy.sql
-- Description: Extends execution registry with frozen policy tracking and recovery state machine.
-- NOTE: ALTER TYPE ... ADD VALUE IF NOT EXISTS cannot run inside a transaction block in PostgreSQL.
-- Steps 1 and 2 must be run outside BEGIN/COMMIT and are idempotent (IF NOT EXISTS).

-- 1. Extend job_run_status check constraint with RECOVERY_REQUIRED
ALTER TABLE metadata.gui_job_registry DROP CONSTRAINT IF EXISTS chk_job_status;
ALTER TABLE metadata.gui_job_registry ADD CONSTRAINT chk_job_status CHECK (
    status IN ('QUEUED', 'DISPATCHED', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED', 'ABANDONED', 'RECOVERY_REQUIRED')
);

-- 2. (Skipped claim_action ENUM creation, using VARCHAR instead)

BEGIN;

-- 3. Separate scope_action (immutable intent) from worker_action (runtime execution phase)
ALTER TABLE metadata.job_scope_claim
    RENAME COLUMN action TO scope_action;

ALTER TABLE metadata.job_scope_claim
    ADD COLUMN IF NOT EXISTS worker_action VARCHAR(50);

-- Backfill worker_action from scope_action for existing rows
UPDATE metadata.job_scope_claim
    SET worker_action = scope_action
    WHERE worker_action IS NULL;

-- Enforce scope_action immutability
CREATE OR REPLACE FUNCTION metadata.prevent_scope_action_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.scope_action IS DISTINCT FROM OLD.scope_action THEN
        RAISE EXCEPTION 'scope_action is immutable and cannot be updated on claim_id=%', OLD.claim_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_scope_action_update ON metadata.job_scope_claim;
CREATE TRIGGER trg_prevent_scope_action_update
    BEFORE UPDATE ON metadata.job_scope_claim
    FOR EACH ROW
    EXECUTE FUNCTION metadata.prevent_scope_action_update();

-- 4. Rebuild active-claim unique index by source_table_id
--    (index name already confirmed from live DB: uq_active_table_scope_claim)
DROP INDEX IF EXISTS metadata.uq_active_table_scope_claim;

CREATE UNIQUE INDEX uq_active_table_scope_claim
    ON metadata.job_scope_claim (source_table_id)
    WHERE status IN ('RESERVED', 'ACTIVE', 'PUBLISHING', 'RECOVERY_REQUIRED')
      AND released_at IS NULL;

-- 5. Extend gold_publication_manifest with expected vs. observed 7-tuple evidence fields
--    (Some target_* columns already exist from migration 041; IF NOT EXISTS is safe)
ALTER TABLE metadata.gold_publication_manifest
    ADD COLUMN IF NOT EXISTS expected_row_count            BIGINT,
    ADD COLUMN IF NOT EXISTS expected_key_fingerprint      VARCHAR(64),
    ADD COLUMN IF NOT EXISTS expected_content_fingerprint  VARCHAR(64),
    ADD COLUMN IF NOT EXISTS expected_schema_fingerprint   VARCHAR(64),
    ADD COLUMN IF NOT EXISTS expected_null_key_count       BIGINT,
    ADD COLUMN IF NOT EXISTS expected_duplicate_key_count  BIGINT,
    ADD COLUMN IF NOT EXISTS expected_codec_version        VARCHAR(32),
    -- observed fields (target_* columns already partially present; IF NOT EXISTS guards duplicates)
    ADD COLUMN IF NOT EXISTS target_row_count              BIGINT,
    ADD COLUMN IF NOT EXISTS target_key_fingerprint        VARCHAR(64),
    ADD COLUMN IF NOT EXISTS target_content_fingerprint    VARCHAR(64),
    ADD COLUMN IF NOT EXISTS target_codec_version          VARCHAR(32);

-- 6. Terminal manifest immutability: block UPDATE on all 4 terminal states.
--    FAILED is a legacy terminal; no new rows use it, but existing rows must be protected.
CREATE OR REPLACE FUNCTION metadata.prevent_terminal_manifest_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IN ('PUBLISHED', 'ROLLED_BACK', 'FAILED', 'RECOVERY_FAILED') THEN
        RAISE EXCEPTION
            'gold_publication_manifest row % is terminal (status=%) -- mutations forbidden',
            OLD.publication_id, OLD.status;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_protect_terminal_manifest_immutability ON metadata.gold_publication_manifest;
CREATE TRIGGER trg_protect_terminal_manifest_immutability
    BEFORE UPDATE ON metadata.gold_publication_manifest
    FOR EACH ROW
    EXECUTE FUNCTION metadata.prevent_terminal_manifest_mutation();

-- 7. Replace 044's status constraint with full compatible vocabulary.
--    Adds: PUBLISHING (pre-DML state), RECOVERY_REQUIRED (reaper target),
--          RECOVERY_FAILED (recovery concluded failure).
--    Keeps: FAILED as legacy terminal (no new rows use it).
DO $$
DECLARE r RECORD;
BEGIN
    FOR r IN (
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = 'metadata'
          AND table_name = 'gold_publication_manifest'
          AND constraint_type = 'CHECK'
          AND constraint_name LIKE '%status%'
    ) LOOP
        EXECUTE 'ALTER TABLE metadata.gold_publication_manifest DROP CONSTRAINT IF EXISTS '
                || quote_ident(r.constraint_name);
    END LOOP;
END $$;

ALTER TABLE metadata.gold_publication_manifest
ADD CONSTRAINT chk_gold_publication_status
CHECK (status IN (
    'PENDING',
    'PUBLISHING',
    'PUBLISHED',
    'ROLLED_BACK',
    'FAILED',
    'RECOVERY_REQUIRED',
    'RECOVERY_FAILED'
));

COMMIT;
