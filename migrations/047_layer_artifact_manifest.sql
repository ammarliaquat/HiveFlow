-- 047: governed layer artifacts without introducing another metadata ledger.
--
-- RECON-01 already provides metadata.attempt_persistence_manifest.  This
-- migration extends that ledger for Bronze/Silver producer artifacts so the
-- worker can consume an exact, verified object on isolated execution/retry.

ALTER TABLE metadata.attempt_persistence_manifest
    ALTER COLUMN window_id DROP NOT NULL;

ALTER TABLE metadata.attempt_persistence_manifest
    ADD COLUMN IF NOT EXISTS source_tag VARCHAR(100),
    ADD COLUMN IF NOT EXISTS target_tag VARCHAR(100),
    ADD COLUMN IF NOT EXISTS source_table VARCHAR(200),
    ADD COLUMN IF NOT EXISTS target_table VARCHAR(200),
    ADD COLUMN IF NOT EXISTS storage_uri VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS storage_format VARCHAR(30),
    ADD COLUMN IF NOT EXISTS artifact_status VARCHAR(30) NOT NULL DEFAULT 'VERIFIED',
    ADD COLUMN IF NOT EXISTS mapping_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS source_key_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS policy_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS artifact_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS cdc_lower_cursor JSONB,
    ADD COLUMN IF NOT EXISTS cdc_upper_cursor JSONB,
    ADD COLUMN IF NOT EXISTS cdc_state_version BIGINT,
    ADD COLUMN IF NOT EXISTS batch_log_id BIGINT,
    ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_attempt_manifest_artifact_status'
          AND conrelid = 'metadata.attempt_persistence_manifest'::regclass
    ) THEN
        ALTER TABLE metadata.attempt_persistence_manifest
            ADD CONSTRAINT chk_attempt_manifest_artifact_status
            CHECK (artifact_status IN ('WRITING', 'VERIFIED', 'CONSUMED', 'SUPERSEDED', 'INVALID'));
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_attempt_manifest_artifact_lookup
    ON metadata.attempt_persistence_manifest
       (table_id, storage_layer, artifact_status, created_at DESC);

-- Existing RECON-01 manifests predate artifact lifecycle fields and are
-- treated as already verified.  Backfill before installing the immutability
-- trigger so this compatibility update is not rejected by that trigger.
UPDATE metadata.attempt_persistence_manifest
SET verified_at = COALESCE(verified_at, created_at)
WHERE artifact_status = 'VERIFIED' AND verified_at IS NULL;

CREATE OR REPLACE FUNCTION metadata.enforce_attempt_manifest_immutability()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'DELETE on metadata.attempt_persistence_manifest is prohibited';
    END IF;

    IF OLD.artifact_status = 'WRITING' AND NEW.artifact_status = 'VERIFIED' THEN
        IF NEW.verified_at IS NULL
           OR (to_jsonb(NEW) - 'artifact_status' - 'verified_at')
              IS DISTINCT FROM
              (to_jsonb(OLD) - 'artifact_status' - 'verified_at') THEN
            RAISE EXCEPTION 'immutable artifact properties cannot change during verification';
        END IF;
        RETURN NEW;
    ELSIF OLD.artifact_status = 'VERIFIED' AND NEW.artifact_status = 'CONSUMED' THEN
        IF (to_jsonb(NEW) - 'artifact_status')
           IS DISTINCT FROM
           (to_jsonb(OLD) - 'artifact_status') THEN
            RAISE EXCEPTION 'immutable artifact properties cannot change during consumption';
        END IF;
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'illegal artifact status transition from % to %',
            OLD.artifact_status, NEW.artifact_status;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_attempt_manifest_immutability
    ON metadata.attempt_persistence_manifest;
CREATE TRIGGER trg_attempt_manifest_immutability
BEFORE UPDATE OR DELETE ON metadata.attempt_persistence_manifest
FOR EACH ROW EXECUTE FUNCTION metadata.enforce_attempt_manifest_immutability();
