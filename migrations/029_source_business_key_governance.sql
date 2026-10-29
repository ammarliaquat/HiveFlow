-- KEY-01: separate discovered source-key evidence from approved execution identity.
-- Zero-trust migration: no legacy key is backfilled or approved.

ALTER TABLE metadata.db_tables
    ALTER COLUMN key_source DROP DEFAULT,
    ALTER COLUMN key_confidence DROP DEFAULT,
    ADD COLUMN IF NOT EXISTS key_candidate_columns JSONB,
    ADD COLUMN IF NOT EXISTS key_evidence_type VARCHAR(50),
    ADD COLUMN IF NOT EXISTS key_candidate_nullable BOOLEAN,
    ADD COLUMN IF NOT EXISTS key_candidate_compatible BOOLEAN,
    ADD COLUMN IF NOT EXISTS key_evidence_fingerprint VARCHAR(64);

ALTER TABLE metadata.db_tables_etl
    ALTER COLUMN key_source DROP DEFAULT,
    ALTER COLUMN key_confidence DROP DEFAULT,
    ADD COLUMN IF NOT EXISTS source_table_master_key INTEGER,
    ADD COLUMN IF NOT EXISTS key_approval_state VARCHAR(20),
    ADD COLUMN IF NOT EXISTS approved_source_business_key JSONB,
    ADD COLUMN IF NOT EXISTS approved_key_evidence VARCHAR(50),
    ADD COLUMN IF NOT EXISTS approved_key_evidence_fingerprint VARCHAR(64),
    ADD COLUMN IF NOT EXISTS key_approved_by VARCHAR(200),
    ADD COLUMN IF NOT EXISTS key_approved_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS key_approval_revision INTEGER;

-- Link legacy execution rows only when their physical source identity is exact.
-- This does not copy or approve any legacy key value.
UPDATE metadata.db_tables_etl etl
SET source_table_master_key = master.table_master_key
FROM metadata.db_tables master
WHERE etl.source_table_master_key IS NULL
  AND etl.source_tag = master.database_tag
  AND etl.source_schema = master.schema_name
  AND etl.source_table = master.table_name;

CREATE UNIQUE INDEX IF NOT EXISTS uq_db_tables_etl_source_master
    ON metadata.db_tables_etl(source_table_master_key)
    WHERE source_table_master_key IS NOT NULL;

CREATE OR REPLACE FUNCTION metadata.is_valid_key_column_array(value JSONB)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    element JSONB;
    normalized TEXT;
    seen TEXT[] := ARRAY[]::TEXT[];
BEGIN
    IF value IS NULL OR jsonb_typeof(value) <> 'array' OR jsonb_array_length(value) = 0 THEN
        RETURN FALSE;
    END IF;
    FOR element IN SELECT * FROM jsonb_array_elements(value)
    LOOP
        IF jsonb_typeof(element) <> 'string' THEN
            RETURN FALSE;
        END IF;
        normalized := lower(btrim(element #>> '{}'));
        IF normalized = '' OR normalized = ANY(seen) THEN
            RETURN FALSE;
        END IF;
        seen := array_append(seen, normalized);
    END LOOP;
    RETURN TRUE;
END;
$$;

ALTER TABLE metadata.db_tables_etl
    DROP CONSTRAINT IF EXISTS fk_db_tables_etl_source_master,
    ADD CONSTRAINT fk_db_tables_etl_source_master
        FOREIGN KEY (source_table_master_key)
        REFERENCES metadata.db_tables(table_master_key),
    DROP CONSTRAINT IF EXISTS ck_db_tables_etl_key_approval_state,
    ADD CONSTRAINT ck_db_tables_etl_key_approval_state
        CHECK (key_approval_state IS NULL OR key_approval_state IN ('APPROVED', 'UNAPPROVED', 'REJECTED')),
    DROP CONSTRAINT IF EXISTS ck_db_tables_etl_approved_key_shape,
    ADD CONSTRAINT ck_db_tables_etl_approved_key_shape
        CHECK (
            approved_source_business_key IS NULL
            OR metadata.is_valid_key_column_array(approved_source_business_key)
        ),
    DROP CONSTRAINT IF EXISTS ck_db_tables_etl_approved_key_contract,
    ADD CONSTRAINT ck_db_tables_etl_approved_key_contract
        CHECK (
            (
                key_approval_state = 'APPROVED'
                AND
                approved_source_business_key IS NOT NULL
                AND approved_key_evidence IS NOT NULL
                AND approved_key_evidence_fingerprint IS NOT NULL
                AND key_approved_by IS NOT NULL
                AND key_approved_at IS NOT NULL
                AND key_approval_revision > 0
                AND source_table_master_key IS NOT NULL
            )
            OR (
                key_approval_state IS DISTINCT FROM 'APPROVED'
                AND approved_source_business_key IS NULL
                AND approved_key_evidence IS NULL
                AND approved_key_evidence_fingerprint IS NULL
                AND key_approved_by IS NULL
                AND key_approved_at IS NULL
                AND key_approval_revision IS NULL
            )
        );

CREATE OR REPLACE FUNCTION metadata.prevent_approved_key_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    evidence RECORD;
BEGIN
    IF OLD.key_approval_state = 'APPROVED' AND (
        NEW.source_table_master_key IS DISTINCT FROM OLD.source_table_master_key
        OR NEW.key_approval_state IS DISTINCT FROM OLD.key_approval_state
        OR NEW.approved_source_business_key IS DISTINCT FROM OLD.approved_source_business_key
        OR NEW.approved_key_evidence IS DISTINCT FROM OLD.approved_key_evidence
        OR NEW.approved_key_evidence_fingerprint IS DISTINCT FROM OLD.approved_key_evidence_fingerprint
        OR NEW.key_approved_by IS DISTINCT FROM OLD.key_approved_by
        OR NEW.key_approved_at IS DISTINCT FROM OLD.key_approved_at
        OR NEW.key_approval_revision IS DISTINCT FROM OLD.key_approval_revision
    ) THEN
        RAISE EXCEPTION 'Approved source business keys are immutable; create a governed replacement approval';
    END IF;

    IF NEW.key_approval_state = 'APPROVED' THEN
        IF OLD.key_approval_state IS DISTINCT FROM 'APPROVED'
           AND NEW.key_approval_revision <= COALESCE(OLD.key_approval_revision, 0) THEN
            RAISE EXCEPTION 'Key approval revision must increase';
        END IF;
        SELECT key_candidate_columns, key_evidence_type,
               key_candidate_nullable, key_candidate_compatible,
               key_evidence_fingerprint
          INTO evidence
          FROM metadata.db_tables
         WHERE table_master_key = NEW.source_table_master_key;
        IF NOT FOUND
           OR evidence.key_candidate_columns IS DISTINCT FROM NEW.approved_source_business_key
           OR evidence.key_evidence_type IS DISTINCT FROM NEW.approved_key_evidence
           OR evidence.key_evidence_fingerprint IS DISTINCT FROM NEW.approved_key_evidence_fingerprint
           OR evidence.key_candidate_nullable IS DISTINCT FROM FALSE
           OR evidence.key_candidate_compatible IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION 'Approval must match current non-nullable compatible source-key evidence';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_db_tables_etl_approved_key_immutable ON metadata.db_tables_etl;
CREATE TRIGGER trg_db_tables_etl_approved_key_immutable
BEFORE INSERT OR UPDATE OF source_table_master_key, key_approval_state,
    approved_source_business_key, approved_key_evidence,
    approved_key_evidence_fingerprint, key_approved_by, key_approved_at,
    key_approval_revision
ON metadata.db_tables_etl
FOR EACH ROW EXECUTE FUNCTION metadata.prevent_approved_key_mutation();

COMMENT ON COLUMN metadata.db_tables.key_candidate_columns IS
    'Discovery evidence only. Never authorizes execution.';
COMMENT ON COLUMN metadata.db_tables_etl.approved_source_business_key IS
    'Ordered source columns authorized by KEY-01. Does not authorize target-keyed writes.';
COMMENT ON COLUMN metadata.db_tables_etl.key_approval_state IS
    'Independent approval state. NULL legacy rows are unapproved.';
