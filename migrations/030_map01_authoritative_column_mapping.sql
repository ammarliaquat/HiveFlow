-- ============================================================
-- FILE: migrations/030_map01_authoritative_column_mapping.sql
-- PURPOSE: MAP-01: Establish Identity-Scoped Column Mapping Authority
-- ============================================================

-- ============================================================
-- PHASE A — CERTIFY
-- ============================================================
DO $$
BEGIN
    -- 1. Reject orphan table_ids
    IF EXISTS (
        SELECT 1 FROM metadata.db_columns_etl c
        LEFT JOIN metadata.db_tables_etl t ON c.table_id = t.table_id
        WHERE t.table_id IS NULL
    ) THEN
        RAISE EXCEPTION 'PHASE A FAILED: Orphan table_ids found in db_columns_etl.';
    END IF;

    -- 2. Reject NULL table_ids
    IF EXISTS (
        SELECT 1 FROM metadata.db_columns_etl WHERE table_id IS NULL
    ) THEN
        RAISE EXCEPTION 'PHASE A FAILED: NULL table_ids found in db_columns_etl.';
    END IF;

    -- 3. Reject duplicate active source mappings
    IF EXISTS (
        SELECT table_id, LOWER(source_column)
        FROM metadata.db_columns_etl
        WHERE is_active = 'y'
        GROUP BY table_id, LOWER(source_column)
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'PHASE A FAILED: Duplicate active source mappings found in db_columns_etl.';
    END IF;

    -- 4. Reject target column collisions (multiple source columns mapping to same target identifier)
    IF EXISTS (
        SELECT table_id, COALESCE(LOWER(target_column), LOWER(source_column))
        FROM metadata.db_columns_etl
        WHERE is_active = 'y'
        GROUP BY table_id, COALESCE(LOWER(target_column), LOWER(source_column))
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'PHASE A FAILED: Target column collisions found in db_columns_etl.';
    END IF;
END $$;

-- ============================================================
-- PHASE B — CONSTRAIN
-- ============================================================

-- Ensure table_id is explicitly NOT NULL (already part of schema but enforced here for correctness)
ALTER TABLE metadata.db_columns_etl ALTER COLUMN table_id SET NOT NULL;

-- Drop unconditional uniqueness to allow historical inactive mappings for the same column
ALTER TABLE metadata.db_columns_etl DROP CONSTRAINT IF EXISTS uq_db_columns_etl_source;

-- Add active uniqueness constraint/index (Partial Index)
-- This ensures that for a given table identity, there is exactly one active mapping per source column.
DROP INDEX IF EXISTS metadata.idx_uq_db_columns_etl_active_source;
CREATE UNIQUE INDEX idx_uq_db_columns_etl_active_source 
    ON metadata.db_columns_etl (table_id, LOWER(source_column)) 
    WHERE is_active = 'y';

-- ============================================================
-- PHASE C — REMOVE LEGACY AUTHORITY
-- ============================================================

-- Drop name-based indexes first
DROP INDEX IF EXISTS metadata.idx_etl_cols_active;
DROP INDEX IF EXISTS metadata.idx_etl_cols_tag_table;

-- Drop obsolete source identity columns, forcing lookup by table_id
ALTER TABLE metadata.db_columns_etl
    DROP COLUMN IF EXISTS source_tag,
    DROP COLUMN IF EXISTS source_schema,
    DROP COLUMN IF EXISTS source_table;

-- Add replacement index on table_id to support fast lookups
CREATE INDEX IF NOT EXISTS idx_etl_cols_table_id_active ON metadata.db_columns_etl (table_id, is_active);

-- ============================================================
-- PHASE D — MAPPING POLICY
-- ============================================================

ALTER TABLE metadata.db_tables_etl 
    ADD COLUMN IF NOT EXISTS mapping_policy VARCHAR(50) DEFAULT 'EXPLICIT' NOT NULL;
