-- ==============================================================================
-- Migration: 013_governance_separation_v3.sql
-- Purpose: Tighten Metadata OS boundaries: separate execution from governance,
--          rename back to is_active_version with partial unique index, simplify scoring
--          (HIGH/MEDIUM/LOW + reasons), and enforce audit check constraints.
-- ==============================================================================

-- 0. Rename is_production_version -> is_active_version if it exists from earlier run
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'metadata' AND table_name = 'table_recommendations' AND column_name = 'is_production_version'
    ) THEN
        ALTER TABLE metadata.table_recommendations RENAME COLUMN is_production_version TO is_active_version;
    END IF;
END $$;

-- 1. Extend table_recommendations with versioning, simplified scoring, and audit fields
ALTER TABLE metadata.table_recommendations
    ADD COLUMN IF NOT EXISTS version_num             INTEGER      NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS is_active_version       BOOLEAN      NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS confidence_level        VARCHAR(20)  DEFAULT 'MEDIUM', -- 'HIGH', 'MEDIUM', 'LOW'
    ADD COLUMN IF NOT EXISTS confidence_reasons      JSONB        DEFAULT '[]',     -- ["TIMESTAMP datatype", "Indexed", "NOT NULL"]
    ADD COLUMN IF NOT EXISTS steward_user            VARCHAR(100),
    ADD COLUMN IF NOT EXISTS steward_notes           TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_at             TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_verified_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP;

-- Backfill confidence_level from legacy confidence_score if it exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'metadata' AND table_name = 'table_recommendations' AND column_name = 'confidence_score'
    ) THEN
        UPDATE metadata.table_recommendations
        SET confidence_level = CASE 
            WHEN confidence_score >= 80 THEN 'HIGH'
            WHEN confidence_score >= 50 THEN 'MEDIUM'
            ELSE 'LOW'
        END
        WHERE confidence_level IS NULL OR confidence_level = 'MEDIUM';
        
        ALTER TABLE metadata.table_recommendations
            DROP COLUMN IF EXISTS confidence_score,
            DROP COLUMN IF EXISTS confidence_breakdown;
    END IF;
END $$;

-- Backfill audit provenance for existing APPROVED / OVERRIDDEN rows before enforcing constraint
UPDATE metadata.table_recommendations
SET steward_user = COALESCE(steward_user, 'migration_backfill'),
    reviewed_at  = COALESCE(reviewed_at, CURRENT_TIMESTAMP)
WHERE recommendation_status IN ('APPROVED', 'OVERRIDDEN')
  AND (steward_user IS NULL OR reviewed_at IS NULL);

-- 2. Enforce Check Constraint: approved/overridden recommendations must have audit provenance
ALTER TABLE metadata.table_recommendations
    DROP CONSTRAINT IF EXISTS check_steward_audit;
ALTER TABLE metadata.table_recommendations
    ADD CONSTRAINT check_steward_audit
    CHECK (
        (recommendation_status NOT IN ('APPROVED', 'OVERRIDDEN')) 
        OR (reviewed_at IS NOT NULL AND steward_user IS NOT NULL)
    );

-- 3. Enforce Partial Unique Index: exactly one active version per table
DROP INDEX IF EXISTS metadata.idx_prod_rec_per_table;
DROP INDEX IF EXISTS metadata.idx_active_rec_per_table;
CREATE UNIQUE INDEX IF NOT EXISTS idx_active_rec_per_table 
    ON metadata.table_recommendations (table_id) 
    WHERE is_active_version = TRUE;

-- 4. Clean up db_tables_etl: remove governance/approval columns so it stores execution metadata only
ALTER TABLE metadata.db_tables_etl
    DROP COLUMN IF EXISTS governance_status,
    DROP COLUMN IF EXISTS approved_cdc_column,
    DROP COLUMN IF EXISTS approved_cdc_type,
    DROP COLUMN IF EXISTS approved_by,
    DROP COLUMN IF EXISTS approved_at;

-- 5. Add explicit scope guardrail comments enforcing bounded context and immutability
COMMENT ON TABLE metadata.table_recommendations IS 'Stores ONLY discovery recommendations for CDC strategy and table classification. Do NOT add operational tuning, compression, partitioning, or generic recommendations here. For other domains, create separate bounded contexts.';
COMMENT ON COLUMN metadata.table_recommendations.is_active_version IS 'True for the active recommendation consumed by UI, APIs, and promotion. Only stewardship actions (Approve/Override) transition this flag.';
COMMENT ON COLUMN metadata.table_recommendations.confidence_reasons IS 'Immutable discovery observations justifying confidence level. Only Discovery writes this; Stewards edit notes instead.';
