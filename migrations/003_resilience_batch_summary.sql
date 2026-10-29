-- ============================================================================
-- Migration 003: Resilience & Batch Summary
-- ============================================================================
-- Adds:
--   1. metadata.batch_summary  — rollup table (one row per run + layer)
--   2. num_partitions_override — manual partition override on db_tables_etl
--   3. target_tag              — databases.ini section for target DB connection
-- ============================================================================

BEGIN;

-- ── 1. Batch Summary Table ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS metadata.batch_summary (
    id              BIGSERIAL    PRIMARY KEY,
    run_id          UUID         NOT NULL,
    source_tag      VARCHAR(100),
    target_tag      VARCHAR(100),
    layer           VARCHAR(20)  NOT NULL,   -- 'bronze', 'silver', 'gold'
    start_ts        TIMESTAMP(3) NOT NULL,
    end_ts          TIMESTAMP(3),
    total_tables    INTEGER      DEFAULT 0,
    succeeded       INTEGER      DEFAULT 0,
    failed          INTEGER      DEFAULT 0,
    skipped         INTEGER      DEFAULT 0,
    total_rows      BIGINT       DEFAULT 0,
    status          VARCHAR(20),             -- 'COMPLETED', 'PARTIAL', 'FAILED'
    error_summary   TEXT,
    created_at      TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bs_run_id   ON metadata.batch_summary(run_id);
CREATE INDEX IF NOT EXISTS idx_bs_start_ts ON metadata.batch_summary(start_ts);

COMMENT ON TABLE metadata.batch_summary IS
    'Per-run, per-layer rollup of pipeline execution. '
    'Query with WHERE run_id = ? to see how an entire run performed.';


-- ── 2. Manual Partition Override ────────────────────────────────────────────

ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS num_partitions_override INTEGER DEFAULT NULL;

COMMENT ON COLUMN metadata.db_tables_etl.num_partitions_override IS
    'Manual override for JDBC numPartitions. NULL = use auto-calculated value '
    'derived from table_row_count (1 partition per 50k rows, min 5, max 200).';


-- ── 3. Target Connection Tag ────────────────────────────────────────────────
-- Mirrors source_tag for target-side lookups.
-- Without this, conn_manager.get(target_schema) fails with KeyError
-- because target_schema holds a schema name like 'land', not an INI
-- section name like 'land_n'.

ALTER TABLE metadata.db_tables_etl
    ADD COLUMN IF NOT EXISTS target_tag VARCHAR(100) DEFAULT NULL;

COMMENT ON COLUMN metadata.db_tables_etl.target_tag IS
    'databases.ini section name for target DB connection (e.g. "land_n"). '
    'NULL = file-based Delta/Parquet target. Mirrors source_tag pattern.';


COMMIT;
