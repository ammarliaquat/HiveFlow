-- ============================================================================
-- Migration 004: Restructure db_tables_etl
-- ============================================================================
-- Changes:
--   1. Add source_database_type (mysql, postgres, etc.)
--   2. Add target_database_type (mysql, postgres, etc.)
--   3. Remove 'lnd_' and 'hist_' prefixes from landing_table / history_table
--   4. Reorder columns: all source columns -> row_count -> all target columns
--   5. Consolidate all prior schema additions (002, 003, old 004)
-- ============================================================================

BEGIN;

-- ── Step 1: Create replacement table with canonical column order ──────────

CREATE TABLE metadata.db_tables_etl_new (
    table_id                SERIAL          PRIMARY KEY,

    -- ── Source Identity (all source columns first) ──────────────────────────
    source_tag              VARCHAR(100)    NOT NULL,
    source_database         VARCHAR(100),
    source_database_type    VARCHAR(50),             -- 'mysql', 'postgres', 'oracle', 'mssql'
    source_schema           VARCHAR(200),
    source_table            VARCHAR(200)    NOT NULL,
    is_active               CHAR(1)         DEFAULT 'y',
    cdc_columns             VARCHAR(500),            -- Inherited from db_tables.cdc_columns
    columns_unique_key      VARCHAR(200),            -- Inherited from db_tables.business_key

    -- ── Row Count ───────────────────────────────────────────────────────────
    table_row_count         BIGINT          DEFAULT 0,

    -- ── Target Mapping (all target columns next) ────────────────────────────
    target_tag              VARCHAR(100)    NOT NULL, -- Connection section in databases.ini
    target_database         VARCHAR(100),             -- Resolved at promotion time
    target_database_type    VARCHAR(50),             -- Resolved at promotion time
    target_schema           VARCHAR(200),             -- Resolved at promotion time
    target_table            VARCHAR(200),             -- Defaults to source_table
    landing_table           VARCHAR(200),             -- Bronze layer table name (no prefix)
    history_table           VARCHAR(200),             -- Silver layer table name (no prefix)

    -- ── ETL Execution Rules ─────────────────────────────────────────────────
    table_type              VARCHAR(50)     DEFAULT 'full',
    priority                INTEGER         DEFAULT 1,
    is_cdc                  CHAR(1)         DEFAULT 'n',
    num_partitions_override INTEGER         DEFAULT NULL,

    -- ── CDC Runtime State ───────────────────────────────────────────────────
    cdc_p_days              INTEGER         DEFAULT 7,
    delta_days              INTEGER         DEFAULT 1,
    last_success_timestamp  TIMESTAMP,

    -- ── Reconciliation ──────────────────────────────────────────────────────
    is_reconcile            CHAR(1)         DEFAULT 'n',
    check_reconcile_column  VARCHAR(200),
    reconcile_measure       VARCHAR(100),
    reconcile_threshold     NUMERIC(5,2)    DEFAULT 0.00,
    reconcile_action        VARCHAR(20)     DEFAULT 'FAIL',

    -- ── Idempotency ─────────────────────────────────────────────────────────
    rerun_flag              CHAR(1)         DEFAULT 'n',
    last_success_date       DATE,

    -- ── Descriptive ─────────────────────────────────────────────────────────
    notes                   TEXT,

    -- ── Discovery Tracking ──────────────────────────────────────────────────
    discovered_at           TIMESTAMP,
    discovered_by           VARCHAR(100),
    last_seen_at            TIMESTAMP,

    -- ── Audit ───────────────────────────────────────────────────────────────
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_db_tables_etl_source_new
        UNIQUE (source_tag, source_table)
);


-- ── Step 2: Copy data from old table → new table with prefix removal ────────

INSERT INTO metadata.db_tables_etl_new (
    table_id,
    source_tag, source_database, source_database_type, source_schema, source_table,
    is_active, cdc_columns, columns_unique_key,
    table_row_count,
    target_tag, target_database, target_database_type, target_schema, target_table,
    landing_table, history_table,
    table_type, priority, is_cdc, num_partitions_override,
    cdc_p_days, delta_days, last_success_timestamp,
    is_reconcile, check_reconcile_column, reconcile_measure,
    reconcile_threshold, reconcile_action,
    rerun_flag, last_success_date,
    notes,
    discovered_at, discovered_by, last_seen_at,
    created_at, updated_at
)
SELECT
    old.table_id,
    old.source_tag,
    old.source_database,
    COALESCE(old.source_database_type, m.database_type),
    old.source_schema,
    old.source_table,
    old.is_active,
    old.cdc_columns,
    old.columns_unique_key,
    COALESCE(old.table_row_count, 0),
    COALESCE(old.target_tag, 'land_n'),
    old.target_database,
    'postgres', -- Target connection type for land_n
    old.target_schema,
    old.target_table,
    -- Strip 'lnd_' and 'hist_' prefixes from landing/history table fields
    CASE WHEN old.landing_table LIKE 'lnd_%' THEN SUBSTRING(old.landing_table FROM 5) ELSE old.landing_table END,
    CASE WHEN old.history_table LIKE 'hist_%' THEN SUBSTRING(old.history_table FROM 6) ELSE old.history_table END,
    old.table_type,
    old.priority,
    old.is_cdc,
    old.num_partitions_override,
    old.cdc_p_days,
    old.delta_days,
    old.last_success_timestamp,
    old.is_reconcile,
    old.check_reconcile_column,
    old.reconcile_measure,
    COALESCE(old.reconcile_threshold, 0.00),
    COALESCE(old.reconcile_action, 'FAIL'),
    COALESCE(old.rerun_flag, 'n'),
    old.last_success_date,
    old.notes,
    old.discovered_at,
    old.discovered_by,
    old.last_seen_at,
    old.created_at,
    old.updated_at
FROM metadata.db_tables_etl old
LEFT JOIN metadata.db_tables m
       ON old.source_tag   = m.database_tag
      AND old.source_table = m.table_name;


-- ── Step 3: Sync the SERIAL sequence to max existing table_id ───────────────

SELECT setval(
    pg_get_serial_sequence('metadata.db_tables_etl_new', 'table_id'),
    COALESCE((SELECT MAX(table_id) FROM metadata.db_tables_etl_new), 0)
);


-- ── Step 4: Drop old table, rename new ──────────────────────────────────────

DROP TABLE metadata.db_tables_etl CASCADE;

ALTER TABLE metadata.db_tables_etl_new
    RENAME TO db_tables_etl;

ALTER TABLE metadata.db_tables_etl
    RENAME CONSTRAINT uq_db_tables_etl_source_new TO uq_db_tables_etl_source;


-- ── Step 5: Recreate indexes ────────────────────────────────────────────────

CREATE INDEX idx_etl_tables_active
    ON metadata.db_tables_etl (is_active, priority);

CREATE INDEX idx_etl_tables_cdc
    ON metadata.db_tables_etl (is_cdc, is_active);

CREATE INDEX idx_etl_tables_tag
    ON metadata.db_tables_etl (source_tag);

CREATE INDEX idx_etl_skip_logic
    ON metadata.db_tables_etl (is_active, rerun_flag, last_success_date);

CREATE INDEX idx_etl_discovered
    ON metadata.db_tables_etl (discovered_at, is_active);


-- ── Step 6: Recreate db_columns_etl FK ──────────────────────────────────────

ALTER TABLE metadata.db_columns_etl
    ADD CONSTRAINT fk_columns_etl_table_id
        FOREIGN KEY (table_id)
        REFERENCES metadata.db_tables_etl(table_id)
        ON DELETE CASCADE;


-- ── Step 7: Comments ────────────────────────────────────────────────────────

COMMENT ON TABLE  metadata.db_tables_etl                         IS 'Subscriber Control: tables the pipeline processes. Managed by promote_table.py';
COMMENT ON COLUMN metadata.db_tables_etl.source_database_type    IS 'Source DB engine type: mysql, postgres, oracle, mssql. Inherited from db_tables.database_type';
COMMENT ON COLUMN metadata.db_tables_etl.target_database_type    IS 'Target DB engine type: postgres, etc. Resolved from databases.ini';
COMMENT ON COLUMN metadata.db_tables_etl.cdc_columns             IS 'Inherited from db_tables.cdc_columns. Drives is_cdc flag';
COMMENT ON COLUMN metadata.db_tables_etl.columns_unique_key      IS 'Inherited from db_tables.business_key. Used for Delta MERGE and parallel JDBC read';
COMMENT ON COLUMN metadata.db_tables_etl.table_type              IS 'full=overwrite, incremental=append with filter, cdc=delta merge';
COMMENT ON COLUMN metadata.db_tables_etl.num_partitions_override IS 'Manual override for JDBC numPartitions. NULL = auto-calculated from table_row_count';
COMMENT ON COLUMN metadata.db_tables_etl.target_tag              IS 'databases.ini section name for target DB connection. Source of truth for target_database/target_schema';

COMMIT;
