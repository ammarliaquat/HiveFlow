-- ============================================================
-- FILE: migrations/001_master_subscriber_schema.sql
-- PURPOSE: Master Metadata Schema DDL — Standardized & Re-ordered Column Hierarchy
-- STRATEGY: DROP + CREATE (clean slate with logical column grouping)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS metadata;

-- ── Step 1: Drop subscriber & audit tables first ──────────────────────────────
DROP TABLE IF EXISTS metadata.db_columns_etl CASCADE;
DROP TABLE IF EXISTS metadata.db_tables_etl  CASCADE;
DROP TABLE IF EXISTS metadata.db_columns     CASCADE;
DROP TABLE IF EXISTS metadata.db_tables      CASCADE;
DROP TABLE IF EXISTS metadata.batch_log       CASCADE;

-- ══════════════════════════════════════════════════════════════════════════════
-- TIER 1: MASTER CATALOG (Crawler-Managed Source Reality)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE metadata.db_tables (
    -- ── 1. Primary Identifiers ────────────────────────────────────────────────
    table_master_key            SERIAL          PRIMARY KEY,
    database_tag                VARCHAR(100)    NOT NULL,
    database_type               VARCHAR(50)     NOT NULL,
    schema_name                 VARCHAR(200)    NOT NULL,
    table_name                  VARCHAR(200)    NOT NULL,
    table_type                  VARCHAR(50)     DEFAULT 'BASE TABLE',

    -- ── 2. Business Key Discovery & Profiling ──────────────────────────────────
    business_key                VARCHAR(200),
    key_source                  VARCHAR(50)     DEFAULT 'PRIMARY_KEY',
    key_confidence              INTEGER         DEFAULT 100,
    duplicate_count             BIGINT          DEFAULT 0,

    -- ── 3. CDC Engine & Strategy Profiling ─────────────────────────────────────
    is_cdc                      CHAR(1)         DEFAULT 'n',
    cdc_columns                 VARCHAR(500),
    cdc_strategy                VARCHAR(50)     DEFAULT 'TIMESTAMP',
    cdc_provider                VARCHAR(50)     DEFAULT 'TIMESTAMP',
    cdc_source                  VARCHAR(50)     DEFAULT 'PROFILED',
    cdc_confidence              INTEGER         DEFAULT 95,
    cdc_health                  VARCHAR(20)     DEFAULT 'OK',
    cdc_null_rows               BIGINT          DEFAULT 0,
    cdc_health_details          TEXT            DEFAULT '{}',
    cdc_profile_date            TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    -- ── 4. Table Statistics & Discovery Metadata ──────────────────────────────
    table_row_count             BIGINT          DEFAULT 0,
    skip_dedup                  VARCHAR(1)      DEFAULT 'n',
    discovered_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    discovered_by               VARCHAR(100),
    last_seen_at                TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    insert_date                 TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_db_tables_catalog UNIQUE (database_tag, schema_name, table_name)
);

CREATE INDEX idx_db_tables_tag ON metadata.db_tables (database_tag);


CREATE TABLE metadata.db_columns (
    column_master_key           SERIAL          PRIMARY KEY,
    table_master_key            INTEGER         NOT NULL REFERENCES metadata.db_tables(table_master_key) ON DELETE CASCADE,
    database_tag                VARCHAR(100)    NOT NULL,
    schema_name                 VARCHAR(200)    NOT NULL,
    table_name                  VARCHAR(200)    NOT NULL,
    column_name                 VARCHAR(200)    NOT NULL,
    data_type                   VARCHAR(100),
    data_length                 INTEGER,
    data_precision              INTEGER,
    data_scale                  INTEGER,
    column_seq                  INTEGER,
    is_nullable                 VARCHAR(1)      DEFAULT 'Y',
    insert_date                 TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_db_columns_catalog UNIQUE (database_tag, schema_name, table_name, column_name)
);

CREATE INDEX idx_db_columns_table_key ON metadata.db_columns (table_master_key);
CREATE INDEX idx_db_columns_tag_table ON metadata.db_columns (database_tag, table_name);


-- ══════════════════════════════════════════════════════════════════════════════
-- TIER 2: SUBSCRIBER CONTROL (Execution Rules — Hand / GUI / Crawler Managed)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE metadata.db_tables_etl (
    -- ── 1. Controls & Status ──────────────────────────────────────────────────
    table_id                    SERIAL          PRIMARY KEY,
    is_active                   CHAR(1)         DEFAULT 'y',
    priority                    INTEGER         DEFAULT 1,
    table_type                  VARCHAR(50)     DEFAULT 'full',
    landing_mode                VARCHAR(50)     DEFAULT 'METADATA',
    schema_evolution_policy     VARCHAR(50)     DEFAULT 'PERMISSIVE',

    -- ── 2. Source System Identity ──────────────────────────────────────────────
    source_tag                  VARCHAR(100)    NOT NULL,
    source_database             VARCHAR(100),
    source_database_type        VARCHAR(50),
    source_schema               VARCHAR(200),
    source_table                VARCHAR(200)    NOT NULL,

    -- ── 3. Target System Identity ──────────────────────────────────────────────
    target_tag                  VARCHAR(100)    NOT NULL,
    target_database             VARCHAR(100),
    target_database_type        VARCHAR(50),
    target_schema               VARCHAR(200),
    target_table                VARCHAR(200),
    landing_table               VARCHAR(200),
    history_table               VARCHAR(200),

    -- ── 4. Natural Business Key & Deduplication ────────────────────────────────
    columns_unique_key          VARCHAR(200),
    key_source                  VARCHAR(50)     DEFAULT 'PRIMARY_KEY',
    key_confidence              INTEGER         DEFAULT 100,
    duplicate_count             BIGINT          DEFAULT 0,
    skip_dedup                  VARCHAR(1)      DEFAULT 'n',

    -- ── 5. Adaptive Partitioning & Parallelism ─────────────────────────────────
    partition_lower_bound       BIGINT          DEFAULT NULL,
    partition_upper_bound       BIGINT          DEFAULT NULL,
    target_rows_per_partition   INTEGER         DEFAULT 250000,
    minimum_partition_threshold INTEGER         DEFAULT 50000,
    maximum_partitions          INTEGER         DEFAULT 32,
    num_partitions_override     INTEGER         DEFAULT NULL,

    -- ── 6. CDC Strategy, Provider & Execution Control ─────────────────────────
    is_cdc                      CHAR(1)         DEFAULT 'n',
    cdc_columns                 VARCHAR(500),
    cdc_strategy                VARCHAR(50)     DEFAULT 'TIMESTAMP',
    cdc_provider                VARCHAR(50)     DEFAULT 'TIMESTAMP',
    cdc_source                  VARCHAR(50)     DEFAULT 'PROFILED',
    cdc_confidence              INTEGER         DEFAULT 95,
    cdc_health                  VARCHAR(20)     DEFAULT 'OK',
    cdc_null_rows               BIGINT          DEFAULT 0,
    cdc_health_details          TEXT            DEFAULT '{}',
    cdc_profile_date            TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    cdc_p_days                  INTEGER         DEFAULT 7,
    delta_days                  INTEGER         DEFAULT 1,
    last_success_timestamp      TIMESTAMP,

    -- ── 7. Reconciliation Rules ────────────────────────────────────────────────
    is_reconcile                CHAR(1)         DEFAULT 'n',
    check_reconcile_column      VARCHAR(200),
    reconcile_measure           VARCHAR(100),
    reconcile_threshold         NUMERIC(5,2)    DEFAULT 0.00,
    reconcile_action            VARCHAR(20)     DEFAULT 'FAIL',

    -- ── 8. Execution Idempotency & Progress Metrics ────────────────────────────
    rerun_flag                  CHAR(1)         DEFAULT 'n',
    last_success_date           TIMESTAMP,
    table_row_count             BIGINT          DEFAULT 0,
    discovered_at               TIMESTAMP,
    discovered_by               VARCHAR(100),
    last_seen_at                TIMESTAMP,

    -- ── 9. Documentation & Audit ───────────────────────────────────────────────
    notes                       TEXT,
    created_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_db_tables_etl_source UNIQUE (source_tag, source_table)
);

CREATE INDEX idx_etl_tables_active ON metadata.db_tables_etl (is_active, priority);
CREATE INDEX idx_etl_tables_cdc ON metadata.db_tables_etl (is_cdc, is_active);
CREATE INDEX idx_etl_tables_tag ON metadata.db_tables_etl (source_tag);


CREATE TABLE metadata.db_columns_etl (
    -- ── 1. Controls & Sequence ────────────────────────────────────────────────
    column_id                   SERIAL          PRIMARY KEY,
    table_id                    INTEGER         NOT NULL REFERENCES metadata.db_tables_etl(table_id) ON DELETE CASCADE,
    is_active                   CHAR(1)         DEFAULT 'y',
    column_sequence             INTEGER,

    -- ── 2. Source Column Contract ──────────────────────────────────────────────
    source_tag                  VARCHAR(100)    NOT NULL,
    source_schema               VARCHAR(200),
    source_table                VARCHAR(200)    NOT NULL,
    source_column               VARCHAR(200)    NOT NULL,
    source_column_datatype      VARCHAR(100),

    -- ── 3. Canonical Type Contract ─────────────────────────────────────────────
    canonical_type              VARCHAR(50),
    is_nullable                 VARCHAR(1)      DEFAULT 'Y',
    max_length                  INTEGER,

    -- ── 4. Target Column Mapping ───────────────────────────────────────────────
    target_column               VARCHAR(200),
    target_column_datatype      VARCHAR(100),
    target_native_type          VARCHAR(100),
    target_column_length        INTEGER,

    -- ── 5. Audit & Governance ──────────────────────────────────────────────────
    created_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    classification_level        VARCHAR(50),
    log_masking                 CHAR(1),
    display_masking             CHAR(1),
    export_masking              CHAR(1),
    schema_version              INTEGER,
    effective_from              TIMESTAMP,
    retired_at                  TIMESTAMP,

    CONSTRAINT uq_db_columns_etl_source UNIQUE (table_id, source_column)
);

CREATE INDEX idx_etl_cols_active ON metadata.db_columns_etl (source_table, is_active);
CREATE INDEX idx_etl_cols_tag_table ON metadata.db_columns_etl (source_tag, source_table, is_active);


-- ══════════════════════════════════════════════════════════════════════════════
-- TIER 3: AUDIT & PROGRESS TELEMETRY (Execution State Model)
-- ══════════════════════════════════════════════════════════════════════════════

CREATE TABLE metadata.batch_log (
    -- ── 1. Identifiers & References ───────────────────────────────────────────
    id                          SERIAL          PRIMARY KEY,
    run_id                      UUID            NOT NULL,
    spark_app_id                VARCHAR(200),

    -- ── 2. Context & Identity ──────────────────────────────────────────────────
    source_tag                  VARCHAR(100),
    target_tag                  VARCHAR(100),
    source_table                VARCHAR(200),
    target_table                VARCHAR(200),
    load_type                   VARCHAR(50),
    priority                    INTEGER,

    -- ── 3. Execution State & Phase ────────────────────────────────────────────
    status                      VARCHAR(30)     DEFAULT 'RUNNING',
    current_phase               VARCHAR(50)     DEFAULT 'PENDING',

    -- ── 4. Progress & Row Metrics ──────────────────────────────────────────────
    source_count                BIGINT          DEFAULT NULL,
    processed_count             BIGINT          DEFAULT NULL,
    target_count                BIGINT          DEFAULT NULL,
    progress_pct                NUMERIC(5,2)    DEFAULT NULL,
    load_pct                    NUMERIC(5,2)    DEFAULT NULL,
    rows_per_second             NUMERIC(12,2)   DEFAULT NULL,

    -- ── 5. Timestamps & Performance Telemetry ─────────────────────────────────
    start_time                  TIMESTAMP,
    end_time                    TIMESTAMP,
    elapsed_seconds             NUMERIC(10,3),
    estimated_completion        TIMESTAMP,
    last_progress_update        TIMESTAMP,

    -- ── 6. Diagnostics & Audit ────────────────────────────────────────────────
    error_message               TEXT,
    notes                       TEXT
);

CREATE INDEX idx_batch_log_run_id ON metadata.batch_log (run_id);
CREATE INDEX idx_batch_log_status ON metadata.batch_log (status, source_tag);
