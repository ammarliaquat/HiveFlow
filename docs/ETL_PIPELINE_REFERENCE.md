# Generic Metadata-Driven PySpark ETL Pipeline Architecture Reference (v3.3.0)

> **Purpose:** Single source of truth for the generic, metadata-driven PySpark ETL pipeline architecture. Defines the system design, relational metadata schemas, Medallion layer contracts, security controls, Streamlit operations dashboard (GUI), and resilience rules.

---

## Table of Contents

1. [Architecture Philosophy & Core Principles](#1-architecture-philosophy--core-principles)
2. [Folder Structure](#2-folder-structure)
3. [Three-Layer Medallion Architecture](#3-three-layer-medallion-architecture)
4. [Relational Metadata Catalog Schemas (PostgreSQL)](#4-relational-metadata-catalog-schemas-postgresql)
5. [Core System Components & Responsibilities](#5-core-system-components--responsibilities)
6. [Configuration Management & Security Controls](#6-configuration-management--security-controls)
7. [CDC, Watermarks & Incremental Synchronization](#7-cdc-watermarks--incremental-synchronization)
8. [Reconciliation, Idempotency & Overflow Guards](#8-reconciliation-idempotency--overflow-guards)
9. [Streamlit Operations Console (GUI) Subsystem](#9-streamlit-operations-console-gui-subsystem)

---

## 1. Architecture Philosophy & Core Principles

The pipeline never hardcodes table names, source connection strings, column definitions, or transformation rules in Python code. All execution instructions live in a **PostgreSQL metadata database**. Adding a new table to the pipeline requires only inserting a row in the metadata catalog—zero code changes required.

### Core Architectural Principles

- **P1: Metadata-Driven Execution:** Pipeline execution rules (watermarks, CDC strategies, partition bounds, target schemas) are driven dynamically by control tables (`metadata.db_tables_etl`).
- **P2: Self-Contained Metadata Rows ([ADR-001](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-001_controlled_denormalization.md)):** `MetadataRow` contains all table/connection attributes to avoid runtime database joins during high-throughput Spark operations.
- **P3: Two-Tier Catalog Architecture ([ADR-003](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-003_two_tier_catalog.md)):** Automated crawlers write exclusively to Tier 1 (`metadata.db_tables`). Tier 2 (`metadata.db_tables_etl`) activation requires human steward promotion via `jobs/promote_table.py` or GUI Page 07.
- **P4: Decoupled DDL Schema Execution:** Target table DDL schema creation and column migrations run deterministically during pre-flight preparation, keeping runtime write streams strictly DML (`INSERT` / `MERGE` / `UPSERT`).
- **P5: Idempotency & Reconciliation Overflow Guard ([ADR-004](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-004_reconciliation_overflow_guard.md)):** All executions are idempotent. Count mismatches flag automatic retry (`rerun_flag='y'`) without updating `last_success_date`.
- **P6: Centralized Stream Security & PII Redaction:** Outbound telemetry and tracebacks automatically redact PII patterns via `GovernedLogFormatter`. Password environment variables enforce non-fallback policies (`ConfigurationError`).
- **P7: Single Extraction Architecture ([ADR-002](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-002_parquet_bronze_format.md)):** Extracting from source databases occurs exactly once per run into Bronze Parquet files. Downstream Silver history deduplication reads landed Bronze Parquet directly without re-querying source databases.

---

## 2. Folder Structure

```text
pyspark_pipeline/
├── config/
│   ├── config.yaml                ← Base pipeline paths, settings, database_ini_path reference
│   ├── config.local.yaml          ← Local development overrides
│   └── databases.ini              ← Multi-tenant database connection profiles (${ENV_VAR})
│
├── src/
│   ├── constants.py               ← Centralized magic strings, statuses, and column names
│   ├── spark_session.py           ← SparkSession factory (supports Spark Connect)
│   │
│   ├── connections/
│   │   └── connection_manager.py  ← INI parser, role enforcement & non-fallback credentials
│   ├── metadata/
│   │   ├── metadata_reader.py     ← Reads db_tables_etl from PostgreSQL
│   │   └── metadata_models.py     ← Self-contained MetadataRow dataclass
│   ├── extract/
│   │   └── extractor.py           ← JDBC source reader with adaptive partitioning
│   ├── transform/
│   │   ├── transformer.py         ← Dedup, casting, normalized composite key formatting
│   │   └── column_mapper.py       ← Applies column renaming and canonical type maps
│   ├── load/
│   │   └── loader.py              ← Writes to Bronze/Silver/Gold; pre-flight DDL provisioning
│   ├── validation/
│   │   └── reconciler.py          ← Row count discrepancy checks & threshold enforcement
│   ├── discovery/
│   │   └── type_mapper.py         ← Source datatype mapping & CDC strategy profiling
│   ├── cdc/
│   │   └── cdc_handler.py         ← Watermark management & Delta MERGE logic
│   ├── security/
│   │   └── data_governance.py     ← GovernedLogFormatter PII stream redaction wrapper
│   ├── logging_/
│   │   └── batch_logger.py        ← Telemetry logging into PostgreSQL metadata.batch_log
│   └── orchestrator/
│       └── etl.py                 ← Core ETL engine, DAG execution & retry guards
│
├── gui/                           ← Streamlit Operations Console Subsystem (13 Pages)
│   ├── app.py                     ← Streamlit main entry point (landing page)
│   ├── requirements.txt           ← GUI dependencies (streamlit, plotly, sqlalchemy)
│   ├── pages/                     ← 13 operational pages (Dashboard, Explorer, History, Triggers, etc.)
│   ├── services/                  ← Database & metadata query abstraction layer
│   └── components/                ← Metric cards, tables, charts, badges, governance cards
│
├── jobs/
│   ├── app_etl.py                 ← ★ Unified CLI entry point (run_pipeline, sync, fix, healthcheck)
│   ├── promote_table.py           ← Human steward catalog promotion script (ADR-003)
│   ├── auto_configure_target.py    ← Pre-flight target DDL generator (Change C.1)
│   ├── discover_and_promote.py    ← Catalog schema discovery crawler
│   └── review_discovered.py       ← Discovery review interface
│
└── tests/                         ← Automated PyTest test suite (44/44 passing)
```

---

## 3. Three-Layer Medallion Architecture

```text
SOURCE SYSTEMS                  MEDALLION PIPELINE LAYERS               TARGET CONSUMERS
──────────────                  ─────────────────────────               ────────────────
Oracle DB         ──JDBC──►    BRONZE (Landing)         ──────────►    Spark SQL
SQL Server        ──JDBC──►    raw Parquet files                      dbt models
PostgreSQL        ──JDBC──►    /data/landing/                         BI Dashboards
MySQL DB          ──JDBC──►    source_tag/table/dt=date/              Reporting Models
                                    │
                                    ▼
                               SILVER (History)
                               deduplicated, typed,
                               audit columns added
                               /data/history/
                                    │
                                    ▼
                               GOLD (Target)
                               incremental CDC
                               PostgreSQL UPSERT / Delta
                               /data/target/
```

### Layer Contracts

| Layer | Alias | Storage Format | Contract & Responsibilities |
|-------|-------|----------------|-----------------------------|
| **Bronze** | Landing | Snappy Parquet ([**ADR-002**](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-002_parquet_bronze_format.md)) | Immutable, raw snapshot of source data. No transformations applied. Exact source replica partitioned by date. |
| **Silver** | History | Parquet / Delta | Deduplicated on business natural keys (`normalized_natural_key`), typed & cast, lineage audit columns appended (`row_hash`, `_dw_key`). |
| **Gold** | Target | PostgreSQL / Delta MERGE | Business-ready analytics models. High-throughput DML (`INSERT` / `MERGE` / `UPSERT`). Target table DDL schema provisioning runs during pre-flight. |

---

## 4. Relational Metadata Catalog Schemas (PostgreSQL)

### 4.1 Master Catalog: `metadata.db_tables` (Tier 1 Source Reality)
Stores structural catalog facts discovered by automated crawlers. Read-only to ETL pipelines.

```sql
CREATE TABLE metadata.db_tables (
    table_master_key    SERIAL          PRIMARY KEY,
    database_tag        VARCHAR(100)    NOT NULL,
    database_type       VARCHAR(50)     NOT NULL,
    schema_name         VARCHAR(200)    NOT NULL,
    table_name          VARCHAR(200)    NOT NULL,
    table_type          VARCHAR(50),
    cdc_columns         VARCHAR(500),
    business_key        VARCHAR(200),
    table_row_count     BIGINT          DEFAULT 0,
    insert_date         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_db_tables_catalog UNIQUE (database_tag, schema_name, table_name)
);
```

### 4.2 Master Catalog: `metadata.db_columns` (Tier 1 Source Column Catalog)
Stores ordinal column positions, data types, lengths, and nullability discovered in source systems.

```sql
CREATE TABLE metadata.db_columns (
    column_master_key   SERIAL          PRIMARY KEY,
    table_master_key    INTEGER         NOT NULL REFERENCES metadata.db_tables(table_master_key) ON DELETE CASCADE,
    database_tag        VARCHAR(100)    NOT NULL,
    schema_name         VARCHAR(200)    NOT NULL,
    table_name          VARCHAR(200)    NOT NULL,
    column_name         VARCHAR(200)    NOT NULL,
    data_type           VARCHAR(100),
    data_length         INTEGER,
    data_precision      INTEGER,
    data_scale          INTEGER,
    column_seq          INTEGER,
    is_nullable         VARCHAR(1)      DEFAULT 'Y',
    insert_date         TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_db_columns_catalog UNIQUE (database_tag, schema_name, table_name, column_name)
);
```

### 4.3 Subscriber Control: `metadata.db_tables_etl` (Tier 2 Execution Control Plane)
Defines active table ingestion rules, execution priorities, CDC strategies, and idempotency status.

```sql
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
```

### 4.4 Subscriber Column Mapping: `metadata.db_columns_etl` (Tier 2 Column Contracts)
Defines active column selection, canonical datatype mappings, target column names, and PII masking rules.

```sql
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
```

### 4.5 Batch Audit Log: `metadata.batch_log` (Execution Telemetry)
Logs execution metrics (`source_count`, `target_count`, `gap`, `elapsed_seconds`, `status`) per table run ID.

```sql
CREATE TABLE metadata.batch_log (
    id                  BIGSERIAL       PRIMARY KEY,
    run_id              UUID            NOT NULL,
    source_tag          VARCHAR(100),
    source_table        VARCHAR(200),
    target_table        VARCHAR(200),
    load_type           VARCHAR(50),
    priority            INTEGER,
    source_count        BIGINT,
    target_count        BIGINT,
    gap                 BIGINT GENERATED ALWAYS AS (source_count - target_count) STORED,
    load_pct            NUMERIC(6,2),
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    elapsed_seconds     NUMERIC(10,2),
    status              VARCHAR(50),
    error_message       TEXT,
    spark_app_id        VARCHAR(200),
    notes               TEXT
);
```

---

## 5. Core System Components & Responsibilities

| Component | Module Path | Core Responsibility |
|-----------|-------------|---------------------|
| `SparkSessionFactory` | [`src/spark_session.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/spark_session.py) | Configures SparkSession builder; supports Spark Connect container routing. |
| `ConnectionManager` | [`src/connections/connection_manager.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/connections/connection_manager.py) | Parses INI section profiles; enforces role-based access & non-fallback credentials (**Defect D-004**). |
| `MetadataReader` | [`src/metadata/metadata_reader.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/metadata/metadata_reader.py) | Queries Tier 2 metadata catalog, constructing self-contained `MetadataRow` objects ([**ADR-001**](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-001_controlled_denormalization.md)). |
| `Extractor` | [`src/extract/extractor.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/extract/extractor.py) | Reads source table via JDBC using adaptive partitioning bounds into Bronze Parquet. |
| `Transformer` | [`src/transform/transformer.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/transform/transformer.py) | Trims composite natural keys (`normalized_natural_key`), deduplicates, casts types, appends audit columns (**Defect D-001**). |
| `Loader` | [`src/load/loader.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/load/loader.py) | Executes pre-flight target table DDL schema creation; writes Gold DML (`INSERT` / `UPSERT` / `MERGE`) (**Change C.1**). |
| `Reconciler` | [`src/validation/reconciler.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/validation/reconciler.py) | Verifies row count thresholds between source and target layers ([**ADR-004**](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-004_reconciliation_overflow_guard.md)). |
| `CDCHandler` | [`src/cdc/cdc_handler.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/cdc/cdc_handler.py) | Computes watermark bounds; enforces mutation-capable CDC timestamp rules (**Defect D-002**). |
| `GovernedLogFormatter` | [`src/security/data_governance.py`](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/src/security/data_governance.py) | Wraps logger StreamHandlers to scrub PII and secrets before console/file emission (**Defect D-003**). |

---

## 6. Configuration Management & Security Controls

All connection settings live in `config/databases.ini`. Plaintext passwords are strictly forbidden. Credentials must use environment variable substitution (`${ENV_VAR}`) and enforce fast-fail initialization (**Defect D-004**):

```ini
[RepositoryDB]
role = both
db_type = PostgresSQL
server = ${META_HOST}
database = ${META_DATABASE}
schema = metadata
user = ${META_USER}
password = ${META_PASSWORD}
port = 5432
```

- **Role Enforcement:** Sections tagged `role = source` reject write calls; `role = target` reject source extraction calls.
- **PII Stream Redaction:** `GovernedLogFormatter` sanitizes all outbound log streams, redacting emails (`<EMAIL_REDACTED>`), CNICs (`<CNIC_REDACTED>`), and passwords before stdout/file emission.

---

## 7. CDC, Watermarks & Incremental Synchronization

- **Mutation-Capable CDC Strategy Guard (**Defect D-002**):** Insertion-only columns (`created_at`, `registration_date`) assign `INSERT_ONLY` strategy. Only mutation-capable columns (`updated_at`, `modified_at`) assign `TIMESTAMP` watermark strategy.
- **Watermark Persistence:** On successful ingestion, `last_success_date` updates to `MAX(watermark_column)` in `metadata.db_tables_etl`.

---

## 8. Reconciliation, Idempotency & Overflow Guards

- **Reconciliation Overflow Guard ([**ADR-004**](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-004_reconciliation_overflow_guard.md)):** When row count discrepancies exceed `reconcile_threshold`, `Reconciler` raises `ReconciliationError`, setting `rerun_flag = 'y'` without updating `last_success_date`.
- **Orchestration Retry State Guard (**Defect D-005**):** Orchestration retry queries exclude `SUCCESS`, `SKIPPED`, and `DEPENDENCY_FAILED` tables, guaranteeing exactly **1 log record per table run ID** in `metadata.batch_log`.

---

## 9. Streamlit Operations Console (GUI) Subsystem

The pipeline includes a dedicated Streamlit web console under `gui/` (`streamlit run gui/app.py`). It reuses the ETL engine's `ConnectionManager` and `metadata_service` layers without duplicating connection or SQL logic.

- **Zero DB Logic Drift:** Reads credentials dynamically from `ConnectionManager` (`src/connections/connection_manager.py`) using `.env` + `databases.ini`.
- **13 Operational Pages:** Covers KPI Dashboard (`Page 01`), Metadata Explorer (`Page 02`), Table Details (`Page 03`), Batch Run Audit Logs (`Page 04`), Analytics Summary (`Page 05`), Alert Center (`Page 06`), Discovery & Steward Promotion (`Page 07`), Connection Manager Profile Editor (`Page 08`), Pipeline Execution Trigger Console (`Page 09`), Active Job Queue Monitor (`Page 10`), CDC Maintenance & Watermark Controls (`Page 11`), Infrastructure Health Checks (`Page 12`), and Protected Reset Tools (`Page 13`).
- **Operational Manual:** Complete launch parameters and page-by-page operational instructions are documented in [**`ETL_EXECUTION_MANUAL.md`**](file:///c:/Users/Administrator/Pictures/landing_pipeline/ETL_EXECUTION_MANUAL.md#10-step-9-streamlit-operations-dashboard-gui-manual).

---

## Enterprise Connection Management Schema (`metadata.connections` & `metadata.connection_history`)

```sql
CREATE TABLE IF NOT EXISTS metadata.connections (
    tag VARCHAR(100) PRIMARY KEY,                                      -- e.g., 'msopforg_opforg', 'land_n'
    connection_type VARCHAR(50) NOT NULL DEFAULT 'DATABASE',           -- 'DATABASE', 'FILE', 'API', etc.
    supports_source BOOLEAN NOT NULL DEFAULT true,
    supports_target BOOLEAN NOT NULL DEFAULT false,
    environment VARCHAR(20) NOT NULL DEFAULT 'PROD' CHECK (environment IN ('DEV', 'TEST', 'UAT', 'PROD')),
    db_type VARCHAR(50) NOT NULL,                                      -- 'postgres', 'mysql', 'oracle', 'mssql'
    server VARCHAR(200) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(200) NOT NULL,
    schema_name VARCHAR(200),
    username VARCHAR(100) NOT NULL,
    credential_reference VARCHAR(255) NOT NULL,                        -- e.g. '${MSOPFORG_DB_PASSWORD}' or 'env://POSTGRES_PASSWORD'
    service_name VARCHAR(100),                                         -- For Oracle DSN or service string
    connection_options JSONB DEFAULT '{}'::jsonb,                      -- e.g. {"sslmode": "require", "fetchSize": 10000}
    
    -- Health & Pre-Save Test Metadata
    last_tested_at TIMESTAMP,
    last_test_status VARCHAR(20) DEFAULT 'UNTESTED',                   -- 'SUCCESS', 'FAILED', 'UNTESTED'
    last_error TEXT,
    last_latency_ms INTEGER,
    
    -- Governance & Ownership
    owner VARCHAR(100) DEFAULT 'Admin',
    team VARCHAR(100) DEFAULT 'Data Engineering',
    purpose TEXT,
    max_parallel_tables INTEGER DEFAULT 5,

    
    -- Lifecycle & Soft Deletes
    is_active CHAR(1) DEFAULT 'y' CHECK (is_active IN ('y', 'n')),
    deleted_at TIMESTAMP,
    deleted_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on active connections by role support for fast startup queries
CREATE INDEX IF NOT EXISTS idx_metadata_connections_active ON metadata.connections(supports_source, supports_target, is_active);

-- Git-Style JSON Snapshot Audit History Table

CREATE TABLE IF NOT EXISTS metadata.connection_history (
    history_id SERIAL PRIMARY KEY,
    tag VARCHAR(100) NOT NULL,
    change_type VARCHAR(20) NOT NULL,                                  -- 'CREATE', 'UPDATE', 'SOFT_DELETE'
    old_value JSONB,                                                   -- Null on CREATE
    new_value JSONB,                                                   -- Null on SOFT_DELETE
    changed_by VARCHAR(100) NOT NULL,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metadata_connection_history_tag ON metadata.connection_history(tag, changed_at DESC);
```

---

## Enterprise Notification Center Schema (`metadata.notifications`)

```sql
CREATE TABLE IF NOT EXISTS metadata.notifications (
    notification_id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Categorization & Structured Code
    notification_type VARCHAR(50) NOT NULL,      -- 'PIPELINE', 'CONNECTION', 'DISCOVERY', 'QUALITY', 'SYSTEM', 'SECURITY', 'VALIDATION'
    event_code VARCHAR(100) NOT NULL,            -- 'PIPELINE_LOAD_FAILED', 'CONNECTION_FAILED', 'QUALITY_CHECK_FAILED', 'SCHEMA_DRIFT', 'DISCOVERY_FAILED'
    created_by_component VARCHAR(100) NOT NULL,  -- 'PIPELINE', 'DISCOVERY', 'CONNECTION', 'QUALITY', 'GUI', 'SYSTEM'
    severity VARCHAR(20) NOT NULL,               -- 'WARNING', 'ERROR', 'CRITICAL' (No INFO stored)
    title VARCHAR(200) NOT NULL,
    message TEXT NOT NULL,
    
    -- Cross-Run Correlation & Deduplication
    correlation_key VARCHAR(150),                -- e.g. 'connection_failure:msopforg_opforg' or 'load_failed:msopforg_opforg.Membership'
    affected_count INTEGER DEFAULT 1,            -- Number of times or tables aggregated into this open incident
    
    -- Direct Context & Log Links
    run_id UUID,
    batch_log_id BIGINT,                         -- Direct link to metadata.batch_log.id for instant inspection
    summary_id BIGINT,                           -- Direct link to metadata.batch_summary.id
    source_tag VARCHAR(100),
    source_table VARCHAR(200),
    layer VARCHAR(50),                           -- 'Bronze', 'Silver', 'Gold', 'System'
    status VARCHAR(50) DEFAULT 'NEW',            -- 'NEW', 'ACKNOWLEDGED', 'RESOLVED'
    
    -- Incident Lifecycle Audit Trail
    acknowledged BOOLEAN DEFAULT false,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP,
    resolved BOOLEAN DEFAULT false,
    resolved_by VARCHAR(100),
    resolved_at TIMESTAMP,
    
    -- Email Tracking
    email_sent BOOLEAN DEFAULT false,
    email_sent_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_notifications_status ON metadata.notifications(status, severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_correlation ON metadata.notifications(correlation_key, status) WHERE correlation_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_notifications_type_sev ON metadata.notifications(notification_type, severity);
CREATE INDEX IF NOT EXISTS idx_notifications_run ON metadata.notifications(run_id);

COMMENT ON TABLE metadata.notifications IS 'Derived operational notification events and actionable alerts for ETL monitoring.';
COMMENT ON COLUMN metadata.notifications.correlation_key IS 'Unique incident key grouping related failures across runs until resolved.';
COMMENT ON COLUMN metadata.notifications.status IS 'Incident lifecycle status: NEW -> ACKNOWLEDGED -> RESOLVED.';
```

---

## Governance & Recommendations Engine Schema (`metadata.table_recommendations`)

```sql
CREATE TABLE IF NOT EXISTS metadata.table_recommendations (
    recommendation_id       SERIAL          PRIMARY KEY,
    table_id                INTEGER         NOT NULL REFERENCES metadata.db_tables(table_master_key) ON DELETE CASCADE,
    version_num             INTEGER         NOT NULL DEFAULT 1,
    is_active_version       BOOLEAN         NOT NULL DEFAULT TRUE,
    recommendation_type     VARCHAR(50)     NOT NULL, -- e.g. 'CDC_STRATEGY', 'PARTITIONING'
    recommended_value       VARCHAR(200)    NOT NULL, -- e.g. 'LAST_UPDATE_DATE'
    recommended_type        VARCHAR(50)     NOT NULL, -- 'TIMESTAMP', 'SEQUENCE', 'SCN', 'NONE'
    confidence_level        VARCHAR(20)     DEFAULT 'MEDIUM', -- 'HIGH', 'MEDIUM', 'LOW'
    confidence_reasons      JSONB           DEFAULT '[]',     -- ["TIMESTAMP datatype", "Indexed", "NOT NULL"]
    performance_risk        VARCHAR(20)     NOT NULL, -- 'LOW', 'MEDIUM', 'HIGH'
    correctness_risk        VARCHAR(20)     NOT NULL, -- 'LOW', 'MEDIUM', 'HIGH'
    recommendation_status   VARCHAR(30)     DEFAULT 'PENDING', -- 'PENDING', 'APPROVED', 'OVERRIDDEN', 'OBSOLETE'
    steward_user            VARCHAR(100),
    steward_notes           TEXT,
    generated_by_engine     VARCHAR(100)    DEFAULT 'MetadataDiscoveryEngine_v1.0',
    generated_at            TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at             TIMESTAMP,
    last_verified_at        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_steward_audit CHECK (
        (recommendation_status IN ('APPROVED', 'OVERRIDDEN') AND steward_user IS NOT NULL AND reviewed_at IS NOT NULL)
        OR (recommendation_status NOT IN ('APPROVED', 'OVERRIDDEN'))
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_active_rec_per_table 
    ON metadata.table_recommendations(table_id) WHERE (is_active_version = TRUE);
```
