# 📖 ETL Pipeline Execution Manual (v3.3.0)

Operational execution manual and reference guide for both **CLI commands** and the **Streamlit Operations Dashboard (GUI)**.

> [!IMPORTANT]
> **Single Unified CLI Entry Point**: All Medallion data execution (`run_landing`, `run_history`, `run_target`, `run_pipeline`), catalog discovery (`sync`), diagnostics (`healthcheck`), and emergency operations (`fix`) route through **`jobs/app_etl.py`**.
>
> **Streamlit Operations Dashboard (GUI)**: In addition to CLI tools, the pipeline includes an interactive 13-page web console (`streamlit run gui/app.py`) for live monitoring, metadata management, catalog promotion, connection testing, and job execution.
>
> **Governance Enforcement ([ADR-003](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-003_two_tier_catalog.md))**: Automated catalog discovery writes exclusively to Tier 1 (`metadata.db_tables`). The `--activate` CLI bypass flag is disabled. Promoting tables to active execution status in Tier 2 (`metadata.db_tables_etl`) requires explicit human steward promotion via `python jobs/promote_table.py` or the GUI Discovery & Promotion panel (`Page 07`).

---

## Table of Contents

1. [Environment & Credentials Configuration](#1-environment--credentials-configuration)
2. [Step 1: Pre-Flight Health Checks & Validation](#2-step-1-pre-flight-health-checks--validation)
3. [Step 2: Schema Discovery & Crawler Crawling](#3-step-2-schema-discovery--crawler-crawling)
4. [Step 3: Human Steward Promotion (`promote_table.py`)](#4-step-3-human-steward-promotion-promote_tablepy)
5. [Step 4: Target Pre-Flight DDL Provisioning (`auto_configure_target.py`)](#5-step-4-target-pre-flight-ddl-provisioning-auto_configure_targetpy)
6. [Step 5: Medallion ETL Workload Execution (`app_etl.py`)](#6-step-5-medallion-etl-workload-execution-app_etlpy)
7. [Step 6: Gold CDC Incremental Synchronization](#7-step-6-gold-cdc-incremental-synchronization)
8. [Step 7: Silver History Restatement & Backfill](#8-step-7-silver-history-restatement--backfill)
9. [Step 8: Troubleshooting, Overrides & Rerun Controls](#9-step-8-troubleshooting-overrides--rerun-controls)
10. [Step 9: Streamlit Operations Dashboard (GUI) Manual](#10-step-9-streamlit-operations-dashboard-gui-manual)
11. [CLI Parameter Reference Matrix](#11-cli-parameter-reference-matrix)

---

## 1. Environment & Credentials Configuration

All source and target database connections live in **`config/databases.ini`** (referenced by `config/config.yaml` / `config/config.local.yaml`). Password fields use environment variable substitution (`${ENV_VAR}`) and strictly enforce non-fallback policies (**Defect D-004**).

```ini
[RepositoryDB]
role = both
env = prod
db_type = PostgresSQL
server = postgres
database = hiveflow_metadata
schema = metadata
user = hiveflow_admin
password = ${META_PASSWORD}
port = 5432

[land_n]
role = target
env = prod
db_type = PostgresSQL
server = postgres-dw
database = hiveflow_metadata
schema = analytics_dw
user = hiveflow_admin
password = ${LAND_N_PASSWORD}
port = 5432
```

Export required credential variables before running any workloads:
```bash
export META_PASSWORD="your_metadata_password"
export LAND_N_PASSWORD="your_target_password"
export MSOPFORG_PASSWORD="your_source_password"
```

---

## 2. Step 1: Pre-Flight Health Checks & Validation

Provides instant fail-fast verification of metadata catalogs, connection configs, and cluster reachability without initiating Spark jobs.

### Commands

**Fast Metadata Catalog Validation (`--validate-only`)**
```bash
python jobs/app_etl.py --validate-only --source_tag msopforg_opforg
```
- **Action:** Runs `MetadataValidator` SQL checks against active metadata rows (`metadata.db_tables_etl`, `metadata.db_columns_etl`).
- **Success Output:** `Status: [ OK ] VALID` report confirming zero missing columns or unmapped data types.

**End-to-End Infrastructure Healthcheck (`--action healthcheck`)**
```bash
python jobs/app_etl.py --action healthcheck
```
- **Action:** Executes `HealthChecker` to test repository database connectivity, schema version, source/target database profiles, and Spark Connect reachability.
- **Success Output:** Formatted ASCII dashboard showing `[ OK ]` status for each component.

---

## 3. Step 2: Schema Discovery & Crawler Crawling

Inspects raw source database catalogs via JDBC and registers discovered structures into Tier 1 Master Catalog (`metadata.db_tables`, `metadata.db_columns`).

### Commands

**Dry-Run Schema Discovery**
```bash
python jobs/discover_and_promote.py --source_tag msopforg_opforg --target_tag land_n --dry-run
```
- **Action:** Scans source schema and previews catalog changes without editing database records.

**Execute Master Catalog Discovery**
```bash
python jobs/discover_and_promote.py --source_tag msopforg_opforg --target_tag land_n
```
- **Action:** Crawls source database, inserting new objects into Tier 1 Master Catalog (`metadata.db_tables`) with `is_active='n'`.

**Review Recently Discovered Objects**
```bash
python jobs/review_discovered.py --source_tag msopforg_opforg --days 7
```
- **Action:** Displays newly cataloged tables and columns pending human steward review.

---

## 4. Step 3: Human Steward Promotion (`promote_table.py`)

Enforces [**ADR-003**](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-003_two_tier_catalog.md). Promotes cataloged tables from Tier 1 (`metadata.db_tables`) to active Tier 2 Execution Control (`metadata.db_tables_etl`).

### Commands

**Promote All Cataloged Tables for a Source**
```bash
python jobs/promote_table.py --source_tag msopforg_opforg --target_tag land_n --all --priority 2
```
- **Action:** Promotes all discovered tables to Tier 2 with Priority 2 (`is_active='y'`).

**Promote Single Table**
```bash
python jobs/promote_table.py --source_tag msopforg_opforg --target_tag land_n --table memberships --priority 1
```
- **Action:** Promotes `memberships` table to Tier 2 with Priority 1.

**Force Refresh Promoted Columns**
```bash
python jobs/promote_table.py --source_tag msopforg_opforg --target_tag land_n --table memberships --force
```
- **Action:** Overwrites active column mapping definitions in `db_columns_etl` with latest source schema facts.

**Audit Schema Drift**
```bash
python jobs/promote_table.py --source_tag msopforg_opforg --target_tag land_n --drift-check
```
- **Action:** Reports structural column additions, deletions, or data type changes between source database and active metadata.

---

## 5. Step 4: Target Pre-Flight DDL Provisioning (`auto_configure_target.py`)

Provisions target PostgreSQL database table DDL syntax (`CREATE TABLE IF NOT EXISTS`, primary keys, `row_hash` columns) during pre-flight, decoupling DDL schema creation from runtime DML write streams (**Change C.1**).

### Commands

**Dry-Run Target DDL Generation**
```bash
python jobs/auto_configure_target.py --source_tag msopforg_opforg --target_schema land_n --tables memberships --dry-run
```
- **Action:** Previews generated target PostgreSQL DDL SQL syntax.

**Execute Target DDL Schema Provisioning**
```bash
python jobs/auto_configure_target.py --source_tag msopforg_opforg --target_schema land_n --tables memberships
```
- **Action:** Creates target table schema in target database and registers active column maps.

---

## 6. Step 5: Medallion ETL Workload Execution (`app_etl.py`)

Unified CLI runner for executing production data pipelines across Bronze (Landing), Silver (History), and Gold (Target) layers.

### Commands

**Full Pipeline Execution (Bronze → Silver → Gold)**
```bash
python jobs/app_etl.py --action run_pipeline --source_tag msopforg_opforg --priority 1
```
- **Action:** Processes active Priority 1 tables through Bronze Parquet landing, Silver history deduplication, and Gold CDC UPSERT. Downstream layers respect exact table dependencies (`eligible_tables`).

**Process Single Table**
```bash
python jobs/app_etl.py --action run_pipeline --source_tag msopforg_opforg --priority 1 --table memberships
```
- **Action:** Executes full Medallion pipeline for `memberships` table only.

**Bronze Landing Only (Skip Silver/Gold)**
```bash
python jobs/app_etl.py --action run_landing --source_tag msopforg_opforg --priority 1
```
- **Action:** Extracts JDBC data into raw Snappy-compressed Parquet files under `/data/landing/` without running Silver/Gold.

---

## 7. Step 6: Gold CDC Incremental Synchronization

Applies incremental updates from Silver history to Gold target tables using watermark-based CDC algorithms and Delta MERGE / PostgreSQL UPSERT logic.

### Commands

**Execute Gold CDC Ingestion**
```bash
python jobs/app_etl.py --action run_target --source_tag msopforg_opforg
```
- **Action:** Queries watermarks, extracts mutated rows, and executes incremental UPSERT/MERGE operations.

---

## 8. Step 7: Silver History Restatement & Backfill

Re-cleanses and restates Silver history tables directly from existing Bronze Parquet files without re-querying source databases.

### Commands

**Backfill Silver Priority Tier**
```bash
python jobs/app_etl.py --action run_history --source_tag msopforg_opforg --priority 1
```
- **Action:** Reads landed Bronze Parquet data, applies column type casting and business key deduplication, and rewrites clean Silver dataset.

---

## 9. Step 8: Troubleshooting, Overrides & Rerun Controls

Emergency operational controls for inspecting run logs, clearing watermarks, and forcing reruns.

### Commands

**Inspect Current Table Watermark & Status**
```bash
python jobs/app_etl.py --action fix --operation check_watermark --source_tag msopforg_opforg --table memberships
```
- **Action:** Prints `last_success_date`, `rerun_flag`, and `last_success_timestamp`.

**Queue Table for Immediate Rerun**
```bash
python jobs/app_etl.py --action fix --operation rerun --source_tag msopforg_opforg --table memberships
```
- **Action:** Flips `rerun_flag = 'y'`, queuing table for re-processing during the next pipeline execution.

**Reset CDC Watermark**
```bash
python jobs/app_etl.py --action fix --operation reset_watermark --source_tag msopforg_opforg --table memberships
```
- **Action:** Clears watermark timestamps, forcing the next Gold run to execute a complete reload.

**View Detailed Execution Audit Log**
```bash
python jobs/app_etl.py --action fix --operation status --run_id 58938285-5343-46ca-a098-af10ffd24a1e
```
- **Action:** Retrieves logged batch run metrics (`source_count`, `target_count`, `gap`, `elapsed_seconds`, `status`) from `metadata.batch_log`.

---

## 10. Step 9: Streamlit Operations Dashboard (GUI) Manual

The ETL Operations Dashboard is a multi-page web application providing visual operations, real-time batch logging, metadata governance, and interactive job triggers.

### 10.1 Quick Start Launch Commands

```bash
# 1. Install GUI dependencies
pip install -r gui/requirements.txt

# 2. Launch Streamlit Operations Console
streamlit run gui/app.py
```
Default URL: **`http://localhost:8501`**

To specify a custom configuration file:
```bash
# Environment Variable override
export ETL_CONFIG_PATH=config/config.local.yaml
streamlit run gui/app.py
```

### 10.2 Module & Page Directory

| # | Page Module | Focus & Features |
|---|---|---|
| **01** | **`01_Dashboard.py`** | **KPI Dashboard:** Real-time throughput cards, active runs count, batch failure rate, and auto-refreshing recent `batch_log` table. |
| **02** | **`02_Pipeline_Explorer.py`** | **Pipeline Explorer:** Filterable metadata grid of active tables (`db_tables_etl`), priority levels, landing modes, and CDC configuration. |
| **03** | **`03_Table_Details.py`** | **Table Details & Schema Inspector:** In-depth view of single table metadata, column contract list (`db_columns_etl`), PII masking levels, and table-level run history. Includes **Flag for Rerun** button. |
| **04** | **`04_Run_History.py`** | **Batch Log Audit Trail:** Searchable, paginated audit trail of all `metadata.batch_log` executions with source/target row counts, processing time, status filters, and CSV export. |
| **05** | **`05_Pipeline_Summary.py`** | **Pipeline Aggregated Analytics:** Plotly performance trend charts, duration percentiles, layer load distributions, and batch summary metrics. |
| **06** | **`06_Notification_Center.py`** | **Alerts & Telemetry:** Configurable Slack/Email webhook notifications, log event stream inspector, and alert history. |
| **07** | **`07_Discovery_Promotion.py`** | **Discovery Crawler & Promotion Panel ([ADR-003](file:///c:/Users/Administrator/Pictures/landing_pipeline/data_pipeline/spark/jobs/pyspark_pipeline/.brain/adr/ADR-003_two_tier_catalog.md)):** UI for running schema crawlers on source databases, viewing Tier 1 cataloged tables (`db_tables`), and performing human steward promotion to Tier 2 (`db_tables_etl`). |
| **08** | **`08_Connection_Manager.py`** | **Database Profile Manager:** Visual editor for connection sections in `databases.ini`, role assignment (`source`/`target`), and live JDBC ping test functionality. |
| **09** | **`09_Execute_Pipeline.py`** | **Interactive Job Launcher:** Visual trigger interface for initiating `run_pipeline`, `run_landing`, `run_history`, or `run_target` workloads with parameter sliders and single-table target selectors. |
| **10** | **`10_Job_Queue.py`** | **Active Task Monitor:** Monitor running and queued PySpark sub-processes, thread concurrency limits, and cancel hung tasks. |
| **11** | **`11_Maintenance.py`** | **Watermark & State Maintenance:** Reset CDC watermarks, clear error flags (`rerun_flag`), compact landing metadata, and purge stale audit logs. |
| **12** | **`12_Health_Check.py`** | **Infrastructure Health Dashboard:** Visual output of `HealthChecker` diagnostic tests (Repository DB, source connections, metadata version, Spark Connect reachability). |
| **13** | **`13_Danger_Zone.py`** | **Emergency & Reset Controls:** Protected operational tools for purging cache files, truncating landing schemas, or re-initializing metadata control tables (requires confirmation phrase). |

---

## 11. CLI Parameter Reference Matrix

<!-- BEGIN_PARAMETER_DESCRIPTIONS -->
| Parameter | Data Type | Choices / Default | Description | Required? |
|---|---|---|---|---|
| `--source_tag` | `String` | - | Source system tag — must match a section name in databases.ini (e.g., source_erp_db). Required for all actions except 'fix --operation status'. | ❌ No |
| `--target_tag` | `String` | - | Optional target tag override (e.g., land_test for DEV, land_n for PROD). Routes target writes to this database connection. | ❌ No |
| `--action` | `String` | Choices: `['run_landing', 'run_history', 'run_target', 'run_pipeline', 'run_governed', 'sync', 'fix', 'healthcheck']` | Pipeline action to run:   run_landing             — Extracts raw data (Bronze)   run_history             — Deduplicates/Appends to history (Silver)   run_target              — Performs UPSERT/MERGE to PostgreSQL (Gold)   run_pipeline            — End-to-end (Bronze -> Silver -> Gold)   sync                    — Discover and auto-promote metadata   fix                     — Emergency operations   healthcheck             — Run diagnostic checks on DBs, schema & metadata | ❌ No |
| `--validate-only` | `Boolean` | - | Run pre-flight metadata validation and exit immediately (overrides --action). If --source_tag is omitted, validates all active tables across all tags. | ❌ No |
| `--config` | `String` | Default: `config/config.yaml` | Path to YAML config file (default: config/config.yaml) | ❌ No |
| `--run_history`, `--run_silver` | `String` | Choices: `['true', 'false']`<br>Default: `true` | Whether to run History/Target layers (default true). Use 'false' to skip them (Landing-only mode). '--run_silver' is accepted as a backward-compatible alias. | ❌ No |
| `--priority` | `Integer` | Choices: `[1, 2, 3]` | Priority tier: 1=High, 2=Medium, 3=Low. Required for run_history; optional for run_pipeline, run_landing, and run_target (defaults to all priorities if omitted). | ❌ No |
| `--table` | `String` | - | Target a single table (bypasses daily skip checks). Optional for load actions; required for fix rerun/reset/check. | ❌ No |
| `--dry-run` | `Boolean` | - | (sync) Preview changes without writing to the database. | ❌ No |
| `--activate` | `Boolean` | - | (sync) After crawl, flip newly discovered tables to is_active='y'. | ❌ No |
| `--deactivate-missing` | `Boolean` | - | (sync) Deactivate tables/columns that no longer exist in source. | ❌ No |
| `--operation` | `String` | Choices: `['rerun', 'reset_watermark', 'check_watermark', 'status']` | (fix) Operation to execute:   rerun           — set rerun_flag='y' so table runs at next execution   reset_watermark — clear CDC watermark, forcing a full re-extract   check_watermark — print current watermark and run-control state   status          — print batch_log rows for the given --run_id | ❌ No |
| `--run_id` | `String` | - | (fix --operation status) UUID of the pipeline run to inspect. | ❌ No |
<!-- END_PARAMETER_DESCRIPTIONS -->

> ¹ Not required for `--action healthcheck` or `fix --operation status`. ² Required for `run_history`; optional for `run_pipeline`, `run_landing`, `run_target`. ³ Required for fix operations `rerun`/`reset_watermark`/`check_watermark`. ⁴ Required when `--action fix`. ⁵ Required when `--operation status`.
