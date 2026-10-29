# HiveFlow — Governed Lakehouse Platform

<div align="center">

![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11-brightgreen.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688.svg)
![React](https://img.shields.io/badge/React-19-61DAFB.svg)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-purple.svg)

**A high-throughput, enterprise-grade Governed Lakehouse Engine & Operational Control Plane built on Apache Spark, FastAPI, and React.**

[Quickstart](#-quickstart-in-3-minutes) • [Architecture](#-architecture-overview) • [Key Features](#-core-capabilities) • [API Reference](#-api-endpoints) • [License](#-license)

</div>

---

## 🌟 Overview

**HiveFlow** is a modern, metadata-driven Lakehouse platform engineered to bridge the gap between distributed Spark batch processing and strict transactional governance. It replaces ad-hoc ETL scripts with a standardized **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**, enforced by an immutable PostgreSQL metadata operating system with 46 automated migrations.

HiveFlow provides:
- **Zero-Code Source Ingestion:** Automated metadata crawlers discover source tables and schemas across PostgreSQL, MySQL, and Oracle.
- **Transactional Medallion Processing:**
  - **Bronze (Landing):** Partitioned immutable Parquet snapshots with zero source locking.
  - **Silver (History):** Surrogate key resolution, column type casting, deduplication, and entity-scoped synthetic keys.
  - **Gold (Analytics):** Atomic staging swap, bounded target locking, and strategy-aware idempotent UPSERT/MERGE convergence.
- **Enterprise RBAC & Forensic Audit:** Granular manual role permissions, secure bcrypt credential hashing, and an immutable operator activity observer.
- **Modern Operational Console:** Sleek dark-mode React UI for real-time pipeline monitoring, discovery promotion, and diagnostic diff inspection.

---

## 🏛 Architecture Overview

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA SYSTEMS                             │
│     PostgreSQL              MySQL                  Oracle              │
└──────────┬────────────────────┬───────────────────────┬────────────────┘
           │                    │                       │
           ▼                    ▼                       ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    MEDALLION PIPELINE (APACHE SPARK)                   │
│                                                                        │
│  ┌───────────────────┐    ┌───────────────────┐    ┌─────────────────┐ │
│  │   BRONZE LAYER    │ ──►│   SILVER LAYER    │ ──►│   GOLD LAYER    │ │
│  │ (Raw Parquet Data)│    │  (Deduplicated &  │    │  (Atomic Swap   │ │
│  │  Streaming SSCursor│   │   Surrogate Keys) │    │  Target UPSERT) │ │
│  └───────────────────┘    └───────────────────┘    └─────────────────┘ │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                  HIVEFLOW METADATA OPERATING SYSTEM                    │
│                                                                        │
│   • Discovery Catalog (Tier-1) ──► Production Policy Catalog (Tier-2)  │
│   • Bounded CDC Attempt Windows (L, H] with CAS Watermark Promotion    │
│   • Strategy-Aware Physical Reconciliation & Fingerprint Ledgers       │
│   • Forensic Operator Activity Log & Before/After State Diffs          │
│   • Enterprise RBAC (Admin, Viewer & Custom Granular Roles)            │
└────────────────────────────────────────────────────────────────────────┘
```

---

## ⚡ Core Capabilities

### 1. Robust Medallion Pipeline Engine
- **Consistent Snapshot Ingestion:** Dedicated transaction isolation (`REPEATABLE READ + START TRANSACTION WITH CONSISTENT SNAPSHOT`) prevents phantom reads and locking stalls.
- **Entity-Scoped Identity & Synthetic Keys:** Automatically generates collision-free `{source_tag}_{source_table}_{keys}` identities, eliminating keyless table blindspots.
- **Atomic Gold Publication:** Full snapshot writes persist into isolated staging before an atomic target table swap protected by configurable lock timeouts.

### 2. Operational Control Plane & REST API
- Built with **FastAPI** using standardized response envelopes, strict request validation, CSRF cookie authentication, and rate limiting.
- Provides 30+ endpoints covering pipeline dispatch, live run telemetry, catalog crawler, system health diagnostics, and user governance.

### 3. Role-Based Access Control & Forensic Observer
- **Custom Granular Roles:** Create roles with manual access checkboxes across Observability, Execution, Governance, Integrations, and Administration.
- **Forensic Activity Observer:** Monitor every operation performed by each user. Includes a side-by-side **JSON Diff Inspector** to examine exact pre- and post-operation states.
- **Safety Invariants:** Enforces zero self-deletion for logged-in operators and prevents dropping the last active administrator.

---

## 🚀 Quickstart in 3 Minutes

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/)
- Git

### 1. Clone & Setup Environment
```bash
git clone https://github.com/your-username/hiveflow-platform.git
cd hiveflow-platform

# Copy example environment configuration
cp .env.example .env
```

### 2. Launch Platform via Docker Compose
```bash
docker compose up -d
```

This starts:
| Container | Service | Port |
| :--- | :--- | :--- |
| `hiveflow-metadata-db` | PostgreSQL 15 Metadata Catalog | `5432` |
| `hiveflow-spark-master` | Apache Spark 3.5 & Spark Connect | `15002` (UI: `8080`) |
| `hiveflow-api` | FastAPI REST Control Plane | `8000` |
| `hiveflow-console` | React Operational GUI Console | `3000` |

### 3. Open the Operational Console
Navigate to [**http://localhost:3000**](http://localhost:3000) in your browser:
- **Default Admin Username:** `admin`
- **Default Admin Password:** `admin123`

---

## 🛠 Local Development Setup (Without Docker)

### 1. Backend & PySpark
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Or on Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start FastAPI control plane
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### 2. Frontend React Console
```bash
cd frontend
npm install
npm run dev
```

### 3. Run Automated Regression Tests
```bash
pytest tests/ -v
```

---

## 📋 API Endpoints Summary

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/v1/auth/me` | Fetch active session & permissions |
| `POST` | `/api/v1/auth/login` | Authenticate with bcrypt & issue HttpOnly JWT |
| `GET` | `/api/v1/dashboard/kpis` | Real-time throughput, status & active runs |
| `POST` | `/api/v1/runs/trigger` | Dispatch pipeline run across Bronze, Silver, Gold |
| `GET` | `/api/v1/discovery/tables` | Crawl & inspect source catalog schemas |
| `POST` | `/api/v1/discovery/promote` | Promote Tier-1 tables to Tier-2 active pipeline |
| `GET` | `/api/v1/users` | List platform users & roles |
| `POST` | `/api/v1/users` | Provision new user with role & project scopes |
| `GET` | `/api/v1/roles/permissions` | List available granular permissions |
| `GET` | `/api/v1/audit/logs` | Query forensic audit ledger by operator & action |

---

## 📚 Documentation & Technical Manuals

Comprehensive guides and architectural references are available in the [`docs/`](docs/) directory:

| Document | Description |
| :--- | :--- |
| [**Data Engineering Concepts Implemented**](docs/CONCEPTS_IMPLEMENTED.md) | In-depth breakdown of 30+ enterprise data engineering concepts implemented across the pipeline (Medallion, CDC, Dynamic Schema, Adaptive Partitioning). |
| [**ETL Execution Manual**](docs/ETL_EXECUTION_MANUAL.md) | Operational execution guide for CLI execution (`app_etl.py`), crawler syncing, table promotion, and restatement. |
| [**ETL Pipeline Reference**](docs/ETL_PIPELINE_REFERENCE.md) | Detailed contract and parameter reference across Bronze, Silver, and Gold execution stages. |
| [**Medallion Architecture Blueprint**](docs/MEDALLION_ARCHITECTURE.md) | Deep-dive explainer on the raw landing, historical deduplication, and atomic Gold persistence tiers. |
| [**User & Operator Operations Manual**](docs/USER_OPERATIONS_MANUAL.md) | Comprehensive walkthrough of the control plane, user credentials, RBAC management, and forensic activity observer. |
| [**Comprehensive Data Engineering Guide**](docs/DATA_ENGINEERING_CONCEPTS.md) | 160KB+ reference manual of data architecture, distributed compute, and storage patterns. |
| [**Server & Deployment Operations Manual**](docs/SERVER_OPERATIONS_MANUAL.md) | Infrastructure provisioning, Docker, Spark Connect setup, and environment hardening. |

---

## 🛡️ Security & Zero-Hardcoding Guarantee

HiveFlow strictly conforms to zero-hardcoding architectural rules:
- **Single Configuration Authority:** All runtime settings and database connections flow through `ConnectionManager`.
- **Zero Fallback Credentials:** All database passwords must be supplied via environment variables (`${ENV_VAR}`).
- **Secret Hygiene:** Passwords and keys are never committed, logged, or exposed in API diagnostics.

---

## 📄 License

HiveFlow is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for complete details.
