# src/constants.py
"""
Central constants file. No magic strings anywhere else in the codebase.
All values that appear more than once live here.
"""

import hashlib
import json
from typing import Optional


class MetadataConstants:
    SCHEMA = "metadata"

    # ── Subscriber Control (ETL Engine) ───────────────────────────────────────
    TABLES_ETL_TABLE  = "db_tables_etl"
    COLUMNS_ETL_TABLE = "db_columns_etl"

    # ── Master Catalog (Source Reality) ───────────────────────────────────────
    TABLES_CATALOG    = "db_tables"
    COLUMNS_CATALOG   = "db_columns"

    # ── Audit ─────────────────────────────────────────────────────────────────
    BATCH_LOG_TABLE   = "batch_log"

    IS_ACTIVE   = "y"
    IS_INACTIVE = "n"


class LoadType:
    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"


class LayerName:
    BRONZE = "bronze"      # raw landing
    SILVER = "silver"      # history
    GOLD = "gold"          # target / business layer


import enum

class PipelinePhase(enum.Enum):
    # Source Resolution & Extraction
    RESOLVING_SOURCE = "RESOLVING_SOURCE"
    CONNECTING_SOURCE = "CONNECTING_SOURCE"
    EXTRACTING = "EXTRACTING"
    BRONZE_WRITE = "BRONZE_WRITE"
    
    # Transformation & Silver
    BRONZE_READ = "BRONZE_READ"
    TRANSFORM = "TRANSFORM"
    DEDUP = "DEDUP"
    SILVER_WRITE = "SILVER_WRITE"
    
    # Target Resolution & Gold
    RESOLVING_TARGET = "RESOLVING_TARGET"
    CONNECTING_TARGET = "CONNECTING_TARGET"
    TARGET_PREFLIGHT = "TARGET_PREFLIGHT"
    GOLD_TRANSFORM = "GOLD_TRANSFORM"
    GOLD_WRITE = "GOLD_WRITE"
    
    # Terminal states
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    ABANDONED = "ABANDONED"
    SKIPPED = "SKIPPED"
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"

    @classmethod
    def valid_transitions(cls):
        return {
            None: {cls.RESOLVING_SOURCE, cls.EXTRACTING, cls.BRONZE_READ, cls.RESOLVING_TARGET, cls.ABANDONED, cls.CANCELLED, cls.SKIPPED, cls.DEPENDENCY_FAILED},
            cls.RESOLVING_SOURCE: {cls.CONNECTING_SOURCE, cls.FAILED, cls.CANCELLED},
            cls.CONNECTING_SOURCE: {cls.EXTRACTING, cls.FAILED, cls.CANCELLED},
            cls.EXTRACTING: {cls.BRONZE_WRITE, cls.TRANSFORM, cls.FAILED, cls.CANCELLED},
            cls.BRONZE_WRITE: {cls.BRONZE_READ, cls.COMPLETED, cls.FAILED, cls.CANCELLED},
            cls.BRONZE_READ: {cls.TRANSFORM, cls.FAILED, cls.CANCELLED},
            cls.TRANSFORM: {cls.DEDUP, cls.SILVER_WRITE, cls.RESOLVING_TARGET, cls.GOLD_TRANSFORM, cls.FAILED, cls.CANCELLED},
            cls.DEDUP: {cls.SILVER_WRITE, cls.RESOLVING_TARGET, cls.FAILED, cls.CANCELLED},
            cls.SILVER_WRITE: {cls.RESOLVING_TARGET, cls.FAILED, cls.CANCELLED},
            cls.RESOLVING_TARGET: {cls.CONNECTING_TARGET, cls.FAILED, cls.CANCELLED},
            cls.CONNECTING_TARGET: {cls.TARGET_PREFLIGHT, cls.FAILED, cls.CANCELLED},
            cls.TARGET_PREFLIGHT: {cls.GOLD_TRANSFORM, cls.GOLD_WRITE, cls.FAILED, cls.CANCELLED},
            cls.GOLD_TRANSFORM: {cls.GOLD_WRITE, cls.FAILED, cls.CANCELLED},
            cls.GOLD_WRITE: {cls.COMPLETED, cls.FAILED, cls.CANCELLED},
        }

    def can_transition_to(self, new_phase: 'PipelinePhase') -> bool:
        # Terminal states can never transition out
        if self in {PipelinePhase.COMPLETED, PipelinePhase.FAILED, PipelinePhase.CANCELLED, PipelinePhase.ABANDONED, PipelinePhase.SKIPPED, PipelinePhase.DEPENDENCY_FAILED}:
            return False
            
        allowed = self.valid_transitions().get(self, set())
        # Any phase can transition to terminal phases (except if it's already terminal, checked above)
        terminal = {PipelinePhase.COMPLETED, PipelinePhase.FAILED, PipelinePhase.CANCELLED, PipelinePhase.ABANDONED, PipelinePhase.SKIPPED, PipelinePhase.DEPENDENCY_FAILED}
        return new_phase in allowed or new_phase in terminal


class DmlStatus:
    ACTIVE = 1
    SOFT_DELETED = 2
    INACTIVE = 0


class RunStatus:
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    PARTIAL = "PARTIAL"
    RECONCILIATION_FAILED = "RECONCILIATION_FAILED"   # New
    RECONCILIATION_WARNING = "RECONCILIATION_WARNING" # New
    NOT_REQUESTED = "NOT_REQUESTED"  # Layer was not part of this execution_scope


class ExecutionScope:
    """Allowed values for FrozenJobExecution.execution_scope."""
    FULL   = "FULL"    # Source → Bronze → Silver → Gold (default)
    BRONZE = "BRONZE"  # Source → Bronze only
    SILVER = "SILVER"  # Verified Bronze artifact → Silver only
    GOLD   = "GOLD"    # Verified Silver artifact → Gold only

    ALL_VALUES = frozenset({"FULL", "BRONZE", "SILVER", "GOLD"})


def normalize_execution_scope(scope: Optional[str]) -> str:
    """Return the one canonical execution scope used by contracts and workers.

    ``None`` means the historical/default FULL execution.  An explicitly
    empty value is rejected so a malformed UI or CLI request cannot silently
    become a full production run.
    """
    if scope is None:
        return ExecutionScope.FULL
    value = str(scope).strip()
    if not value:
        raise ValueError("execution_scope must not be empty or whitespace")
    canonical = value.upper()
    if canonical not in ExecutionScope.ALL_VALUES:
        raise ValueError(
            f"invalid execution_scope {scope!r}; allowed values: "
            f"{sorted(ExecutionScope.ALL_VALUES)}"
        )
    return canonical


def compute_artifact_fingerprint(
    *,
    storage_layer: str,
    storage_uri: str,
    storage_format: str,
    row_count: int,
    schema_fingerprint: str,
    policy_fingerprint: str,
    mapping_fingerprint: str,
    source_key_fingerprint: str,
    cdc_window_id: Optional[str] = None,
) -> str:
    """Create a stable identity for a persisted layer artifact.

    This is an identity/freshness fingerprint, not a claim that every byte of
    a remote object was hashed on the driver.  The producer verifies the
    object and row count before publishing this identity.
    """
    payload = {
        "storage_layer": str(storage_layer).upper(),
        "storage_uri": str(storage_uri),
        "storage_format": str(storage_format).lower(),
        "row_count": int(row_count),
        "schema_fingerprint": str(schema_fingerprint),
        "policy_fingerprint": str(policy_fingerprint),
        "mapping_fingerprint": str(mapping_fingerprint),
        "source_key_fingerprint": str(source_key_fingerprint),
        "cdc_window_id": str(cdc_window_id) if cdc_window_id else None,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class Priority:
    HIGH = 1       # critical tables, processed first
    MEDIUM = 2     # dependent tables
    LOW = 3        # large or low-urgency tables


class FileFormat:
    PARQUET = "parquet"
    DELTA = "delta"
    ORC = "orc"
    AVRO = "avro"


class WriteMode:
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"          # Delta Lake only


class AuditColumns:
    DML_TIMESTAMP = "dml_timestamp"
    DML_STATUS = "dml_status"
    SOURCE_TAG = "source_tag"
    SOURCE_IDENTIFICATION_KEY = "source_identification_key"
    SOURCE_IDENTIFICATION_TABLE = "source_identification_table_name"
    PIPELINE_RUN_ID = "pipeline_run_id"
    LANDING_INGESTION_TIMESTAMP = "landing_ingestion_timestamp"



class DefaultValues:
    CHUNK_SIZE = 100_000
    NUM_PARTITIONS = 10
    CDC_LOOKBACK_DAYS = 7
    DELTA_DAYS = 1
    DML_STATUS = DmlStatus.ACTIVE


class DBType:
    ORACLE = "oracle"
    MSSQL = "mssql"
    POSTGRES = "postgres"
    MYSQL = "mysql"

    JDBC_DRIVERS = {
        "oracle": "oracle.jdbc.OracleDriver",
        "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "postgres": "org.postgresql.Driver",
        "mysql": "com.mysql.cj.jdbc.Driver",
    }
