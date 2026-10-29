# src/load/loader.py
# -----------------------------------------------------------------------------
# STEP 3 IN OUR DATA PIPELINE: THE LOADER (BRONZE, SILVER, AND GOLD DELIVERY)
# -----------------------------------------------------------------------------
# Think of this file like our Master Delivery Driver and Safe Warehouse Stacker!
# After toys and books are extracted from the warehouse (Extractor) and cleaned
# and polished (Transformer), the Loader safely delivers them to their storage rooms:
#   1. Bronze Room (Landing): The raw intake dock (fast, snappy Parquet files).
#   2. Silver Room (History): The clean archive room where historical records live.
#   3. Gold Room (Target Store): The sparkling final storefront where business apps
#      and analysts read the data (via safe database UPSERT or clean full snapshots)!
# -----------------------------------------------------------------------------

# hashlib lets us create deterministic digital barcodes/fingerprints (SHA-256)
import hashlib
# os lets us talk to the computer's operating system (like checking if Windows or Linux)
import os
from urllib.parse import urlsplit
# Callable and Optional are type hints: Callable means a function, Optional means can be None
from typing import Any, Callable, Dict, Optional, Tuple

# SparkSession is our main Spark engine, DataFrame is the distributed table in memory
from pyspark.sql import SparkSession, DataFrame, functions as F
# BooleanType represents True/False data types in Spark
from pyspark.sql.types import BooleanType
# MetadataRow holds the configuration instruction card for the current table
from src.metadata.metadata_models import MetadataRow
# FileFormat ('parquet', 'delta') and WriteMode ('append', 'overwrite') are our standard file constants
from src.constants import FileFormat, WriteMode
# ConnectionManager is our single source of truth for connecting to databases
from src.connections.connection_manager import ConnectionManager
# Helpful database tools: executing JDBC queries, upserting, quoting table/column names, setting JDBC options
from src.utils.db_utils import (
    safe_execute_jdbc,
    execute_upsert,
    quote_column_name,
    quote_table_name,
    apply_jdbc_options,
    log_jdbc_connection_diagnostic,
)
from src.utils import db_utils as _db_utils
# SchemaValidator checks that columns match the destination room before writing
from src.load.schema_validator import SchemaValidator, LoaderValidationError
# TargetSchemaInspector inspects target database constraints (like primary keys and unique indexes)
from src.load.target_schema_inspector import get_schema_inspector
# Stage0 policy checks that we have trusted business keys and valid safety freeze configurations
from src.validation.stage0_policy import has_stage0_trusted_business_key, validate_stage0_execution_policy

# logging lets us write operational diary entries to the terminal and log files
import logging


# -----------------------------------------------------------------------------
# Error Classes: Clear, Machine-Classified Labels for What Went Wrong
# -----------------------------------------------------------------------------
# If anything fails during delivery, these custom errors tell the operator
# EXACTLY what happened instead of giving a mystery error!

class LoaderError(Exception):
    """Base error for all loading and delivery failures."""
    error_code = "LOADER_ERROR"
    error_category = "LOADER"
    retryable = False

class TargetResolutionError(LoaderError):
    """Raised when the target database tag or name cannot be found in configuration."""
    error_code = "TARGET_RESOLUTION_FAILED"
    error_category = "RESOLUTION"
    retryable = False

class TargetConnectionError(LoaderError):
    """Raised when our delivery truck cannot reach or connect to the target database."""
    error_code = "TARGET_CONNECTION_FAILED"
    error_category = "CONNECTIVITY"
    retryable = True

class TargetTimeoutError(TargetConnectionError):
    """Raised when the target database took too long to answer our knock on the door."""
    error_code = "TARGET_CONNECTION_TIMEOUT"
    error_category = "CONNECTIVITY"
    retryable = True

class TargetRefusedError(TargetConnectionError):
    """Raised when the target database actively refused our connection (door slammed shut)."""
    error_code = "TARGET_CONNECTION_REFUSED"
    error_category = "CONNECTIVITY"
    retryable = True

class TargetUnreachableError(TargetConnectionError):
    """Raised when the network path or server address could not be reached at all."""
    error_code = "TARGET_UNREACHABLE"
    error_category = "CONNECTIVITY"
    retryable = True

class TargetAuthenticationError(TargetConnectionError):
    """Raised when username or password was rejected by the target database."""
    error_code = "TARGET_AUTHENTICATION_FAILED"
    error_category = "AUTHENTICATION"
    retryable = False

class TargetPreflightError(LoaderError):
    """Raised when safety checks failed BEFORE we even started writing (pre-flight check)."""
    error_code = "TARGET_PREFLIGHT_FAILED"
    error_category = "PREFLIGHT"
    retryable = False

class TargetTableNotFoundError(LoaderError):
    """Raised when the target table does not exist and cannot be provisioned."""
    error_code = "TARGET_TABLE_NOT_FOUND"
    error_category = "SCHEMA"
    retryable = False

class GoldWriteError(LoaderError):
    """Raised when writing the final Gold data into the database fails."""
    error_code = "GOLD_WRITE_FAILED"
    error_category = "PERSISTENCE"
    retryable = True


# Initialize logger for this module
log = logging.getLogger("loader")

# Keep both patch points compatible while callers migrate to the module-level import.
# Older tests and integrations patch ``src.utils.db_utils`` directly;
# newer target-provisioning tests patch ``src.load.loader``.
_ORIGINAL_APPLY_JDBC_OPTIONS = apply_jdbc_options


def _apply_jdbc_options(builder, target_cfg):
    """Apply JDBC connection options (like timeouts and fetch sizes) to the Spark DataFrameReader/Writer."""
    fn = apply_jdbc_options
    if fn is _ORIGINAL_APPLY_JDBC_OPTIONS:
        module_fn = _db_utils.apply_jdbc_options
        if module_fn is not _ORIGINAL_APPLY_JDBC_OPTIONS:
            fn = module_fn
    return fn(builder, target_cfg)


# -----------------------------------------------------------------------------
# The Loader Class: Our Master Delivery Driver
# -----------------------------------------------------------------------------
class Loader:

    def __init__(self, spark: SparkSession, conn_manager: ConnectionManager):
        # Save reference to the active SparkSession engine
        self.spark = spark
        # Save storage folder paths (landing_base, history_base, target_base)
        self.paths = conn_manager.paths
        # Save the ConnectionManager for database connection resolution
        self.conn_manager = conn_manager

        # Check whether Delta Lake is installed in the current Python environment
        # to avoid throwing 78 repeated SparkClassNotFoundExceptions during execution
        try:
            from delta.tables import DeltaTable
            self._delta_available = True
        except ImportError:
            self._delta_available = False

    @staticmethod
    def _log_target_route(target_cfg) -> None:
        """Log the resolved Spark target route without credentials or query options."""
        parsed = urlsplit(str(target_cfg.url).replace("jdbc:", "", 1))
        log.info(
            "[TargetRoute] tag=%s context=SPARK host=%s port=%s database=%s",
            getattr(target_cfg, "connection_tag", ""),
            parsed.hostname or "",
            parsed.port or "",
            parsed.path.lstrip("/"),
        )

    # -------------------------------------------------------------------------
    # Security Check: Require an active batch-log lease before writing anything!
    # -------------------------------------------------------------------------
    @staticmethod
    def _require_persistence_ownership(
        before_persist: Optional[Callable[[], None]],
    ) -> None:
        """
        Verify that a live batch-log lease owns the next persistence action.
        Think of this like showing your security badge at the warehouse gate:
        if you don't have a valid active badge, the gate stays locked!
        """
        from src.exceptions import ExecutionOwnershipRequiredError

        # If no badge callback was provided, raise an immediate error
        if before_persist is None:
            raise ExecutionOwnershipRequiredError(
                "Persistence requires an active batch_log lease."
            )
        # Execute the security check callback (verifies lease in metadata database)
        before_persist()

    # -------------------------------------------------------------------------
    # Target Probe Classifier: Translates confusing network/database errors
    # -------------------------------------------------------------------------
    @staticmethod
    def _classify_target_probe_failure(error: Exception) -> None:
        """
        Raise a stable, machine-classified target error unless the probe proves
        the table is simply absent (which is normal and allows auto-provisioning).
        """
        message = str(error)
        normalized = message.lower().replace("’", "'")
        # Phrases that mean: "The database is healthy, but the table doesn't exist yet!"
        # NOTE: Spark Connect JDBC wraps the PostgreSQL error as:
        #   relation "schema.table" does not exist
        # MySQL wraps error as:
        #   Table 'schema.table' doesn't exist
        # The table name sits between the prefix and suffix, so we check for canonical catch-alls:
        missing_table_markers = (
            "does not exist",           # PostgreSQL via Spark Connect: relation "schema.table" does not exist
            "relation does not exist",  # PostgreSQL via local Spark (no embedded name)
            "table or view does not exist", # Oracle: ORA-00942
            "doesn't exist",            # MySQL: Table 'schema.table' doesn't exist
            "unknown table",            # MySQL alternate
            "invalid object name",      # SQL Server (MSSQL): Invalid object name 'schema.table'
            "no such table",            # SQLite: no such table: schema.table
            "ora-00942",                # Oracle error code
        )
        # If the table is simply absent, we return cleanly so we can build it!
        if any(marker in normalized for marker in missing_table_markers):
            return
        # If it timed out, classify as TargetTimeoutError
        if any(marker in normalized for marker in ("timed out", "timeout", "sockettimeoutexception")):
            raise TargetTimeoutError(f"Target connection timed out: {message}") from error
        # If connection refused by host, classify as TargetRefusedError
        if "connection refused" in normalized:
            raise TargetRefusedError(f"Target connection was refused: {message}") from error
        # If password or access denied, classify as TargetAuthenticationError
        if any(marker in normalized for marker in ("authentication failed", "access denied", "password")):
            raise TargetAuthenticationError(f"Target authentication failed: {message}") from error
        # If unreachable, classify as TargetUnreachableError
        if any(marker in normalized for marker in ("unreachable", "connection attempt failed")):
            raise TargetUnreachableError(f"Target is unreachable: {message}") from error
        # Any other unexpected error during preflight
        raise TargetPreflightError(f"Target table probe failed: {message}") from error

    # ══════════════════════════════════════════════════════════════════════════
    # 1. BRONZE LAYER (Landing — Raw Parquet Files)
    # ══════════════════════════════════════════════════════════════════════════
    def write_landing(
        self,
        df: DataFrame,
        meta: MetadataRow,
        date_partition: str,
        before_persist: Optional[Callable[[], None]] = None,
        run_id: Optional[str] = None,
    ) -> str:
        """
        Write raw extracted data to Bronze (Landing) layer in Parquet format.
        Preserves native Spark datatypes (Decimal, Timestamp, Integer) exactly.
        Instruments write timing and inspects output directory file metrics.
        Progress is recorded by the orchestrator only after it verifies the
        persisted Parquet output.
        """
        import time
        from pathlib import Path
        from src.validation.stage0_policy import validate_stage0_execution_policy

        # Step A: Validate that this table is allowed to execute under Stage 0 safety freeze
        validate_stage0_execution_policy(meta)

        # Step B: Build the storage path: landing_base/source_tag/table_name/dt=YYYYMMDD
        path = (
            f"{self.paths['landing_base']}"
            f"/{meta.source_tag}"
            f"/{meta.landing_table or meta.source_table}"
            f"/dt={date_partition}"
        )
        if run_id:
            path = f"{path}/run_{run_id}"

        # Step C: Read target file size in MB from configuration (defaults to 128 MB)
        target_mb = self.conn_manager._raw_config.get("pipeline", {}).get("target_file_size_mb", 128)
        # Record the start time so we can calculate throughput
        write_start = time.time()
        # Security check: verify active lease badge
        self._require_persistence_ownership(before_persist)

        # One Spark-managed overwrite — no df.rdd access, compatible with Spark Connect.
        # Preserve native partitioning so Spark can use the cluster efficiently.
        log.info(
            f"[{meta.source_table}] [BRONZE_WRITE] Writing to Parquet (~{target_mb}MB target file size)..."
        )
        df.write.mode("overwrite").parquet(path)

        # Calculate how many seconds the write took
        write_elapsed = max(0.001, time.time() - write_start)

        # Step D: File Metric Tracking: Inspect the output folder on disk
        total_bytes = 0
        file_count = 0
        # Normalize local directory path for Windows/Linux
        local_dir_path = path.replace("file:///", "").replace("file://", "")
        if os.name == "nt" and not (len(local_dir_path) > 1 and local_dir_path[1] == ":"):
            local_dir_path = "C:/" + local_dir_path.lstrip("/")
        
        # Measure size of all generated .parquet files
        p = Path(local_dir_path)
        if p.exists():
            for file_path in p.rglob("*.parquet"):
                if file_path.is_file():
                    file_count += 1
                    total_bytes += file_path.stat().st_size

        # Convert bytes to megabytes
        total_mb = total_bytes / (1024 * 1024)
        avg_mb = (total_mb / file_count) if file_count > 0 else 0.0
        # Calculate write speed in Megabytes per second (MB/s)
        write_mbps = (total_mb / write_elapsed) if write_elapsed > 0 else 0.0

        # Log our performance statistics
        log.info(
            f"[{meta.source_table}] [BRONZE_WRITE_METRICS] Write Time: {write_elapsed:.3f}s | "
            f"Files: {file_count} | Size: {total_mb:.2f} MB | Avg File Size: {avg_mb:.2f} MB | Throughput: {write_mbps:.2f} MB/s"
        )
        return path

    # ══════════════════════════════════════════════════════════════════════════
    # 2. SILVER LAYER (History — Clean Parquet or Delta Archive)
    # ══════════════════════════════════════════════════════════════════════════
    def write_history(
        self,
        df: DataFrame,
        meta: MetadataRow,
        before_persist: Optional[Callable[[], None]] = None,
        run_id: Optional[str] = None,
        return_artifact: bool = False,
    ):
        """
        Write to Silver (history) layer.
        Appends or overwrites depending on whether a unique natural key exists.
        Falls back to Parquet if Delta format is not available in the Spark session.
        """
        # Step A: Build the folder path for history storage
        path = (
            f"{self.paths['history_base']}"
            f"/{meta.source_tag}"
            f"/{meta.history_table or meta.source_table}"
        )

        from src.validation.stage0_policy import (
            is_source_identity_execution_enabled,
            validate_stage0_execution_policy,
        )

        # Step B: Check Stage 0 policy
        validate_stage0_execution_policy(meta)
        # If we have a verified natural key, append new history; otherwise overwrite
        if is_source_identity_execution_enabled(meta):
            write_mode = WriteMode.APPEND
        else:
            write_mode = WriteMode.OVERWRITE
            print(f"  [Loader] No natural key for {meta.source_table} — using OVERWRITE for Silver")

        # Step C: Write using Delta Lake if available, else fallback to Parquet
        if self._delta_available:
            try:
                self._require_persistence_ownership(before_persist)
                (
                    df.write
                    .format(FileFormat.DELTA)
                    .mode(write_mode)
                    .partitionBy("source_tag")
                    .save(path)
                )
                print(f"  [Loader] Silver written using Delta Lake format ({write_mode}) at {path}")
            except Exception as e:
                print(f"  [Loader] Delta write failed, falling back to Parquet {write_mode} for Silver: {e}")
                self._delta_available = False # invalidate cache if it fails at runtime
                self._require_persistence_ownership(before_persist)
                (
                    df.write
                    .mode(write_mode)
                    .partitionBy("source_tag")
                    .parquet(path)
                )
        else:
            self._require_persistence_ownership(before_persist)
            (
                df.write
                .mode(write_mode)
                .partitionBy("source_tag")
                .parquet(path)
            )
        if run_id and return_artifact:
            # The cumulative history remains the canonical Silver store.  A
            # separate Parquet batch is immutable and gives Gold/retries an
            # exact producer-run input without filtering shared history.  It
            # must live outside the history table root: Spark recursively
            # reads that root during verification, and a nested run directory
            # would be mistaken for a conflicting partition layout.
            artifact_path = (
                f"{self.paths['history_base']}/artifacts/{meta.source_tag}"
                f"/{meta.history_table or meta.source_table}/run_{run_id}"
            )
            self._require_persistence_ownership(before_persist)
            df.write.mode("overwrite").format(FileFormat.PARQUET).save(artifact_path)
            return path, artifact_path
        return path

    def _write_history_delta_merge(
        self,
        df: DataFrame,
        meta: MetadataRow,
        base_path: str,
        merge_key: str,
        before_persist: Optional[Callable[[], None]] = None,
    ) -> str:
        """Delta Lake MERGE implementation for Silver history."""
        from src.exceptions import CDCPolicyNotMigratedError

        # Stage 0 Safety Freeze explicitly disables keyed Silver MERGE until KEY-01 migration
        raise CDCPolicyNotMigratedError(
            "Keyed Silver MERGE is disabled during the Stage 0 safety freeze."
        )

        # Retained unreachable implementation for KEY-01 migration work.
        from delta.tables import DeltaTable

        path_exists = True
        try:
            self.spark.read.format("delta").load(base_path)
        except Exception:
            path_exists = False

        if not path_exists:
            log.info(f"[Loader] Initializing Delta table at {base_path}")
            self._require_persistence_ownership(before_persist)
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(base_path)
            )
        else:
            log.info(f"[Loader] Delta MERGE on {merge_key} at {base_path}")
            delta_table = DeltaTable.forPath(self.spark, base_path)

            merge_cond = f"target.{merge_key} = updates.{merge_key}"
            if "source_identification_table" in df.columns:
                merge_cond += " AND target.source_identification_table = updates.source_identification_table"

            self._require_persistence_ownership(before_persist)
            (
                delta_table.alias("target")
                .merge(df.alias("updates"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        return base_path

    def _write_history_parquet_fallback(
        self,
        df: DataFrame,
        meta: MetadataRow,
        base_path: str,
        date_partition: str = None,
        before_persist: Optional[Callable[[], None]] = None,
    ) -> str:
        """Parquet overwrite fallback when Delta Lake is not installed."""
        path = f"{base_path}/dt={date_partition}" if date_partition else base_path
        self._require_persistence_ownership(before_persist)
        (
            df.write
            .mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .save(path)
        )
        return path

    # ══════════════════════════════════════════════════════════════════════════
    # 3. GOLD LAYER (Target — JDBC UPSERT / Delta Lake)
    # ══════════════════════════════════════════════════════════════════════════

    def write_target(
        self,
        df: DataFrame,
        meta: MetadataRow,
        merge_key: str,
        before_target_dml: Optional[Callable[[], None]] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Write to Gold (target) layer.
        Routes to JDBC staging-table UPSERT if target_tag is configured,
        otherwise writes to Delta Lake on disk.
        """
        # Verify active batch lease ownership
        self._require_persistence_ownership(before_target_dml)

        # Resolve the target database configuration
        target_cfg = None
        spark_target_cfg = None
        driver_target_cfg = None
        if meta.target_tag:
            try:
                target_cfg = self.conn_manager.get(meta.target_tag, expected_role="target")
                from src.connections.connection_manager import ExecutionContext
                spark_target_cfg = self.conn_manager.resolve_jdbc(
                    target_cfg, ExecutionContext.SPARK
                )
                driver_target_cfg = self.conn_manager.resolve_jdbc(
                    target_cfg, ExecutionContext.DRIVER
                )
                self._log_target_route(spark_target_cfg)
            except KeyError:
                log.warning(f"[Loader] target_tag '{meta.target_tag}' not found.")

        # If a target database is configured, perform an atomic staging UPSERT
        if target_cfg:
            return self._upsert_via_staging(
                df,
                meta,
                target_cfg,
                merge_key,
                before_target_dml,
                spark_target_cfg=spark_target_cfg,
                driver_target_cfg=driver_target_cfg,
            )
        else:
            # Otherwise write to file-based Delta Lake
            path, metrics = self._write_target_delta_merge(
                df, meta, merge_key, before_target_dml
            )
            return path, metrics

    def write_target_bounded_exception(
        self,
        df: DataFrame,
        meta: MetadataRow,
        merge_key: str,
        exception_values,
        before_target_dml: Optional[Callable[[], None]] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Publish a bounded-exception identity in two deterministic lanes.

        Normal key values use the existing idempotent staging UPSERT.  The
        small, frozen exception set is replaced atomically in one target
        statement, preserving all source rows without weakening uniqueness for
        the normal lane.  Target adapters must certify this capability before
        any DML; unsupported engines fail closed.
        """
        from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec
        from src.exceptions import BusinessKeyRequiredError, TargetPreflightError

        SchemaValidator.validate_dataframe(df, meta.source_table)
        approved_keys = tuple(getattr(meta, "canonical_source_business_key", ()) or ())
        if len(approved_keys) != 1 or str(merge_key).strip().lower() != approved_keys[0].lower():
            raise BusinessKeyRequiredError(
                "Bounded-exception publication requires the approved single-column source key."
            )
        values = Int64ExceptionCodec.normalize(exception_values)
        identity = getattr(meta, "identity_mode", IdentityMode.EXACT_KEY)
        identity_name = str(getattr(identity, "value", identity) or "").upper()
        if identity_name != IdentityMode.BOUNDED_EXCEPTION_KEY.value or not values:
            raise TargetPreflightError(
                f"TARGET_CONDITIONAL_IDENTITY_INVALID: {meta.source_table} has no valid bounded exception contract."
            )
        key_column = approved_keys[0]
        if key_column not in df.columns:
            raise BusinessKeyRequiredError(
                f"Approved bounded-exception key column '{key_column}' is absent from the Gold projection."
            )

        target_connection = self.conn_manager.get(meta.target_tag, expected_role="target")
        from src.connections.connection_manager import ExecutionContext
        spark_target_cfg = self.conn_manager.resolve_jdbc(target_connection, ExecutionContext.SPARK)
        driver_target_cfg = self.conn_manager.resolve_jdbc(target_connection, ExecutionContext.DRIVER)
        self._log_target_route(spark_target_cfg)
        write_schema = getattr(target_connection, "schema", None) or meta.target_schema or "public"
        table_name = meta.target_table or meta.source_table
        target_fqn = f"{write_schema}.{table_name}"

        # Preflight on the complete projection provisions the target schema and
        # certifies the partial/filtered uniqueness boundary.  It also checks
        # normal-lane duplicate groups while allowing only frozen exceptions.
        self.provision_target_schema_preflight(
            df,
            meta,
            target_connection,
            merge_key=merge_key,
            before_persist=before_target_dml,
            spark_target_cfg=spark_target_cfg,
        )

        exception_df = df.filter(F.col(key_column).isin(list(values)))
        normal_df = df.filter(~F.col(key_column).isin(list(values)))
        exception_count = exception_df.count()
        normal_count = normal_df.count()
        normal_metrics: Dict[str, Any] = {
            "rows": 0,
            "target_persisted_rows": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped": 0,
        }
        if normal_count:
            _, normal_metrics = self._upsert_via_staging(
                normal_df,
                meta,
                target_connection,
                merge_key=merge_key,
                before_target_dml=before_target_dml,
                spark_target_cfg=spark_target_cfg,
                driver_target_cfg=driver_target_cfg,
                conditional_exception_values=values,
            )

        exception_metrics: Dict[str, Any] = {
            "rows": exception_count,
            "staging_write_sec": 0.0,
            "replace_execute_sec": 0.0,
        }
        # Always execute the exception replacement, even when the current
        # source window contains zero exception rows.  The empty staged set is
        # meaningful: it removes stale target rows for a frozen exception key
        # that has since been deleted at the source.  Spark JDBC creates the
        # schema for an empty DataFrame before the statement executes.
        matching_cols = list(exception_df.columns)
        try:
            exception_df, matching_cols = self._align_target_dataframe(
                exception_df,
                meta,
                target_connection,
                spark_target_cfg,
                target_fqn,
                before_target_dml,
            )
        except LoaderValidationError:
            raise
        exception_stage = f"{target_fqn}_cond_exceptions_stg"
        control_cfg = {
            "url": getattr(driver_target_cfg, "url", None) or getattr(driver_target_cfg, "jdbc_url", None),
            "user": getattr(driver_target_cfg, "username", None) or getattr(driver_target_cfg, "user", None),
            "password": getattr(driver_target_cfg, "password", None),
            "driver": getattr(driver_target_cfg, "driver", getattr(target_connection, "driver", "")),
            "schema": write_schema,
            "table": table_name,
        }
        db_type = getattr(target_connection, "db_type", None) or getattr(spark_target_cfg, "db_type", "")
        from src.utils.db_utils import build_bounded_exception_dml
        lane_sql = build_bounded_exception_dml(
            db_type=db_type,
            normal_stage_table=f"{target_fqn}_normal_unused",
            exception_stage_table=exception_stage,
            target_table=target_fqn,
            merge_key=merge_key,
            columns=matching_cols,
            exception_values=values,
        )
        try:
            self._require_persistence_ownership(before_target_dml)
            writer = _apply_jdbc_options(exception_df.write.format("jdbc"), spark_target_cfg)
            writer.option("dbtable", quote_table_name(exception_stage, db_type)) \
                .option("batchsize", "10000") \
                .mode("overwrite") \
                .save()
            replace_sql = lane_sql.get("exception_replace")
            if not replace_sql:
                raise TargetPreflightError(
                    f"TARGET_CONDITIONAL_IDENTITY_UNSUPPORTED: atomic exception replacement is not available for {db_type}."
                )
            self._require_persistence_ownership(before_target_dml)
            safe_execute_jdbc(
                df=None,
                target_cfg={**control_cfg, "sql": replace_sql},
                operation="execute_sql",
                spark=self.spark,
            )
        finally:
            try:
                safe_execute_jdbc(
                    df=None,
                    target_cfg={
                        **control_cfg,
                        "sql": f"DROP TABLE IF EXISTS {quote_table_name(exception_stage, db_type)}",
                    },
                    operation="drop",
                    spark=self.spark,
                )
            except Exception as exc:
                log.warning("[Loader] Could not drop conditional exception stage %s: %s", exception_stage, exc)
        exception_metrics["target_persisted_rows"] = exception_count

        total_rows = normal_count + exception_count
        capability = getattr(meta, "_target_identity_capability", None)
        capability_evidence = dict(getattr(capability, "evidence", {}) or {})
        capability_evidence["capable"] = bool(
            getattr(capability, "supported", False)
        )
        metrics = {
            **normal_metrics,
            "rows": total_rows,
            "target_persisted_rows": total_rows,
            "rows_inserted": int(normal_metrics.get("rows_inserted", 0)),
            "rows_updated": int(normal_metrics.get("rows_updated", 0)),
            "exception_rows_replaced": exception_count,
            "target_reconciliation": {
                "proof_mode": "BOUNDED_EXCEPTION_REPLACE",
                "staging_table": f"{target_fqn}_cond_exceptions_stg" if exception_count else None,
                "staged_rows": total_rows,
                "matched_rows": total_rows,
                "missing_rows": 0,
                "expected_rows": total_rows,
                "identity_mode": IdentityMode.BOUNDED_EXCEPTION_KEY.value,
            },
            "target_identity_capability": capability_evidence,
        }
        return f"jdbc:{target_fqn}", metrics

    def provision_target_schema_preflight(
        self,
        df: Optional[DataFrame],
        meta: MetadataRow,
        target_cfg,
        merge_key: Optional[str] = None,
        before_persist: Optional[Callable[[], None]] = None,
        spark_target_cfg=None,
    ) -> None:
        """
        Target Preflight Checklist:
        1. Ensure business key is valid (no NULLs, no duplicates).
        2. Create the target table from the frozen dataframe schema when absent.
        3. Ensure target database uniqueness (PK/UNIQUE) before target DML.
        4. Fail hard if verification is unavailable or incompatible.
        """
        from src.exceptions import (
            BusinessKeyRequiredError,
            TargetConstraintRequiredError,
            TargetPreflightError,
        )
        from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec
        # Verify active lease badge
        self._require_persistence_ownership(before_persist)

        # 1. Null / Duplicate checks on business keys
        raw_key = getattr(meta, "canonical_source_business_key", None)
        if raw_key and df is not None:
            from src.utils.db_utils import normalize_and_validate_merge_keys
            try:
                raw_key = normalize_and_validate_merge_keys(raw_key, columns=df.columns)
            except Exception as e:
                pass # fallback to metadata casing if missing or error
        key_tuple = tuple(raw_key) if isinstance(raw_key, (list, tuple)) else tuple()
        raw_identity = getattr(meta, "identity_mode", None)
        if raw_identity is None:
            approved_key = getattr(getattr(meta, "table_contract", None), "approved_source_business_key", None)
            raw_identity = getattr(approved_key, "identity_mode", IdentityMode.EXACT_KEY)
        identity_name = str(getattr(raw_identity, "value", raw_identity) or IdentityMode.EXACT_KEY).upper()
        conditional_identity = identity_name == IdentityMode.BOUNDED_EXCEPTION_KEY.value
        try:
            exception_values = Int64ExceptionCodec.normalize(
                getattr(meta, "exception_values", ())
                or getattr(getattr(getattr(meta, "table_contract", None), "approved_source_business_key", None), "exception_values", ())
                or ()
            )
        except Exception as exc:
            raise TargetPreflightError(
                f"TARGET_CONDITIONAL_IDENTITY_INVALID: exception values for {meta.source_table} are invalid."
            ) from exc
        if conditional_identity and (len(key_tuple) != 1 or not exception_values):
            raise TargetPreflightError(
                f"TARGET_CONDITIONAL_IDENTITY_INVALID: {meta.source_table} requires one key column and a bounded exception set."
            )
        is_direct = (str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT")
        is_keyed_upsert = not is_direct and ((str(getattr(meta, "is_cdc", "")).lower() == "y") or bool(key_tuple))
        trusted_key = has_stage0_trusted_business_key(meta)
        if is_keyed_upsert and not trusted_key:
            raise BusinessKeyRequiredError(
                f"No explicitly trusted execution key is available for UPSERT on {meta.source_table}."
            )
        if not is_direct:
            validate_stage0_execution_policy(meta)
        
        if df is not None and is_keyed_upsert and trusted_key:
            # Check A: Make sure no business key column has blank / NULL values
            null_cond = " OR ".join([f"{k} IS NULL" for k in key_tuple])
            null_count = df.filter(null_cond).count()
            if null_count > 0:
                raise TargetPreflightError(
                    f"TARGET_KEY_NULL_VALUES: {null_count} row(s) contain NULL in business keys {key_tuple} for {meta.source_table}"
                )
            # Check B: Make sure there are no duplicate rows with the exact same business key
            duplicate_input = df
            if conditional_identity:
                duplicate_input = df.filter(~F.col(key_tuple[0]).isin(list(exception_values)))
            dup_df = duplicate_input.groupBy(*key_tuple).count().filter("count > 1")
            dup_count = dup_df.count()
            if dup_count > 0:
                raise TargetPreflightError(
                    f"TARGET_KEY_DUPLICATES: {dup_count} duplicate key(s) {key_tuple} for {meta.source_table}"
                )

        # Build the full schema.table name
        write_schema = target_cfg.schema or meta.target_schema or "public"
        table_name = meta.target_table or meta.source_table
        target_fqn = f"{write_schema}.{table_name}"
        alias_kw = " " if target_cfg.db_type == "oracle" else " AS "

        spark_target_cfg = spark_target_cfg or target_cfg
        # 2. Check if the table already exists through the same Spark endpoint
        # that will create/read it.  The driver endpoint remains reserved for
        # transactional constraint DDL below.
        table_exists = self._target_table_exists(
            target_fqn, target_cfg.db_type, alias_kw, spark_target_cfg
        )

        # If table doesn't exist, automatically provision it from DataFrame schema
        if not table_exists:
            self._provision_missing_target(
                df=df,
                meta=meta,
                target_cfg=target_cfg,
                target_fqn=target_fqn,
                key_tuple=tuple() if conditional_identity else (key_tuple if is_keyed_upsert else tuple()),
                before_persist=before_persist,
                spark_target_cfg=spark_target_cfg,
            )
            if not self._target_table_exists(
                target_fqn, target_cfg.db_type, alias_kw, spark_target_cfg
            ):
                raise TargetTableNotFoundError(
                    f"TARGET_TABLE_CREATE_FAILED: {target_fqn} was not visible after provisioning."
                )

        # 3. Check for unique constraint on target table
        if conditional_identity:
            inspector = get_schema_inspector(target_cfg)
            try:
                capability = inspector.certify_bounded_exception_key(
                    write_schema,
                    table_name,
                    key_tuple,
                    exception_values,
                    target_cfg,
                    create=True,
                )
            except Exception as exc:
                raise TargetPreflightError(
                    f"TARGET_CONDITIONAL_IDENTITY_INSPECTION_FAILED: Could not certify bounded identity for {target_fqn}: {exc}"
                ) from exc
            if not capability.supported:
                reason = capability.evidence.get("reason") if capability.evidence else "target adapter did not certify bounded identity"
                raise TargetPreflightError(
                    f"TARGET_CONDITIONAL_IDENTITY_UNSUPPORTED: {target_fqn}: {reason}"
                )
            # The capability is immutable evidence for this invocation; retain
            # it only on the mutable metadata object for the caller's metrics.
            setattr(meta, "_target_identity_capability", capability)
        elif is_keyed_upsert:
            inspector = get_schema_inspector(target_cfg)
            try:
                has_constraint = inspector.supports_unique_key(
                    write_schema, table_name, key_tuple, target_cfg
                )
            except Exception as e:
                raise TargetPreflightError(
                    f"TARGET_CONSTRAINT_INSPECTION_FAILED: Could not inspect schema for {target_fqn}: {e}"
                ) from e
            if not has_constraint:
                self._create_target_unique_constraint(
                    meta=meta,
                    target_cfg=target_cfg,
                    target_fqn=target_fqn,
                    key_tuple=key_tuple,
                    before_persist=before_persist,
                )
                try:
                    has_constraint = inspector.supports_unique_key(
                        write_schema, table_name, key_tuple, target_cfg
                    )
                except Exception as e:
                    raise TargetPreflightError(
                        f"TARGET_CONSTRAINT_INSPECTION_FAILED: Could not inspect schema for {target_fqn}: {e}"
                    ) from e
            if not has_constraint:
                raise TargetConstraintRequiredError(
                    f"An existing compatible target constraint is required for {target_fqn}."
                )

        # Relax NOT NULL on non-key columns for pre-existing PostgreSQL targets.
        # _provision_missing_target already does this for newly-created tables;
        # here we extend the same idempotent protection to tables that already
        # existed (e.g. tbltasklog.createddate contains MySQL zero-dates read as
        # NULL by Spark which would otherwise violate a pre-existing NOT NULL).
        if str(getattr(target_cfg, "db_type", "")).lower() in {
            "postgres", "postgresql", "postgressql"
        }:
            from src.utils.db_utils import _relax_not_null_constraints
            try:
                merge_key_val = ",".join(key_tuple) if key_tuple else ""
                _relax_not_null_constraints(target_cfg, target_fqn, merge_key_val)
            except Exception as _relax_exc:
                log.warning(
                    "[Preflight] Could not relax NOT NULL for pre-existing %s: %s",
                    target_fqn, _relax_exc,
                )

    @staticmethod
    def _target_constraint_name(target_fqn: str, key_tuple: tuple) -> str:
        """Return a deterministic, safe, bounded constraint identifier (e.g. uq_etl_1a2b3c4d)."""
        material = f"{target_fqn}|{','.join(key_tuple)}".encode("utf-8")
        return f"uq_etl_{hashlib.sha256(material).hexdigest()[:16]}"

    def _target_table_exists(self, target_fqn: str, db_type: str, alias_kw: str, spark_target_cfg) -> bool:
        """Probe target existence through the Spark-side target connection."""
        try:
            (
                _apply_jdbc_options(self.spark.read.format("jdbc"), spark_target_cfg)
                .option(
                    "dbtable",
                    f"(SELECT 1 FROM {quote_table_name(target_fqn, db_type)} WHERE 1=0){alias_kw}schema_temp",
                )
                .load()
                .limit(1)
                .count()
            )
            return True
        except Exception as probe_error:
            self._classify_target_probe_failure(probe_error)
            return False

    def _provision_missing_target(
        self,
        *,
        df: Optional[DataFrame],
        meta: MetadataRow,
        target_cfg,
        target_fqn: str,
        key_tuple: tuple,
        before_persist: Optional[Callable[[], None]],
        spark_target_cfg,
    ) -> None:
        """
        Create a missing target table during the owned landing preflight.
        Creates the schema and empty mapped table only; the caller rechecks
        table visibility before it creates or verifies the key constraint.
        """
        if df is None:
            raise TargetTableNotFoundError(
                f"Target table {target_fqn} is absent and no source schema is available for provisioning."
            )
        target_tag = str(getattr(meta, "target_tag", "") or "").strip()
        if not target_tag:
            raise TargetResolutionError(
                f"Cannot provision {target_fqn}: target_tag is missing from the frozen contract."
            )

        self._require_persistence_ownership(before_persist)
        db_type = str(getattr(target_cfg, "db_type", "") or "").lower()
        if db_type not in {"postgres", "postgresql", "postgressql", "mysql", "oracle"}:
            raise TargetPreflightError(
                f"TARGET_AUTO_PROVISION_UNSUPPORTED: target database type '{db_type}'."
            )

        write_schema = getattr(target_cfg, "schema", None) or getattr(meta, "target_schema", None) or "public"
        quoted_target = quote_table_name(target_fqn, db_type)

        # Step 1: PostgreSQL requires CREATE SCHEMA IF NOT EXISTS before creating tables
        if db_type in {"postgres", "postgresql", "postgressql"}:
            self._require_persistence_ownership(before_persist)
            with self.conn_manager.get_db_connection(target_tag, expected_role="target") as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"CREATE SCHEMA IF NOT EXISTS {quote_table_name(write_schema, db_type)}"
                    )
                conn.commit()

        # Step 2: Use Spark JDBC with limit(0) to create the table structure without writing rows
        self._require_persistence_ownership(before_persist)
        writer = _apply_jdbc_options(df.limit(0).write.format("jdbc"), spark_target_cfg)
        writer.option("dbtable", quoted_target).mode("ignore").save()

        # Step 3: Relax NOT NULL constraints for non-PK columns
        from src.utils.db_utils import _relax_not_null_constraints
        try:
            merge_key_tuple = getattr(meta, "canonical_source_business_key", tuple()) or tuple()
            merge_key_val = ",".join(merge_key_tuple) if merge_key_tuple else ""
            _relax_not_null_constraints(target_cfg, target_fqn, merge_key_val)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Could not relax NOT NULL constraints for {target_fqn}: {e}")

    @staticmethod
    def _postgres_ddl_type(data_type) -> str:
        """Map a frozen Spark projection type to lossless PostgreSQL DDL."""
        simple = str(data_type.simpleString()).lower()
        if simple == "string":
            return "TEXT"
        if simple in {"byte", "short", "int", "integer"}:
            return "INTEGER"
        if simple in {"bigint", "long"}:
            return "BIGINT"
        if simple.startswith("decimal("):
            return simple.upper().replace("DECIMAL", "NUMERIC", 1)
        if simple in {"float", "double"}:
            return "DOUBLE PRECISION"
        if simple == "boolean":
            return "BOOLEAN"
        if simple == "date":
            return "DATE"
        if simple.startswith("timestamp"):
            return "TIMESTAMP"
        if simple == "binary":
            return "BYTEA"
        raise LoaderValidationError(
            f"TARGET_SCHEMA_EVOLUTION_UNSUPPORTED_TYPE: {simple}"
        )

    def _evolve_target_schema(
        self,
        *,
        df: DataFrame,
        meta: MetadataRow,
        target_cfg,
        target_fqn: str,
        target_schema_fields,
        before_persist: Optional[Callable[[], None]],
    ) -> bool:
        """Add frozen projection columns missing from an existing PostgreSQL target."""
        target_names = {field.name.lower() for field in target_schema_fields}
        missing_fields = [
            field for field in df.schema.fields if field.name.lower() not in target_names
        ]
        if not missing_fields:
            return False

        policy = str(getattr(meta, "schema_evolution_policy", "PERMISSIVE") or "").upper()
        if policy not in {"PERMISSIVE", "AUTO"}:
            raise LoaderValidationError(
                f"TARGET_SCHEMA_EVOLUTION_REQUIRED: {target_fqn} is missing "
                f"{len(missing_fields)} frozen projection column(s)."
            )
        db_type = str(getattr(target_cfg, "db_type", "") or "").lower()
        if db_type not in {"postgres", "postgresql", "postgressql"}:
            raise LoaderValidationError(
                f"TARGET_SCHEMA_EVOLUTION_UNSUPPORTED: additive evolution for {db_type}."
            )

        additions = ", ".join(
            "ADD COLUMN IF NOT EXISTS "
            f"{quote_column_name(field.name, 'postgres')} "
            f"{self._postgres_ddl_type(field.dataType)}"
            for field in missing_fields
        )
        self._require_persistence_ownership(before_persist)
        with self.conn_manager.get_db_connection(
            meta.target_tag, expected_role="target"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {quote_table_name(target_fqn, 'postgres')} {additions}"
                )
            conn.commit()
        log.info(
            "[TargetSchemaEvolution] Added %s frozen projection column(s) to %s.",
            len(missing_fields),
            target_fqn,
        )
        return True

    def _create_target_unique_constraint(
        self,
        *,
        meta: MetadataRow,
        target_cfg,
        target_fqn: str,
        key_tuple: tuple,
        before_persist: Optional[Callable[[], None]],
    ) -> None:
        """Create the approved target key constraint before any target DML."""
        if not key_tuple:
            return

        target_tag = str(getattr(meta, "target_tag", "") or "").strip()
        if not target_tag:
            raise TargetResolutionError(
                f"Cannot create a target constraint for {target_fqn}: target_tag is missing from the frozen contract."
            )

        db_type = str(getattr(target_cfg, "db_type", "") or "").lower()
        quoted_target = quote_table_name(target_fqn, db_type)
        constraint_name = quote_column_name(
            self._target_constraint_name(target_fqn, key_tuple), db_type
        )
        try:
            self._require_persistence_ownership(before_persist)
            with self.conn_manager.get_db_connection(target_tag, expected_role="target") as conn:
                # Resolve actual target column casing from information_schema so that
                # tables provisioned with UPPERCASE columns (via Spark JDBC, e.g. DESIG_ID)
                # and tables provisioned with lowercase columns (via PostgreSQL DDL,
                # e.g. task_id) both succeed.  Without this, quoting the business-key
                # column name as-is from metadata fails for whichever casing is wrong.
                write_schema = (
                    getattr(target_cfg, "schema", None)
                    or getattr(meta, "target_schema", None)
                    or "public"
                )
                table_name_only = target_fqn.split(".", 1)[-1]
                actual_case_map: dict = {}
                col_type_map: dict = {}
                if db_type in {"postgres", "postgresql", "postgressql", "mysql"}:
                    with conn.cursor() as _cur:
                        _cur.execute(
                            "SELECT column_name, data_type FROM information_schema.columns "
                            "WHERE LOWER(table_schema) = LOWER(%s) AND LOWER(table_name) = LOWER(%s)",
                            (write_schema, table_name_only),
                        )
                        for col_name, data_type in _cur.fetchall():
                            actual_case_map[col_name.lower()] = col_name
                            col_type_map[col_name.lower()] = str(data_type).lower()
                resolved_keys = [
                    actual_case_map.get(col.lower(), col) for col in key_tuple
                ]

                # In MySQL, BLOB/TEXT columns cannot be used in a UNIQUE constraint / index
                # without an explicit key length (MySQL error 1170). Spark JDBC provisions string
                # columns as LONGTEXT/TEXT. For all resolved business key columns that are TEXT/BLOB,
                # convert them to VARCHAR so the UNIQUE index can be created cleanly.
                if db_type == "mysql":
                    varchar_len = 255 if len(resolved_keys) <= 3 else max(64, 768 // len(resolved_keys))
                    with conn.cursor() as _cur:
                        for col in resolved_keys:
                            c_type = col_type_map.get(col.lower(), "")
                            if c_type in {
                                "text", "tinytext", "mediumtext", "longtext",
                                "blob", "tinyblob", "mediumblob", "longblob",
                            }:
                                _cur.execute(
                                    f"ALTER TABLE {quoted_target} MODIFY {quote_column_name(col, db_type)} VARCHAR({varchar_len})"
                                )
                    conn.commit()

                quoted_columns = ", ".join(
                    quote_column_name(col, db_type) for col in resolved_keys
                )
                with conn.cursor() as cur:
                    cur.execute(
                        f"ALTER TABLE {quoted_target} ADD CONSTRAINT {constraint_name} "
                        f"UNIQUE ({quoted_columns})"
                    )
                conn.commit()
        except Exception as exc:
            # If a concurrent landing attempt already created the constraint, accept it if verified
            try:
                write_schema = getattr(target_cfg, "schema", None) or getattr(meta, "target_schema", None) or "public"
                if get_schema_inspector(target_cfg).supports_unique_key(
                    write_schema,
                    getattr(meta, "target_table", None) or getattr(meta, "source_table", ""),
                    key_tuple,
                    target_cfg,
                ):
                    return
            except Exception:
                pass
            raise TargetPreflightError(
                f"TARGET_CONSTRAINT_CREATE_FAILED: could not create a unique constraint for {target_fqn}: {exc}"
            ) from exc

    def _align_target_dataframe(
        self,
        df: DataFrame,
        meta: MetadataRow,
        target_cfg,
        spark_target_cfg,
        target_fqn: str,
        before_persist: Optional[Callable[[], None]],
    ) -> Tuple[DataFrame, list]:
        """Align one frozen projection with an already-provisioned target.

        Both conditional lanes use this same mapping so the exception lane
        cannot drift from the normal upsert lane.  The target schema is read
        through Spark JDBC, while additive evolution remains governed by the
        existing schema policy.
        """
        if str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT":
            alias_kw = " " if str(target_cfg.db_type).lower() == "oracle" else " AS "
            target_schema_df = (
                _apply_jdbc_options(self.spark.read.format("jdbc"), spark_target_cfg)
                .option(
                    "dbtable",
                    f"(SELECT * FROM {quote_table_name(target_fqn, target_cfg.db_type)} WHERE 1=0){alias_kw}schema_temp",
                )
                .load()
            )
            target_cols_map = {f.name.lower(): f.name for f in target_schema_df.schema.fields}
            direct_cols = [
                F.col(c).alias(target_cols_map[c.lower()])
                for c in df.columns
                if c.lower() in target_cols_map
            ]
            aligned_df = df.select(*direct_cols)
            return aligned_df, list(aligned_df.columns)
        alias_kw = " " if str(target_cfg.db_type).lower() == "oracle" else " AS "
        target_schema_df = (
            _apply_jdbc_options(self.spark.read.format("jdbc"), spark_target_cfg)
            .option(
                "dbtable",
                f"(SELECT * FROM {quote_table_name(target_fqn, target_cfg.db_type)} WHERE 1=0){alias_kw}schema_temp",
            )
            .load()
        )
        if self._evolve_target_schema(
            df=df,
            meta=meta,
            target_cfg=target_cfg,
            target_fqn=target_fqn,
            target_schema_fields=target_schema_df.schema.fields,
            before_persist=before_persist,
        ):
            target_schema_df = (
                _apply_jdbc_options(self.spark.read.format("jdbc"), spark_target_cfg)
                .option(
                    "dbtable",
                    f"(SELECT * FROM {quote_table_name(target_fqn, target_cfg.db_type)} WHERE 1=0){alias_kw}schema_temp",
                )
                .load()
            )
        return SchemaValidator.validate_and_align_schema(
            df=df,
            target_schema_fields=target_schema_df.schema.fields,
            source_table=meta.source_table,
            target_fqn=target_fqn,
            evolution_policy=getattr(meta, "schema_evolution_policy", "PERMISSIVE"),
        )

    def _upsert_via_staging(
        self,
        df: DataFrame,
        meta: MetadataRow,
        target_cfg,
        merge_key: str,
        before_target_dml=None,
        spark_target_cfg=None,
        driver_target_cfg=None,
        conditional_exception_values=None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Write to a JDBC target using a staging-table UPSERT:
        1. Execute pre-flight DDL provisioning if not already completed.
        2. Align DataFrame columns with target table schema using SchemaValidator.
        3. Write to staging table via JDBC overwrite (DML).
        4. Execute DB-type-specific UPSERT SQL (DML).
        5. Drop staging table.
        """
        # Validate DataFrame schema locally
        SchemaValidator.validate_dataframe(df, meta.source_table)

        # Step 1: Pre-flight DDL provisioning and constraint verification
        self.provision_target_schema_preflight(
            df,
            meta,
            target_cfg,
            merge_key,
            before_persist=before_target_dml,
            spark_target_cfg=spark_target_cfg,
        )

        spark_target_cfg = spark_target_cfg or target_cfg
        driver_target_cfg = driver_target_cfg or target_cfg

        from src.utils.db_utils import normalize_and_validate_merge_keys

        # Step 2: Validate that merge_key exactly matches trusted canonical metadata key
        canonical_merge_keys = normalize_and_validate_merge_keys(
            merge_key,
            columns=list(df.columns),
        )
        metadata_keys = [key.lower() for key in (meta.canonical_source_business_key or tuple())]
        if [key.lower() for key in canonical_merge_keys] != metadata_keys:
            from src.exceptions import BusinessKeyRequiredError

            raise BusinessKeyRequiredError(
                f"The UPSERT key {canonical_merge_keys} must exactly match the trusted canonical metadata key {metadata_keys}."
            )

        # Step 3: Resolve destination table full name
        write_schema = target_cfg.schema or meta.target_schema or "public"
        table_name = meta.target_table or meta.source_table
        target_fqn = f"{write_schema}.{table_name}"
        driver_class = target_cfg.driver

        # Step 4: Align columns with target table schema using SchemaValidator
        try:
            write_df, matching_cols = self._align_target_dataframe(
                df, meta, target_cfg, spark_target_cfg, target_fqn, before_target_dml
            )
        except Exception as e:
            if isinstance(e, LoaderValidationError):
                raise e
            log.warning(
                f"  [Loader] Could not query target schema: {e}. Writing raw DataFrame."
            )
            write_df = df
            matching_cols = list(df.columns)

        target_jdbc_url = spark_target_cfg.url

        # Step 5: Build target config dictionary for db_utils
        target_cfg_dict = {
            "url": target_jdbc_url,
            "user": getattr(spark_target_cfg, "username", None) or spark_target_cfg.user,
            "password": spark_target_cfg.password,
            "driver": spark_target_cfg.driver,
            "schema": write_schema,
            "table": table_name,
        }
        driver_cfg_dict = {
            "url": getattr(driver_target_cfg, "url", None)
            or getattr(driver_target_cfg, "jdbc_url", None),
            "user": getattr(driver_target_cfg, "username", None)
            or getattr(driver_target_cfg, "user", None),
            "password": getattr(driver_target_cfg, "password", None),
            "driver": getattr(driver_target_cfg, "driver", driver_class),
            "schema": write_schema,
            "table": table_name,
        }

        # Step 6: Pre-write diagnostic logging
        _diag_row_count = write_df.count()  # single Spark action — reused below
        log.info(f"  [Loader Pre-Write Diagnostic] Target Table: '{target_fqn}'")
        log.info(f"  [Loader Pre-Write Diagnostic] Row Count   : {_diag_row_count}")
        log.info(f"  [Loader Pre-Write Diagnostic] Columns ({len(write_df.columns)}): {list(write_df.columns)}")
        log.info(f"  [Loader Pre-Write Diagnostic] Schema:\n{write_df.schema.simpleString()}")

        # Step 6b: Sort staging DataFrame within partitions by merge key
        # Ensures index page locality when target database joins/probes staging data.
        if merge_key and hasattr(write_df, "sortWithinPartitions"):
            try:
                key_cols = [k.strip() for k in merge_key.split(",") if k.strip()] if isinstance(merge_key, str) else list(merge_key)
                valid_sort_keys = [k for k in key_cols if k in write_df.columns]
                if valid_sort_keys:
                    write_df = write_df.sortWithinPartitions(*valid_sort_keys)
            except Exception as sort_err:
                log.debug(f"[Loader] Non-fatal sortWithinPartitions error: {sort_err}")

        # Step 7: Execute UPSERT via temporary staging table
        log.info(f"  [Loader] UPSERT to {target_fqn} via staging table (db_type={target_cfg.db_type})")
        target_cols_list = matching_cols if isinstance(matching_cols, list) else list(write_df.columns)
        target_col_case_map = {c.lower(): c for c in target_cols_list}
        if isinstance(merge_key, str):
            aligned_merge_key = ",".join(
                target_col_case_map.get(k.strip().lower(), k.strip())
                for k in merge_key.split(",")
                if k.strip()
            )
        elif isinstance(merge_key, (list, tuple)):
            aligned_merge_key = [
                target_col_case_map.get(str(k).lower(), str(k)) for k in merge_key
            ]
        else:
            aligned_merge_key = merge_key

        upsert_metrics = execute_upsert(
            spark=self.spark,
            df=write_df,
            target_cfg=target_cfg_dict,
            target_table=target_fqn,
            merge_key=aligned_merge_key,
            columns=target_cols_list,
            db_type=target_cfg.db_type,
            before_target_dml=before_target_dml,
            driver_target_cfg=driver_cfg_dict,
            conditional_exception_values=conditional_exception_values,
        )

        # Step 8: If PostgreSQL target, run ANALYZE to update query planner statistics
        if target_cfg.db_type.lower() == "postgres":
            try:
                analyze_cfg = {
                    **driver_cfg_dict,
                    "schema": write_schema,
                    "table": table_name,
                    "sql": f"ANALYZE {quote_table_name(target_fqn, target_cfg.db_type)}",
                }

                self._require_persistence_ownership(before_target_dml)
                safe_execute_jdbc(
                    df=None,
                    target_cfg=analyze_cfg,
                    operation="execute_sql",
                    spark=self.spark,
                )

                log.info(f"ANALYZE completed for {target_fqn}")

            except Exception as e:
                log.warning(f"ANALYZE skipped for {target_fqn}: {e}")    
        return f"jdbc:{target_fqn}", upsert_metrics

    # -------------------------------------------------------------------------
    # Gold Full Snapshot: TRUNCATE + bulk INSERT (for reference / lookup tables)
    # -------------------------------------------------------------------------
    def write_target_overwrite(
        self,
        df: DataFrame,
        meta: MetadataRow,
        before_target_dml: Optional[Callable[[], None]] = None,
    ) -> str:
        """
        Write to Gold (target) layer using TRUNCATE + INSERT (full snapshot).
        Used for is_cdc='n' tables (reference/dimension tables).
        Routing:
          - JDBC target_tag -> TRUNCATE then bulk INSERT via JDBC.
          - File-based      -> Parquet overwrite.
        """
        from src.validation.stage0_policy import validate_stage0_execution_policy

        validate_stage0_execution_policy(meta)
        self._require_persistence_ownership(before_target_dml)
        SchemaValidator.validate_dataframe(df, meta.source_table)

        target_cfg = None
        target_connection = None
        if meta.target_tag:
            try:
                target_connection = self.conn_manager.get(meta.target_tag, expected_role="target")
                from src.connections.connection_manager import ExecutionContext
                target_cfg = self.conn_manager.resolve_jdbc(target_connection, ExecutionContext.SPARK)
                driver_target_cfg = self.conn_manager.resolve_jdbc(target_connection, ExecutionContext.DRIVER)
                self._log_target_route(target_cfg)
            except Exception as resolution_error:
                raise TargetResolutionError(
                    f"Could not resolve configured target tag '{meta.target_tag}'."
                ) from resolution_error

        if target_cfg:
            write_schema = target_connection.schema or meta.target_schema or "public"
            table_name = meta.target_table or meta.source_table
            target_fqn = f"{write_schema}.{table_name}"
            driver_class = target_cfg.driver

            merge_key_tuple = meta.canonical_source_business_key or tuple()
            merge_key_val = ",".join(merge_key_tuple) if merge_key_tuple else None
            # Provision schema preflight
            self.provision_target_schema_preflight(
                df,
                meta,
                target_connection,
                merge_key=merge_key_val,
                before_persist=before_target_dml,
                spark_target_cfg=target_cfg,
            )

            # Align columns with target table using SchemaValidator
            write_df = df
            matching_cols = []
            if str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT":
                log.info(f"  [Loader] DIRECT landing mode: aligning columns with target table {target_fqn}")
                alias_kw = " " if target_cfg.db_type == "oracle" else " AS "
                target_schema_df = (
                    _apply_jdbc_options(self.spark.read.format("jdbc"), target_cfg)
                    .option("dbtable", f"(SELECT * FROM {quote_table_name(target_fqn, target_cfg.db_type)} WHERE 1=0){alias_kw}schema_temp")
                    .load()
                )
                target_cols_map = {f.name.lower(): f.name for f in target_schema_df.schema.fields}
                for c in df.columns:
                    if c.lower() in target_cols_map:
                        target_name = target_cols_map[c.lower()]
                        if c != target_name:
                            write_df = write_df.withColumnRenamed(c, target_name)
                matching_cols = [target_cols_map[c.lower()] for c in df.columns if c.lower() in target_cols_map]
                write_df = write_df.select(*matching_cols)
            else:
                try:
                    alias_kw = " " if target_cfg.db_type == "oracle" else " AS "
                    target_schema_df = (
                        _apply_jdbc_options(self.spark.read.format("jdbc"), target_cfg)
                        .option("dbtable", f"(SELECT * FROM {quote_table_name(target_fqn, target_cfg.db_type)} WHERE 1=0){alias_kw}schema_temp")
                        .load()
                    )
                    if self._evolve_target_schema(
                        df=df,
                        meta=meta,
                        target_cfg=target_connection,
                        target_fqn=target_fqn,
                        target_schema_fields=target_schema_df.schema.fields,
                        before_persist=before_target_dml,
                    ):
                        target_schema_df = (
                            _apply_jdbc_options(self.spark.read.format("jdbc"), target_cfg)
                            .option("dbtable", f"(SELECT * FROM {quote_table_name(target_fqn, target_cfg.db_type)} WHERE 1=0){alias_kw}schema_temp")
                            .load()
                        )
                    write_df, matching_cols = SchemaValidator.validate_and_align_schema(
                        df=df,
                        target_schema_fields=target_schema_df.schema.fields,
                        source_table=meta.source_table,
                        target_fqn=target_fqn,
                        evolution_policy=getattr(meta, "schema_evolution_policy", "PERMISSIVE"),
                    )
                except Exception as e:
                    if isinstance(e, LoaderValidationError):
                        raise e
                    raise TargetPreflightError(
                        f"Could not validate existing target schema for {target_fqn}: {e}"
                    ) from e

            # TRUNCATE existing rows in target table
            target_cfg_dict = {
                "url": driver_target_cfg.url,
                "user": driver_target_cfg.user,
                "password": driver_target_cfg.password,
                "driver": driver_class,
                "schema": write_schema,
                "table": table_name,
            }

            _diag_row_count = getattr(meta, "source_count", None) or 0  # avoid redundant Spark action
            log.info(f"  [Loader Pre-Write Diagnostic] Target Table: '{target_fqn}'")
            log.info(f"  [Loader Pre-Write Diagnostic] Row Count   : {_diag_row_count}")
            log.info(f"  [Loader Pre-Write Diagnostic] Columns ({len(write_df.columns)}): {list(write_df.columns)}")
            log.info(f"  [Loader Pre-Write Diagnostic] Schema:\n{write_df.schema.simpleString()}")

            log.info(f"  [Loader] TRUNCATE {target_fqn} (full snapshot mode)")
            self._require_persistence_ownership(before_target_dml)
            safe_execute_jdbc(df=None, target_cfg=target_cfg_dict, operation="truncate", spark=self.spark)

            # Bulk INSERT all rows into target table
            log.info(f"  [Loader] INSERT {_diag_row_count} rows into {target_fqn}")
            self._require_persistence_ownership(before_target_dml)
            writer = _apply_jdbc_options(write_df.write.format("jdbc"), target_cfg)
            writer.option("dbtable", quote_table_name(target_fqn, target_cfg.db_type)) \
                .option("batchsize", "10000") \
                .mode("append") \
                .save()

            log.info(f"  [Loader] Full snapshot written to {target_fqn}")
            return f"jdbc:{target_fqn}"
        else:
            # File-based: plain overwrite with Parquet
            path = (
                f"{self.paths['target_base']}"
                f"/{meta.target_schema or 'default'}"
                f"/{meta.target_table or meta.source_table}"
            )
            self._require_persistence_ownership(before_target_dml)
            df.write.mode("overwrite").parquet(path)
            log.info(f"  [Loader] Full snapshot written (Parquet overwrite) at {path}")
            return path

    def _write_target_delta_merge(
        self,
        df: DataFrame,
        meta: MetadataRow,
        merge_key: str,
        before_target_dml: Optional[Callable[[], None]] = None,
    ) -> str:
        """Write to Gold (target) layer using Delta Lake MERGE on disk."""
        from src.exceptions import CDCPolicyNotMigratedError

        self._require_persistence_ownership(before_target_dml)
        raise CDCPolicyNotMigratedError(
            "Keyed Gold Delta MERGE is disabled during the Stage 0 safety freeze."
        )

        # Retained unreachable implementation for KEY-01/CDC-01 migration work.
        path = (
            f"{self.paths['target_base']}"
            f"/{meta.target_schema or 'default'}"
            f"/{meta.target_table or meta.source_table}"
        )

        if self._delta_available:
            try:
                from delta.tables import DeltaTable

                # Check if path exists and is delta
                is_delta = False
                try:
                    is_delta = DeltaTable.isDeltaTable(self.spark, path)
                except Exception:
                    pass

                if is_delta:
                    delta_table = DeltaTable.forPath(self.spark, path)
                    self._require_persistence_ownership(before_target_dml)
                    (
                        delta_table.alias("tgt")
                        .merge(df.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    print(f"  [Loader] Gold merged using Delta Lake at {path}")
                else:
                    self._require_persistence_ownership(before_target_dml)
                    df.write.format(FileFormat.DELTA).mode(WriteMode.OVERWRITE).save(path)
                    print(f"  [Loader] Gold initialized using Delta Lake at {path}")

                return path

            except (ImportError, ModuleNotFoundError) as e:
                log.warning(f"  [Loader] Delta Lake package not installed, falling back to Parquet overwrite: {e}")
                self._delta_available = False
                self._require_persistence_ownership(before_target_dml)
                df.write.mode(WriteMode.OVERWRITE).parquet(path)
                return path
            except Exception as e:
                err_str = str(e).lower()
                if "classnotfoundexception" in err_str or "failed to find data source: delta" in err_str:
                    log.warning(f"  [Loader] Spark Delta package unavailable, falling back to Parquet overwrite: {e}")
                    self._delta_available = False
                    self._require_persistence_ownership(before_target_dml)
                    df.write.mode(WriteMode.OVERWRITE).parquet(path)
                    return path
                log.error(f"  [Loader] Delta MERGE failed at {path}: {e}")
                raise
        else:
            self._require_persistence_ownership(before_target_dml)
            df.write.mode(WriteMode.OVERWRITE).parquet(path)
            return path
