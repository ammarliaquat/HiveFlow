# src/orchestrator/etl.py
import os
import atexit
import signal
import uuid
import logging
import json
from numbers import Integral
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Callable, Tuple, Any, Sequence

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.connections.connection_manager import ConnectionManager, ExecutionContext
from src.metadata.metadata_reader import MetadataReader
from src.metadata.metadata_reader import MetadataReader
from src.metadata.metadata_models import MetadataRow
from src.extract.extractor import Extractor
from src.transform.transformer import Transformer
from src.transform.column_mapper import ColumnMapper
from src.load.loader import Loader
from src.cdc.cdc_handler import CDCHandler
from src.logging_.batch_logger import BatchLogger, ExecutionLeaseLostError
from src.utils.db_utils import test_jdbc_connection, safe_execute_jdbc, quote_table_name, apply_jdbc_options
from src.constants import (
    RunStatus,
    LayerName,
    PipelinePhase,
    ExecutionScope,
    normalize_execution_scope,
    compute_artifact_fingerprint,
)
from src.validation.reconciler import Reconciler
from src.logging_.stage_profiler import StageProfiler
from src.validation.metadata_validator import MetadataValidator
from src.quality.dq_engine import DQEngine
from src.validation.schema_drift_detector import SchemaDriftDetector, SchemaDriftException
from src.metadata.lineage_service import LineageService
from src.load.schema_validator import SchemaValidator
from src.connections.schema_certifier import SchemaCertifier
from src.validation.layer_artifacts import (
    dataframe_schema_fingerprint,
    load_verified_artifact,
    publish_verified_artifact,
)

log = logging.getLogger("etl")


class ETL:

    def __init__(self, config_path: str = "config/config.yaml", schema_certifier: Optional[Any] = None):
        self.conn_manager = ConnectionManager(config_path)
        self.spark_cfg = self.conn_manager._raw_config["spark"]
        
        # SCHEMA-CERT-01 / SCHEMA-CERT-02: Hard-block startup if metadata DB ledger does not match local migration files
        self.schema_certifier = schema_certifier if schema_certifier is not None else SchemaCertifier(self.conn_manager)
        self.schema_certifier.certify()
        
        # Build Spark Session
        from src.spark_session import SparkSessionFactory
        self.spark = SparkSessionFactory.get_session(self.conn_manager._raw_config)
        
        try:
            self.spark_app_id = self.spark.sparkContext.applicationId
        except Exception:
            self.spark_app_id = "spark-connect-session"
            
        max_age_hours = self.conn_manager.pipeline_config.get("max_age_hours", 3)
        self.metadata_reader = MetadataReader(self.conn_manager, max_age_hours=max_age_hours)
        self.column_mapper = ColumnMapper(self.conn_manager)
        self.extractor = Extractor(self.spark, self.conn_manager)
        self.loader = Loader(self.spark, self.conn_manager)
        self.cdc_handler = CDCHandler(self.metadata_reader, self.spark)
        
        self.max_parallel_tables = self.conn_manager.pipeline_config.get("max_parallel_tables", 5)
        self.current_action = None
        self._active_logger: Optional[BatchLogger] = None  # tracks current run's logger

    def _register_cancellation_guards(self, logger: "BatchLogger") -> None:
        """
        Registers atexit + SIGTERM + SIGINT handlers that immediately flush
        CANCELLED status to batch_log when the process exits for any reason.

        All three paths call logger.mark_run_cancelled(), which is a single
        atomic UPDATE on the RUNNING rows — designed to complete in milliseconds.
        """
        self._active_logger = logger

        # ── atexit: fires on normal exit, sys.exit(), and unhandled exceptions ──
        def _atexit_handler():
            if self._active_logger:
                self._active_logger.mark_run_cancelled(
                    reason="Process exited before completion (atexit)."
                )
        atexit.register(_atexit_handler)

        # ── SIGTERM: fired by OS, container orchestrators, Task Managers ─────────
        def _sigterm_handler(signum, frame):
            if self._active_logger:
                self._active_logger.mark_run_cancelled(
                    reason="Process received SIGTERM (kill / system shutdown)."
                )
            raise SystemExit(1)

        # ── SIGINT: fired by Ctrl+C ───────────────────────────────────────────────
        def _sigint_handler(signum, frame):
            if self._active_logger:
                self._active_logger.mark_run_cancelled(
                    reason="Cancelled by operator (Ctrl+C / SIGINT)."
                )
            raise KeyboardInterrupt

        try:
            signal.signal(signal.SIGTERM, _sigterm_handler)
            signal.signal(signal.SIGINT, _sigint_handler)
        except (OSError, ValueError):
            # Signal registration may fail in non-main threads — atexit still covers it
            pass

    def _clear_cancellation_guards(self) -> None:
        """Call after a run completes normally to deactivate the atexit guard."""
        self._active_logger = None

    def _contract_from_frozen_table(self, frozen_table: Any):
        """Build the immutable table contract carried by a governed submission."""
        payload = getattr(frozen_table, "contract_payload", None) or {}
        cdc_payload = payload.get("cdc_policy")
        cdc_policy = None
        if cdc_payload:
            from src.metadata.policy_models import ResolvedCDCPolicy

            cdc_policy = ResolvedCDCPolicy.from_dict(cdc_payload)

        from src.metadata.key_contract import (
            ApprovedBusinessKey,
            KeyEvidenceType,
            ResolvedTableContract,
            ResolvedTargetBusinessKey,
        )

        source_key = payload.get("source_business_key")
        if source_key is None and "source_business_key" not in payload and frozen_table.business_key_columns:
            # Compatibility for immutable submissions created before the
            # key payload was added.  These entries already carry their
            # frozen key and are not equivalent to an explicit keyless policy.
            source_key = {
                "columns": list(frozen_table.business_key_columns),
                "evidence_type": "PRIMARY_KEY",
                "evidence_fingerprint": frozen_table.business_key_fingerprint,
            }
        source_key_columns = tuple()
        approved_source_key = None
        target_business_key = None
        if source_key:
            source_key_columns = tuple(source_key.get("columns") or frozen_table.business_key_columns)
            source_key_evidence = source_key.get("evidence_type") or "PRIMARY_KEY"
            source_key_fingerprint = (
                source_key.get("evidence_fingerprint")
                or getattr(frozen_table, "business_key_fingerprint", "")
            )
            from src.contracts.conditional_key import IdentityMode, Int64ExceptionCodec
            raw_identity_mode = source_key.get("identity_mode") or getattr(
                frozen_table, "identity_mode", IdentityMode.EXACT_KEY.value
            )
            try:
                identity_mode = (
                    raw_identity_mode
                    if isinstance(raw_identity_mode, IdentityMode)
                    else IdentityMode(str(raw_identity_mode).strip().upper())
                )
            except ValueError as exc:
                raise RuntimeError(
                    f"GOVERNED_IDENTITY_MODE_INVALID: {raw_identity_mode!r}"
                ) from exc
            raw_exception_payload = (
                source_key.get("exception_values_payload")
                or getattr(frozen_table, "conditional_exception_values_payload", None)
            )
            raw_exception_values = source_key.get("exception_values") or getattr(
                frozen_table, "conditional_exception_values", None
            )
            if isinstance(raw_exception_payload, str) and raw_exception_payload.strip():
                try:
                    raw_exception_payload = json.loads(raw_exception_payload)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "GOVERNED_EXCEPTION_PAYLOAD_INVALID: frozen source key payload is not valid JSON."
                    ) from exc
            if raw_exception_payload is not None:
                exception_values = Int64ExceptionCodec.decode(raw_exception_payload)
            else:
                exception_values = Int64ExceptionCodec.normalize(raw_exception_values or ())
            exc_fp = (
                source_key.get("exception_values_fingerprint")
                or source_key.get("exception_evidence_fingerprint")
                or getattr(frozen_table, "conditional_exception_fingerprint", "")
                or (Int64ExceptionCodec.fingerprint(exception_values) if exception_values else "")
            )
            approved_source_key = ApprovedBusinessKey(
                columns=source_key_columns,
                evidence_type=KeyEvidenceType(str(source_key_evidence).upper()),
                evidence_fingerprint=str(source_key_fingerprint),
                identity_mode=identity_mode,
                exception_values=exception_values,
                exception_values_fingerprint=str(exc_fp),
                exception_evidence_fingerprint=str(exc_fp),
            )
            target_key = payload.get("target_business_key") or {
                "columns": list(frozen_table.business_key_columns),
            }
            target_key_columns = tuple(target_key.get("columns") or frozen_table.business_key_columns)
            target_business_key = ResolvedTargetBusinessKey.build(
                table_id=frozen_table.table_id,
                source_key_columns=source_key_columns,
                target_key_columns=target_key_columns,
                source_key_evidence_fingerprint=str(source_key_fingerprint),
                mapping_fingerprint=str(frozen_table.column_mapping_fingerprint),
                identity_mode=identity_mode,
                exception_values=exception_values,
            )
        elif cdc_policy and cdc_policy.strategy.value != "FULL_SNAPSHOT":
            raise RuntimeError("GOVERNED_CDC_KEY_MISSING: CDC contract has no approved business key")

        from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings

        resolved_mappings = None
        # Contracts created before mappings were included in the frozen payload
        # remain executable under their original compatibility behavior.  New
        # contracts must carry mappings and prove that they match the frozen
        # fingerprint before the worker can use them.
        raw_mappings = payload.get("mappings")
        if raw_mappings is not None:
            mapping_policy = MappingPolicy(payload.get("mapping_policy", "EXPLICIT"))
            resolved_mappings = ResolvedTableMappings.build(
                table_id=frozen_table.table_id,
                raw_mappings=raw_mappings,
                mapping_policy=mapping_policy,
            )
            if resolved_mappings.fingerprint != str(frozen_table.column_mapping_fingerprint):
                raise RuntimeError(
                    "GOVERNED_MAPPING_FINGERPRINT_MISMATCH: frozen mapping payload does not match "
                    f"the submission fingerprint for table_id {frozen_table.table_id}."
                )

        return ResolvedTableContract(
            table_id=frozen_table.table_id,
            policy_version_id=str(frozen_table.policy_version_id),
            version_number=1,
            policy_fingerprint=str(frozen_table.policy_fingerprint),
            approved_source_business_key=approved_source_key,
            resolved_mappings=resolved_mappings,
            canonical_target_business_key=target_business_key,
            contract_payload=payload,
            contract_fingerprint=str(payload.get("contract_fingerprint") or frozen_table.policy_fingerprint),
            cdc_policy=cdc_policy,
        )

    def _metadata_row_from_frozen_table(self, frozen_table: Any) -> MetadataRow:
        """Build the MetadataRow used by real governed layer execution."""
        contract = self._contract_from_frozen_table(frozen_table)
        source_key_columns = tuple(
            getattr(getattr(contract, "approved_source_business_key", None), "columns", ())
        )
        last_success_timestamp = (
            getattr(frozen_table, "last_success_timestamp", None)
            or getattr(frozen_table, "last_success_date", None)
        )
        meta = MetadataRow(
            id=frozen_table.table_id,
            source_tag=frozen_table.source_tag,
            source_schema=getattr(frozen_table, "source_schema", None),
            source_table=frozen_table.source_table,
            source_db_type=getattr(frozen_table, "source_db_type", ""),
            landing_table=getattr(frozen_table, "landing_table", None) or frozen_table.source_table,
            history_table=getattr(frozen_table, "history_table", None) or frozen_table.source_table,
            target_table=frozen_table.target_table,
            target_schema=frozen_table.target_schema,
            is_active="y",
            priority=frozen_table.priority,
            load_type=getattr(frozen_table, "load_type", "full"),
            is_cdc=str(getattr(frozen_table, "is_cdc", "n") or "n").lower(),
            cdc_columns=getattr(frozen_table, "cdc_columns", None),
            cdc_p_days=int(getattr(frozen_table, "cdc_p_days", 0) or 0),
            delta_days=int(getattr(frozen_table, "delta_days", 0) or 0),
            last_success_timestamp=last_success_timestamp,
            is_reconcile=str(getattr(frozen_table, "is_reconcile", "n") or "n").lower(),
            reconcile_column=getattr(frozen_table, "reconcile_column", None),
            reconcile_measure=getattr(frozen_table, "reconcile_measure", "count"),
            reconcile_threshold=float(getattr(frozen_table, "reconcile_threshold", 0.0) or 0.0),
            reconcile_action=str(getattr(frozen_table, "reconcile_action", "FAIL") or "FAIL").upper(),
            chunk_size=int(getattr(frozen_table, "target_rows_per_partition", 0) or 0),
            num_partitions=0,
            partition_column=getattr(frozen_table, "cdc_columns", None),
            source_key_prefix=(
                getattr(frozen_table, "source_key_prefix", None)
                or f"{frozen_table.source_tag}_{frozen_table.source_table}_"
            ),


            natural_key_column=",".join(source_key_columns),
            target_tag=frozen_table.target_tag,
            last_success_date=getattr(frozen_table, "last_success_date", None),
            rerun_flag=str(getattr(frozen_table, "rerun_flag", "n") or "n").lower(),
            partition_lower_bound=getattr(frozen_table, "partition_lower_bound", None),
            partition_upper_bound=getattr(frozen_table, "partition_upper_bound", None),
            target_rows_per_partition=int(getattr(frozen_table, "target_rows_per_partition", 250000) or 250000),
            minimum_partition_threshold=int(getattr(frozen_table, "minimum_partition_threshold", 50000) or 50000),
            maximum_partitions=int(getattr(frozen_table, "maximum_partitions", 32) or 32),
            key_candidate_nullable=not bool(source_key_columns),
            key_candidate_compatible=bool(source_key_columns),
            current_key_evidence_fingerprint=(
                frozen_table.business_key_fingerprint if source_key_columns else None
            ),
            key_approval_state="APPROVED" if source_key_columns else "UNAPPROVED",
            approved_source_business_key=list(source_key_columns) if source_key_columns else None,
            approved_key_evidence="FROZEN_CONTRACT" if source_key_columns else None,
            approved_key_evidence_fingerprint=(
                frozen_table.business_key_fingerprint if source_key_columns else None
            ),
            key_approved_by="governed_submission" if source_key_columns else None,
            key_approved_at=datetime.now() if source_key_columns else None,
            key_approval_revision=1 if source_key_columns else None,
            cdc_strategy=getattr(frozen_table, "cdc_strategy", None),
        )
        meta.table_contract = contract
        meta.resolved_mappings = contract.resolved_mappings
        approved_key = getattr(contract, "approved_source_business_key", None)
        # Keep conditional identity available to the layer adapters without
        # expanding the legacy MetadataRow schema.  The values originate in
        # the immutable frozen contract and are never re-profiled at runtime.
        meta.identity_mode = getattr(approved_key, "identity_mode", None)
        meta.exception_values = tuple(getattr(approved_key, "exception_values", ()) or ())
        cdc_policy = getattr(contract, "cdc_policy", None)
        meta.cdc_activation_state = str(
            getattr(cdc_policy, "activation_state", "") or ""
        ).upper() or None
        meta.frozen_capability_evidence = getattr(
            cdc_policy, "capability_evidence", None
        )
        # This is deliberately a dynamic execution-only attribute.  The
        # MetadataRow model remains compatible with legacy callers while the
        # governed worker carries the exact upstream artifact reference.
        meta.upstream_artifact = getattr(frozen_table, "upstream_artifact", None)
        landing_mode = getattr(frozen_table, "landing_mode", None)
        if not landing_mode or landing_mode == "METADATA":
            try:
                with self.conn_manager.get_metadata_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COALESCE(landing_mode, 'METADATA') FROM metadata.db_tables_etl WHERE table_id = %s",
                            (frozen_table.table_id,)
                        )
                        r = cur.fetchone()
                        if r and r[0]:
                            landing_mode = r[0]
            except Exception:
                pass
        meta.landing_mode = str(landing_mode or "METADATA").upper()
        return meta

    def _activate_bounded_exception_after_bootstrap(
        self,
        meta: MetadataRow,
        *,
        gold_metrics: Optional[dict],
        logger: Optional[BatchLogger],
        log_id: Optional[int],
    ) -> str:
        """Activate sequence CDC after a successful bounded bootstrap.

        The first bounded-exception run is deliberately a governed full
        snapshot.  Once Gold has persisted and verified that snapshot, the
        existing policy-governance service promotes the same frozen identity
        to executable sequence CDC.  A control-plane activation failure never
        invalidates already-published data; it remains visible as a warning and
        the next governed run can retry activation.
        """
        identity = getattr(meta, "identity_mode", None)
        identity_name = str(getattr(identity, "value", identity) or "").upper()
        activation_state = str(
            getattr(meta, "cdc_activation_state", "") or ""
        ).upper()
        if identity_name != "BOUNDED_EXCEPTION_KEY" or activation_state != "BOOTSTRAP_REQUIRED":
            return ""

        evidence = (gold_metrics or {}).get("target_identity_capability") or {}
        try:
            from src.metadata.policy_governance import PolicyGovernanceService

            result = PolicyGovernanceService(
                self.conn_manager
            ).activate_bounded_exception_cdc(
                meta.id,
                capability_evidence=evidence,
            )
            if logger and log_id:
                logger.update_stage_metrics(
                    log_id,
                    {
                        "cdc_activation": {
                            "status": "ACTIVE",
                            "identity_mode": identity_name,
                            "policy_version_id": getattr(result, "policy_version_id", None),
                        }
                    },
                )
            log.info(
                "[%s] Bounded bootstrap completed; sequence CDC activated.",
                meta.source_table,
            )
            return "CDC activated after verified bootstrap"
        except Exception as exc:
            if logger and log_id:
                logger.update_stage_metrics(
                    log_id,
                    {
                        "cdc_activation": {
                            "status": "PENDING",
                            "identity_mode": identity_name,
                            "error": str(exc),
                        }
                    },
                )
            log.warning(
                "[%s] Gold bootstrap succeeded but sequence CDC activation is pending: %s",
                meta.source_table,
                exc,
            )
            return "CDC activation pending (bootstrap data remains valid)"

    def _publish_layer_artifact(
        self,
        meta: MetadataRow,
        *,
        run_id: str,
        log_id: Optional[int],
        storage_layer: str,
        storage_uri: str,
        row_count: int,
        storage_format: str = "parquet",
        dataframe: Optional[DataFrame] = None,
    ) -> Any:
        """Publish a verified run-scoped layer artifact for governed workers."""
        contract = getattr(meta, "table_contract", None)
        if not contract:
            raise RuntimeError("GOVERNED_ARTIFACT_POLICY_MISSING: frozen policy contract is absent")
        source_key = getattr(contract, "approved_source_business_key", None)
        target_key = getattr(contract, "canonical_target_business_key", None)
        source_key_fp = getattr(source_key, "evidence_fingerprint", None) or meta.current_key_evidence_fingerprint or ""
        mapping_fp = getattr(target_key, "mapping_fingerprint", None) or getattr(contract, "contract_fingerprint", "")
        if not mapping_fp:
            raise RuntimeError("GOVERNED_ARTIFACT_POLICY_MISSING: mapping fingerprint is absent")
        schema_fp = dataframe_schema_fingerprint(dataframe) if dataframe is not None else getattr(contract, "contract_fingerprint", "")
        window = getattr(meta, "resolved_cdc_window", None)
        ref = publish_verified_artifact(
            self.conn_manager,
            run_id=run_id,
            table_id=meta.id,
            attempt_number=1,
            source_tag=meta.source_tag,
            target_tag=meta.target_tag or "",
            source_table=meta.source_table,
            target_table=meta.target_table or meta.source_table,
            storage_layer=storage_layer,
            storage_uri=storage_uri,
            storage_format=storage_format,
            row_count=row_count,
            policy_version_id=contract.policy_version_id,
            policy_fingerprint=contract.policy_fingerprint,
            mapping_fingerprint=str(mapping_fp),
            source_key_fingerprint=str(source_key_fp),
            schema_fingerprint=str(schema_fp),
            cdc_window_id=str(window.window_id) if window else None,
            cdc_lower_cursor=getattr(window, "lower_cursor", None) if window else None,
            cdc_upper_cursor=getattr(window, "upper_cursor", None) if window else None,
            cdc_state_version=getattr(meta, "cdc_state_version", None),
            batch_log_id=log_id,
        )
        return ref

    def _read_verified_artifact(self, meta: MetadataRow, artifact: Any) -> DataFrame:
        """Read and verify the exact artifact bound by a frozen contract."""
        if artifact is None:
            raise RuntimeError(
                f"GOVERNED_ARTIFACT_REQUIRED: no upstream artifact is bound for {meta.source_table}"
            )
        persisted = load_verified_artifact(
            self.conn_manager,
            artifact_id=getattr(artifact, "artifact_id", None),
        )
        immutable_fields = (
            "producer_run_id", "storage_uri", "storage_format", "row_count",
            "policy_version_id", "policy_fingerprint", "mapping_fingerprint",
            "source_key_fingerprint", "schema_fingerprint", "artifact_fingerprint",
        )
        for field_name in immutable_fields:
            if str(getattr(persisted, field_name)) != str(getattr(artifact, field_name)):
                raise RuntimeError(f"GOVERNED_ARTIFACT_MISMATCH: {field_name} differs from frozen contract")
        df = self.extractor.read_artifact(persisted)
        actual_count = Reconciler.count_and_cache(df)
        if int(actual_count) != int(artifact.row_count):
            raise RuntimeError(
                f"GOVERNED_ARTIFACT_ROW_COUNT_MISMATCH: expected {artifact.row_count}, got {actual_count}"
            )
        actual_schema_fp = dataframe_schema_fingerprint(df)
        if str(actual_schema_fp) != str(artifact.schema_fingerprint):
            raise RuntimeError(
                "GOVERNED_ARTIFACT_SCHEMA_MISMATCH: persisted schema differs from "
                f"the certified artifact; table={meta.source_table}, "
                f"expected_fingerprint={artifact.schema_fingerprint}, "
                f"actual_fingerprint={actual_schema_fp}"
            )
        expected_fp = compute_artifact_fingerprint(
            storage_layer=artifact.storage_layer,
            storage_uri=artifact.storage_uri,
            storage_format=artifact.storage_format,
            row_count=artifact.row_count,
            schema_fingerprint=artifact.schema_fingerprint,
            policy_fingerprint=artifact.policy_fingerprint,
            mapping_fingerprint=artifact.mapping_fingerprint,
            source_key_fingerprint=artifact.source_key_fingerprint,
            cdc_window_id=artifact.cdc_window_id,
        )
        if expected_fp != artifact.artifact_fingerprint:
            raise RuntimeError("GOVERNED_ARTIFACT_FINGERPRINT_MISMATCH: artifact identity is invalid")
        return df

    @staticmethod
    def _cursor_payload(value: Any) -> Any:
        """Normalize a JSON/DB cursor value for deterministic comparison."""
        if value is None or isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    def _restore_persisted_cdc_context(self, meta: MetadataRow, artifact: Any) -> None:
        """Restore and validate the producer CDC window before Gold target DML."""
        if str(getattr(meta, "is_cdc", "n")).lower() != "y":
            return
        if str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT":
            return
        window_id = getattr(artifact, "cdc_window_id", None)
        if not window_id:
            raise RuntimeError("GOVERNED_CDC_WINDOW_MISSING: Silver artifact has no CDC window identity")

        from src.cdc.cdc_window_resolver import CDCWindowResolver

        resolver = CDCWindowResolver(self.conn_manager)
        window = resolver._get_persisted_attempt_window(
            str(artifact.producer_run_id), meta.id, int(artifact.producer_attempt_number)
        )
        if not window or str(window.window_id) != str(window_id):
            raise RuntimeError("GOVERNED_CDC_WINDOW_MISMATCH: artifact window is not persisted")
        meta.resolved_cdc_window = window
        meta.cdc_state_version = getattr(artifact, "cdc_state_version", None)

        with self.conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT committed_cursor, state_version
                    FROM metadata.table_cdc_state
                    WHERE table_id = %s AND policy_version_id = %s
                    """,
                    [meta.id, getattr(meta.table_contract, "policy_version_id", None)],
                )
                state_row = cur.fetchone()
        observed_cursor = self._cursor_payload(state_row[0]) if state_row else None
        expected_cursor = self._cursor_payload(getattr(artifact, "cdc_lower_cursor", None))
        observed_version = int(state_row[1]) if state_row and state_row[1] is not None else None
        expected_version = getattr(artifact, "cdc_state_version", None)
        if observed_cursor != expected_cursor or observed_version != expected_version:
            raise RuntimeError(
                "GOVERNED_CDC_STATE_MISMATCH: committed cursor/state version changed "
                "after the upstream artifact was produced"
            )

    def _reconcile_persisted_cdc_target(
        self,
        meta: MetadataRow,
        *,
        run_id: str,
        dedup_count: int,
        target_cfg: Any,
        gold_metrics: Optional[dict],
    ) -> None:
        """Persist target convergence evidence for a Gold artifact consumer."""
        window = getattr(meta, "resolved_cdc_window", None)
        contract = getattr(meta, "table_contract", None)
        target_evidence = (gold_metrics or {}).get("target_reconciliation")
        if not window or not contract or not getattr(contract, "cdc_policy", None):
            raise RuntimeError("GOVERNED_CDC_RECONCILIATION_CONTEXT_MISSING")
        if not target_evidence:
            raise RuntimeError("TARGET_RECONCILIATION_MISSING: UPSERT returned no physical target evidence.")
        target_schema = getattr(target_cfg, "schema", None) or meta.target_schema
        if not target_schema:
            raise RuntimeError(f"TARGET_SCHEMA_REQUIRED: {meta.target_tag} has no configured target schema.")
        target_table = meta.target_table or meta.source_table
        evidence_fingerprint = compute_artifact_fingerprint(
            storage_layer="GOLD",
            storage_uri=f"{target_schema}.{target_table}",
            storage_format="jdbc",
            row_count=dedup_count,
            schema_fingerprint=getattr(contract, "contract_fingerprint", ""),
            policy_fingerprint=contract.policy_fingerprint,
            mapping_fingerprint=getattr(contract.canonical_target_business_key, "mapping_fingerprint", ""),
            source_key_fingerprint=getattr(contract.approved_source_business_key, "evidence_fingerprint", ""),
            cdc_window_id=str(window.window_id),
        )
        from src.validation.strategy_reconciler import StrategyAwareReconciler

        reconciler = StrategyAwareReconciler(self.conn_manager)
        manifest = reconciler.freeze_persistence_manifest(
            run_id=run_id,
            table_id=meta.id,
            attempt_number=1,
            contract=contract,
            window=window,
            storage_layer="target",
            storage_object_identity=f"{target_schema}.{target_table}",
            expected_keys=None,
            schema_fingerprint=getattr(contract, "contract_fingerprint", ""),
            expected_row_count=dedup_count,
            expected_key_fingerprint=evidence_fingerprint,
        )
        result = reconciler.reconcile_staged_target_convergence(
            run_id=run_id,
            table_id=meta.id,
            attempt_number=1,
            contract=contract,
            window=window,
            manifest=manifest,
            target_evidence=target_evidence,
        )
        meta.cdc_reconciliation_id = result.reconciliation_id

    def _build_governed_cdc_filter(self, meta: MetadataRow, run_id: str) -> Optional[str]:
        """Resolve a governed CDC-01 bounded extraction filter from frozen policy."""
        contract = getattr(meta, "table_contract", None)
        if not contract or not getattr(contract, "cdc_policy", None):
            from src.exceptions import CDCPolicyNotPromotedError

            raise CDCPolicyNotPromotedError(
                f"CDC table {meta.source_tag}.{meta.source_table} has no frozen CDC policy. "
                "The governed worker will not re-read mutable metadata."
            )

        from src.cdc.cdc_window_resolver import CDCWindowResolver
        from src.metadata.policy_models import CDCStrategy

        resolver = CDCWindowResolver(self.conn_manager, spark=self.spark)
        window = resolver.resolve_and_persist_attempt_window(
            run_id=run_id,
            table_id=meta.id,
            attempt_number=1,
            contract=contract,
            source_tag=meta.source_tag,
            source_schema=meta.source_schema,
            source_table=meta.source_table,
            overlap_days=meta.cdc_p_days,
        )
        meta.resolved_cdc_window = window
        # Preserve the lower-bound state version with the artifact.  Gold uses
        # it for a compare-and-swap commit after target publication.
        meta.cdc_state_version = resolver.load_committed_state_version(
            meta.id, contract.policy_version_id
        )
        if window.strategy == CDCStrategy.TIMESTAMP_HIGH_WATERMARK:
            meta.partition_column = None
            meta.partition_lower_bound = None
            meta.partition_upper_bound = None
        else:
            meta.partition_column = window.upper_cursor.columns[0]
            meta.partition_lower_bound = (
                window.lower_cursor.values[0]
                if window.lower_cursor is not None
                else 0
            )
            meta.partition_upper_bound = window.upper_cursor.values[0]
        source_cfg = self.conn_manager.get(meta.source_tag, expected_role="source")
        exception_values = tuple(
            getattr(
                getattr(contract, "approved_source_business_key", None),
                "exception_values",
                (),
            )
            or getattr(meta, "exception_values", ())
            or ()
        )
        return resolver.build_extraction_filter_sql(
            window,
            dialect=getattr(source_cfg, "db_type", "mysql"),
            overlap_days=meta.cdc_p_days,
            exception_values=exception_values,
        )

    def _validate_raw_cdc_key_window(
        self,
        meta: MetadataRow,
        df: DataFrame,
        *,
        log_id: Optional[int],
        logger: Optional[BatchLogger],
    ) -> None:
        """Reject a malformed CDC window before deduplication or target DML."""
        contract = getattr(meta, "table_contract", None)
        approved_key = getattr(contract, "approved_source_business_key", None)
        key_columns = tuple(getattr(approved_key, "columns", ()) or ())
        identity_mode = getattr(approved_key, "identity_mode", None)
        identity_name = str(getattr(identity_mode, "value", identity_mode) or "EXACT_KEY").upper()
        exception_values = tuple(getattr(approved_key, "exception_values", ()) or ())
        policy_version_id = getattr(contract, "policy_version_id", None)
        if not key_columns or not policy_version_id:
            from src.exceptions import CDCPolicyNotPromotedError

            raise CDCPolicyNotPromotedError(
                f"CDC table {meta.source_tag}.{meta.source_table} has no frozen approved business key."
            )

        null_condition = None
        for column in key_columns:
            condition = F.col(column).isNull()
            null_condition = condition if null_condition is None else null_condition | condition
        null_key_count = df.filter(null_condition).count()

        duplicate_key_count = 0
        if not null_key_count:
            duplicate_groups = df.groupBy(*key_columns).count().filter(F.col("count") > 1)
            if identity_name == "BOUNDED_EXCEPTION_KEY":
                # Conditional identity is a two-lane contract.  Duplicates
                # are permitted only for the frozen, typed exception values;
                # every other key must remain unique in the bounded window.
                if len(key_columns) != 1 or not exception_values:
                    raise RuntimeError(
                        "GOVERNED_EXCEPTION_CONTRACT_INVALID: conditional identity "
                        "has no single key column and exception set."
                    )
                duplicate_key_count = duplicate_groups.filter(
                    ~F.col(key_columns[0]).isin(list(exception_values))
                ).count()
            else:
                duplicate_key_count = duplicate_groups.count()

        if not null_key_count and not duplicate_key_count:
            return

        reason = (
            f"CDC raw window failed approved business-key integrity: "
            f"null_keys={null_key_count}, duplicate_keys={duplicate_key_count}."
        )
        if logger and log_id:
            logger.update_stage_metrics(log_id, {
                "cdc": {
                    "reconciliation_outcome": "REJECTED",
                    "failure_code": (
                        "RECONCILIATION_NULL_KEY_DETECTED"
                        if null_key_count else "RECONCILIATION_DUPLICATE_KEY_DETECTED"
                    ),
                    "reason": reason,
                    "null_key_count": int(null_key_count),
                    "duplicate_key_count": int(duplicate_key_count),
                    "next_execution_mode": "REPROFILE_REQUIRED",
                }
            })

        from src.exceptions import ReconciliationDuplicateKeyError, ReconciliationNullKeyError

        if null_key_count:
            raise ReconciliationNullKeyError(reason)
        raise ReconciliationDuplicateKeyError(reason)

    @staticmethod
    def _conditional_identity_parts(meta: MetadataRow) -> tuple:
        """Return the frozen conditional key column and exception values.

        Conditional identity is carried by the approved source-key contract;
        no mutable catalog lookup is performed while a run is executing.
        """
        contract = getattr(meta, "table_contract", None)
        approved_key = getattr(contract, "approved_source_business_key", None)
        mode = getattr(getattr(approved_key, "identity_mode", None), "value", None)
        mode = str(mode or getattr(meta, "identity_mode", "EXACT_KEY")).upper()
        values = tuple(
            getattr(approved_key, "exception_values", ())
            or getattr(meta, "exception_values", ())
            or ()
        )
        columns = tuple(getattr(approved_key, "columns", ()) or ())
        if mode != "BOUNDED_EXCEPTION_KEY":
            return None, ()
        if len(columns) != 1 or not values:
            raise RuntimeError(
                "GOVERNED_EXCEPTION_CONTRACT_INVALID: conditional identity requires "
                "one approved key column and at least one exception value."
            )
        return columns[0], values

    def _commit_governed_cdc_window(
        self,
        meta: MetadataRow,
        run_id: str,
        log_id: Optional[int],
        expected_state_version: Optional[int] = None,
    ) -> None:
        """Advance governed CDC state only through the authoritative OBS-01 committer."""
        window = getattr(meta, "resolved_cdc_window", None)
        contract = getattr(meta, "table_contract", None)
        if not window or not contract or not getattr(contract, "cdc_policy", None):
            return

        reconciliation_id = getattr(meta, "cdc_reconciliation_id", None)
        if not log_id or not reconciliation_id:
            from src.exceptions import CDCPolicyNotMigratedError

            raise CDCPolicyNotMigratedError(
                f"Governed CDC for {meta.source_tag}.{meta.source_table} requires "
                "commit-authoritative reconciliation before cursor advancement."
            )

        from src.cdc.atomic_committer import AtomicCDCCommitter

        AtomicCDCCommitter(self.conn_manager).commit_attempt_watermark_and_success(
            batch_log_id=log_id,
            reconciliation_id=reconciliation_id,
            run_id=run_id,
            expected_state_version=expected_state_version,
        )

    def _parent_layer_statuses(self, parent_run_id: str, meta: MetadataRow) -> dict:
        """Return the latest terminal status recorded per layer for one parent table."""
        with self.conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT load_type, status
                    FROM metadata.batch_log
                    WHERE run_id::text = %s
                      AND source_tag = %s
                      AND target_tag = %s
                      AND source_table = %s
                      AND target_table = %s
                    ORDER BY id ASC
                """, [
                    str(parent_run_id),
                    meta.source_tag,
                    meta.target_tag,
                    meta.source_table,
                    meta.target_table or meta.source_table,
                ])
                rows = cur.fetchall()
        statuses = {}
        for row in rows:
            load_type = row["load_type"] if isinstance(row, dict) else row[0]
            status = row["status"] if isinstance(row, dict) else row[1]
            if load_type and status:
                statuses[str(load_type).lower()] = str(status).upper()
        return statuses

    def _has_matching_parent_cdc_window(self, meta: MetadataRow, parent_run_id: str) -> bool:
        """Require the parent CDC window to match this table's frozen policy exactly."""
        contract = getattr(meta, "table_contract", None)
        policy = getattr(contract, "cdc_policy", None)
        if not contract or not policy:
            return False

        from src.cdc.cdc_window_resolver import CDCWindowResolver

        window = CDCWindowResolver(self.conn_manager)._get_persisted_attempt_window(
            str(parent_run_id), meta.id, 1
        )
        return bool(
            window
            and str(window.policy_version_id) == str(contract.policy_version_id)
            and str(window.cdc_policy_fingerprint) == str(policy.cdc_policy_fingerprint)
        )

    def _governed_retry_layer(self, meta: MetadataRow, parent_run_id: Optional[str]) -> str:
        """Choose the first layer that must be re-executed for a failed table.

        A retry is table-scoped.  A successful parent Bronze artifact is a
        valid input for a failed Silver layer, and a successful parent Silver
        artifact is a valid input for a failed Gold layer.  The method never
        skips a failed upstream layer and never treats a missing/NOT_REQUESTED
        layer as a reusable artifact.
        """
        if not parent_run_id:
            return LayerName.BRONZE

        statuses = self._parent_layer_statuses(str(parent_run_id), meta)
        completed = {RunStatus.SUCCESS, RunStatus.RECONCILIATION_WARNING}
        bronze_status = statuses.get(LayerName.BRONZE)
        silver_status = statuses.get(LayerName.SILVER)
        gold_status = statuses.get(LayerName.GOLD)

        if bronze_status not in completed:
            return LayerName.BRONZE

        # A failed Silver may consume the parent's successful Bronze artifact.
        # NOT_REQUESTED or absent Silver means the parent was not a complete
        # producer for this stage, so rebuild from Bronze instead.
        if silver_status not in completed:
            if silver_status in {
                RunStatus.FAILED,
                RunStatus.RECONCILIATION_FAILED,
                "CANCELLED",
                "DEPENDENCY_FAILED",
            }:
                return LayerName.SILVER
            return LayerName.BRONZE

        # A successful Gold table needs no retry.  If it is selected anyway,
        # rebuild from source rather than silently claiming it is complete.
        if gold_status in completed:
            return LayerName.BRONZE

        if str(meta.is_cdc).lower() == "y" and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
            return (
                LayerName.GOLD
                if self._has_matching_parent_cdc_window(meta, str(parent_run_id))
                else LayerName.BRONZE
            )

        if self._has_current_parent_silver_artifact(meta, str(parent_run_id)):
            return LayerName.GOLD
        return LayerName.BRONZE

    def _has_current_parent_silver_artifact(self, meta: MetadataRow, parent_run_id: str) -> bool:
        """Guard static full-snapshot Silver reuse against a later overwrite."""
        with self.conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT run_id::text
                    FROM metadata.batch_log
                    WHERE LOWER(source_tag) = LOWER(%s)
                      AND LOWER(target_tag) = LOWER(%s)
                      AND LOWER(source_table) = LOWER(%s)
                      AND LOWER(target_table) = LOWER(%s)
                      AND LOWER(load_type) = %s
                      AND status IN (%s, %s)
                    ORDER BY id DESC
                    LIMIT 1
                """, [
                    meta.source_tag,
                    meta.target_tag,
                    meta.source_table,
                    meta.target_table or meta.source_table,
                    LayerName.SILVER,
                    RunStatus.SUCCESS,
                    RunStatus.RECONCILIATION_WARNING,
                ])
                row = cur.fetchone()
        if not row:
            return False
        artifact_run_id = row["run_id"] if isinstance(row, dict) else row[0]
        return str(artifact_run_id) == str(parent_run_id)

    # ── Isolated-layer prereq guards ──────────────────────────────────────────

    def _assert_bronze_artifact_exists(self, meta: MetadataRow) -> None:
        """
        Silver-only prereq guard.
        Confirm that a Bronze Parquet artefact exists in the batch_log for this
        table (most recent row with load_type='bronze' and a terminal SUCCESS
        or RECONCILIATION_WARNING status).
        Raises RuntimeError with SILVER_LAYER_PREREQ_MISSING if absent.
        """
        with self.conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM metadata.batch_log
                    WHERE LOWER(source_tag)  = LOWER(%s)
                      AND LOWER(target_tag)  = LOWER(%s)
                      AND LOWER(source_table) = LOWER(%s)
                      AND LOWER(load_type)    = %s
                      AND status IN (%s, %s)
                    ORDER BY id DESC
                    LIMIT 1
                """, [
                    meta.source_tag,
                    meta.target_tag,
                    meta.source_table,
                    LayerName.BRONZE,
                    RunStatus.SUCCESS,
                    RunStatus.RECONCILIATION_WARNING,
                ])
                row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"SILVER_LAYER_PREREQ_MISSING: No verified Bronze artifact found for "
                f"{meta.source_tag}.{meta.source_table}. "
                "Run Bronze (or Full Pipeline) before requesting Silver-only execution."
            )

    def _resolve_latest_verified_silver_run_id(self, meta: MetadataRow) -> str:
        """
        Gold-only prereq guard.
        Returns the run_id of the most recent batch_log row with
        load_type='silver' and a terminal SUCCESS or RECONCILIATION_WARNING
        status for this table.
        Raises RuntimeError with GOLD_LAYER_PREREQ_MISSING if absent.
        Matching uses table identity and artifact status — not just date.
        """
        with self.conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT run_id::text
                    FROM metadata.batch_log
                    WHERE LOWER(source_tag)  = LOWER(%s)
                      AND LOWER(target_tag)  = LOWER(%s)
                      AND LOWER(source_table) = LOWER(%s)
                      AND LOWER(target_table) = LOWER(%s)
                      AND LOWER(load_type)    = %s
                      AND status IN (%s, %s)
                    ORDER BY id DESC
                    LIMIT 1
                """, [
                    meta.source_tag,
                    meta.target_tag,
                    meta.source_table,
                    meta.target_table or meta.source_table,
                    LayerName.SILVER,
                    RunStatus.SUCCESS,
                    RunStatus.RECONCILIATION_WARNING,
                ])
                row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"GOLD_LAYER_PREREQ_MISSING: No verified Silver artifact found for "
                f"{meta.source_tag}.{meta.source_table}. "
                "Run Silver (or Full Pipeline) before requesting Gold-only execution."
            )
        return str(row["run_id"] if isinstance(row, dict) else row[0])

    def _log_not_requested(self, meta: MetadataRow, layer_name: str, run_id: str, logger: BatchLogger) -> None:
        """Write a NOT_REQUESTED batch_log entry for a layer skipped by execution_scope."""
        now = datetime.now()
        log_id = logger.log_start(
            source_tag=meta.source_tag,
            source_table=meta.source_table,
            target_table=meta.target_table or meta.source_table,
            load_type=layer_name,
            priority=meta.priority,
            target_tag=meta.target_tag,
            policy_version_id=getattr(getattr(meta, "table_contract", None), "policy_version_id", None),
            policy_fingerprint=getattr(getattr(meta, "table_contract", None), "policy_fingerprint", None),
        )
        logger.log(
            source_tag=meta.source_tag,
            source_table=meta.source_table,
            target_table=meta.target_table or meta.source_table,
            load_type=layer_name,
            priority=meta.priority,
            source_count=None,
            target_count=None,
            start_time=now,
            end_time=now,
            status=RunStatus.NOT_REQUESTED,
            error_message=None,
            target_tag=meta.target_tag,
            log_id=log_id,
        )

    def run_frozen_table(
        self,
        frozen_table: Any,
        run_id: str,
        resume_from_run_id: Optional[str] = None,
        execution_scope: Optional[str] = None,
    ) -> List[MetadataRow]:
        """
        Execute one server-frozen governed table through the real pipeline layers,
        respecting the immutable execution_scope frozen in the contract.

        execution_scope values and their behavior:
          FULL   (default) — Source → Bronze → Silver → Gold (unchanged)
          BRONZE           — Source → Bronze only; Silver/Gold logged as NOT_REQUESTED
          SILVER           — Prereq-guarded; Bronze artifact required → Silver only
          GOLD             — Prereq-guarded; Silver artifact required → Gold only

        Layer workers, fencing, claims, and retries are unchanged in all modes.
        """
        if frozen_table.authority_mode != "GOVERNED":
            raise RuntimeError(
                "GOVERNED_EXECUTION_INVALID: frozen table authority_mode must be GOVERNED"
            )

        # The scope is a job-level contract field.  The worker receives it
        # explicitly from action_run_governed; the fallback keeps older callers
        # on the unchanged FULL path while rejecting unknown values.
        try:
            execution_scope = normalize_execution_scope(execution_scope)
        except ValueError as exc:
            raise RuntimeError(f"GOVERNED_EXECUTION_INVALID: {exc}") from exc

        meta = self._metadata_row_from_frozen_table(frozen_table)
        self.current_action = "run_governed"
        self.last_run_id = run_id
        date_partition = datetime.now().strftime("%Y-%m-%d")

        # ── FULL scope: original behavior, resume-retry logic untouched ──────
        if execution_scope == ExecutionScope.FULL:
            resume_layer = self._governed_retry_layer(meta, resume_from_run_id)
            if (
                resume_layer in {LayerName.SILVER, LayerName.GOLD}
                and getattr(meta, "upstream_artifact", None) is None
            ):
                # Old parent runs can predate the immutable artifact ledger.
                # Do not read a mutable landing/history location in that case;
                # rerun from source so the retry remains deterministic.
                log.warning(
                    "Governed retry has no verified parent artifact for %s; "
                    "rerunning from Bronze instead of consuming mutable storage.",
                    meta.source_table,
                )
                resume_layer = LayerName.BRONZE

            if resume_layer == LayerName.BRONZE:
                bronze_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
                self.last_logger = bronze_logger
                self._register_cancellation_guards(bronze_logger)
                try:
                    bronze_tables = self._execute_table_batch(
                        [meta],
                        LayerName.BRONZE,
                        meta.source_tag,
                        bronze_logger,
                        self._process_bronze_table,
                    )
                finally:
                    self._clear_cancellation_guards()
                if meta.id not in {table.id for table in bronze_tables}:
                    raise RuntimeError(f"GOVERNED_BRONZE_INCOMPLETE: {meta.source_table}")

                silver_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
                self.last_logger = silver_logger
                self._register_cancellation_guards(silver_logger)
                try:
                    silver_tables = self._execute_table_batch(
                        [meta],
                        LayerName.SILVER,
                        meta.source_tag,
                        silver_logger,
                        lambda table_meta, log_id=None, logger=None: self._process_silver_table(
                            table_meta, run_id, log_id, logger, date_partition,
                            bronze_artifact=getattr(table_meta, "_bronze_artifact", None)
                        ),
                    )
                finally:
                    self._clear_cancellation_guards()
                if meta.id not in {table.id for table in silver_tables}:
                    raise RuntimeError(f"GOVERNED_SILVER_INCOMPLETE: {meta.source_table}")
            elif resume_layer == LayerName.SILVER:
                log.info(
                    "Governed retry skips Bronze for %s from parent run %s; "
                    "Silver will consume the verified parent Bronze artifact.",
                    meta.source_table,
                    resume_from_run_id,
                )
                silver_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
                self.last_logger = silver_logger
                self._register_cancellation_guards(silver_logger)
                try:
                    silver_tables = self._execute_table_batch(
                        [meta],
                        LayerName.SILVER,
                        meta.source_tag,
                        silver_logger,
                        lambda table_meta, log_id=None, logger=None: self._process_silver_table(
                            table_meta,
                            run_id,
                            log_id,
                            logger,
                            date_partition,
                            bronze_artifact=getattr(table_meta, "upstream_artifact", None),
                        ),
                    )
                finally:
                    self._clear_cancellation_guards()
                if meta.id not in {table.id for table in silver_tables}:
                    raise RuntimeError(f"GOVERNED_SILVER_INCOMPLETE: {meta.source_table}")
            else:
                log.info(
                    "Governed retry skips Bronze and Silver for %s from parent run %s; "
                    "Gold will consume the verified parent Silver artifact.",
                    meta.source_table,
                    resume_from_run_id,
                )

            gold_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
            self.last_logger = gold_logger
            self._register_cancellation_guards(gold_logger)
            try:
                gold_tables = self._execute_table_batch(
                    [meta],
                    LayerName.GOLD,
                    meta.source_tag,
                    gold_logger,
                    lambda table_meta, log_id=None, logger=None: self._process_gold_table(
                        table_meta,
                        run_id,
                        log_id,
                        logger,
                        cdc_window_run_id=(
                            resume_from_run_id
                            if resume_layer == LayerName.GOLD and str(table_meta.is_cdc).lower() == "y" and str(getattr(table_meta, "landing_mode", "METADATA")).upper() != "DIRECT"
                            else None
                        ),
                        silver_artifact_run_id=(
                            resume_from_run_id
                            if resume_layer == LayerName.GOLD
                            else (run_id if str(table_meta.is_cdc).lower() == "y" and str(getattr(table_meta, "landing_mode", "METADATA")).upper() != "DIRECT" else None)
                        ),
                        use_persisted_silver=True,
                        silver_artifact=(
                            getattr(table_meta, "_silver_artifact", None)
                            or getattr(table_meta, "upstream_artifact", None)
                        ),
                    ),
                )
            finally:
                self._clear_cancellation_guards()
            if meta.id not in {table.id for table in gold_tables}:
                raise RuntimeError(f"GOVERNED_GOLD_INCOMPLETE: {meta.source_table}")
            return gold_tables

        # ── BRONZE scope: Source → Bronze only ───────────────────────────────
        if execution_scope == ExecutionScope.BRONZE:
            log.info(
                "[execution_scope=BRONZE] Running Bronze only for %s (Silver and Gold: NOT_REQUESTED).",
                meta.source_table,
            )
            bronze_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
            self.last_logger = bronze_logger
            self._register_cancellation_guards(bronze_logger)
            try:
                bronze_tables = self._execute_table_batch(
                    [meta],
                    LayerName.BRONZE,
                    meta.source_tag,
                    bronze_logger,
                    self._process_bronze_table,
                )
            finally:
                self._clear_cancellation_guards()
            if meta.id not in {table.id for table in bronze_tables}:
                raise RuntimeError(f"GOVERNED_BRONZE_INCOMPLETE: {meta.source_table}")

            # Emit NOT_REQUESTED for skipped layers so the GUI shows truthful state
            nr_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
            self._log_not_requested(meta, LayerName.SILVER, run_id, nr_logger)
            self._log_not_requested(meta, LayerName.GOLD, run_id, nr_logger)
            return bronze_tables

        # ── SILVER scope: prereq Bronze artifact → Silver only ───────────────
        if execution_scope == ExecutionScope.SILVER:
            log.info(
                "[execution_scope=SILVER] Running Silver only for %s; "
                "checking for verified Bronze artifact.",
                meta.source_table,
            )
            nr_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
            self._log_not_requested(meta, LayerName.BRONZE, run_id, nr_logger)
            # Record the requested scope before the guard so a failed
            # prerequisite is attributed to Silver, not an absent Bronze row.
            if getattr(meta, "upstream_artifact", None) is None:
                self._assert_bronze_artifact_exists(meta)

            silver_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
            self.last_logger = silver_logger
            self._register_cancellation_guards(silver_logger)
            try:
                silver_tables = self._execute_table_batch(
                    [meta],
                    LayerName.SILVER,
                    meta.source_tag,
                    silver_logger,
                    lambda table_meta, log_id=None, logger=None: self._process_silver_table(
                        table_meta, run_id, log_id, logger, date_partition,
                        bronze_artifact=getattr(table_meta, "upstream_artifact", None)
                    ),
                )
            finally:
                self._clear_cancellation_guards()
            if meta.id not in {table.id for table in silver_tables}:
                raise RuntimeError(f"GOVERNED_SILVER_INCOMPLETE: {meta.source_table}")

            self._log_not_requested(meta, LayerName.GOLD, run_id, nr_logger)
            return silver_tables

        # ── GOLD scope: prereq Silver artifact → Gold only ───────────────────
        if execution_scope == ExecutionScope.GOLD:
            log.info(
                "[execution_scope=GOLD] Running Gold only for %s; "
                "consuming the frozen verified Silver artifact.",
                meta.source_table,
            )
            nr_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
            self._log_not_requested(meta, LayerName.BRONZE, run_id, nr_logger)
            self._log_not_requested(meta, LayerName.SILVER, run_id, nr_logger)
            # Fail closed if Silver is absent; resolve its persisted artifact run.
            silver_artifact = getattr(meta, "upstream_artifact", None)
            if silver_artifact is None:
                raise RuntimeError(
                    "GOVERNED_ARTIFACT_REFERENCE_REQUIRED: Gold-only execution requires "
                    "the exact verified Silver artifact frozen at submission."
                )
            silver_artifact_run_id = str(silver_artifact.producer_run_id)
            log.info(
                "[execution_scope=GOLD] Consuming Silver artifact from run_id=%s.",
                silver_artifact_run_id,
            )

            gold_logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
            self.last_logger = gold_logger
            self._register_cancellation_guards(gold_logger)
            try:
                gold_tables = self._execute_table_batch(
                    [meta],
                    LayerName.GOLD,
                    meta.source_tag,
                    gold_logger,
                    lambda table_meta, log_id=None, logger=None: self._process_gold_table(
                        table_meta,
                        run_id,
                        log_id,
                        logger,
                        cdc_window_run_id=None,
                        silver_artifact_run_id=silver_artifact_run_id,
                        use_persisted_silver=True,
                        silver_artifact=silver_artifact,
                    ),
                )
            finally:
                self._clear_cancellation_guards()
            if meta.id not in {table.id for table in gold_tables}:
                raise RuntimeError(f"GOVERNED_GOLD_INCOMPLETE: {meta.source_table}")
            return gold_tables

        # Unreachable — guarded by ALL_VALUES check above
        raise RuntimeError(f"GOVERNED_EXECUTION_INVALID: unhandled execution_scope {execution_scope!r}")

    def validate_metadata(
        self,
        source_tag: Optional[str] = None,
        source_table: Optional[str] = None,
        target_tag: Optional[str] = None,
        raise_on_error: bool = True,
        quiet: bool = False,
    ):
        """Execute pre-flight metadata validation using framework MetadataValidator."""
        validator = MetadataValidator(self.conn_manager)
        return validator.validate(
            source_tag=source_tag,
            source_table=source_table,
            target_tag=target_tag,
            raise_on_error=raise_on_error,
            quiet=quiet,
        )

    def _execute_table_batch(
        self,
        tables: Sequence[MetadataRow],
        layer_name: str,
        target_tag: str,
        logger: BatchLogger,
        worker_fn: Callable[[MetadataRow], Optional[Tuple[int, int]]],
        on_success: Optional[Callable[[MetadataRow], None]] = None
    ) -> List[MetadataRow]:
        if not tables:
            return []
        self._validate_eligible_tables(tables, argument_name="tables")
        tables = list(tables)
        # Pre-flight framework validation check before processing batch
        try:
            if len(tables) == 1:
                self.validate_metadata(
                    source_tag=tables[0].source_tag or target_tag,
                    source_table=tables[0].source_table,
                    target_tag=tables[0].target_tag,
                    raise_on_error=True,
                    quiet=True,
                )
            else:
                self.validate_metadata(
                    source_tag=tables[0].source_tag if tables else target_tag,
                    target_tag=tables[0].target_tag if tables else None,
                    raise_on_error=True,
                    quiet=True,
                )
        except Exception as val_err:
            for meta in tables:
                log_id = logger.log_start(
                    source_tag=meta.source_tag,
                    source_table=meta.source_table,
                    target_table=meta.target_table or meta.source_table,
                    load_type=layer_name,
                    priority=meta.priority,
                    target_tag=meta.target_tag,
                    policy_version_id=getattr(getattr(meta, "table_contract", None), "policy_version_id", None),
                    policy_fingerprint=getattr(getattr(meta, "table_contract", None), "policy_fingerprint", None),
                )
                logger.log(
                    source_tag=meta.source_tag,
                    source_table=meta.source_table,
                    target_table=meta.target_table or meta.source_table,
                    load_type=layer_name,
                    priority=meta.priority,
                    source_count=0,
                    target_count=0,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    status=RunStatus.FAILED,
                    error_message=f"Pre-flight metadata validation failed: {val_err}",
                    target_tag=meta.target_tag,
                    log_id=log_id,
                )
            raise val_err

        succeeded_tables = []
        
        lineage_svc = LineageService(self.conn_manager)
        tables = lineage_svc.build_execution_dag(tables)
        execution_statuses = {}

        def process_table(meta: MetadataRow) -> str:
            start_time = datetime.now()
            source_count = 0
            target_count = 0
            status = RunStatus.SUCCESS
            err_msg = None
            notes = None
            
            # Check DAG dependencies before starting
            dep_status = lineage_svc.check_dependencies_status(meta.id, execution_statuses)
            if dep_status == "DEPENDENCY_FAILED":
                status = "DEPENDENCY_FAILED"
                err_msg = "Skipped due to upstream dependency failure."
                logger.log(
                    source_tag=meta.source_tag, source_table=meta.source_table,
                    target_table=meta.target_table or meta.source_table,
                    load_type=layer_name, priority=meta.priority,
                    source_count=0, target_count=0, start_time=start_time, end_time=datetime.now(),
                    status=status, error_message=err_msg, target_tag=meta.target_tag
                )
                return status

            target_table_name = (
                meta.landing_table if layer_name == LayerName.BRONZE else
                meta.history_table if layer_name == LayerName.SILVER else
                meta.target_table
            ) or meta.source_table

            log_id = None
            lock_conn = None
            is_locked = False
            try:
                # ── Session Advisory lock: Held throughout single table processing ──
                try:
                    lock_conn = self.conn_manager.get_raw_metadata_db_connection()
                    cur = lock_conn.cursor()
                    cur.execute(
                        "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s))",
                        [meta.source_tag, meta.source_table]
                    )
                    is_locked = cur.fetchone()[0]
                except Exception as lock_err:
                    log.warning(f"Could not acquire session advisory lock for {meta.source_table}: {lock_err}")
                    is_locked = False

                if not is_locked:
                    print(f"Skipping {meta.source_table} - TABLE_LOCKED (Advisory lock busy)")
                    status = RunStatus.SKIPPED
                    err_msg = "EXECUTION_OWNERSHIP_REQUIRED: Table is locked by another running pipeline."
                    return status


                # ── MAP-01: Resolve Mapping Snapshot exactly once per execution attempt ──
                try:
                    from src.metadata.metadata_models import MappingPolicy
                    policy_enum = MappingPolicy[meta.mapping_policy.upper()]
                    
                    # We need the DataFrame schema for IDENTITY frozen snapshot if not yet fetched?
                    # Wait, we don't have the DataFrame here. 
                    # If IDENTITY, we can only fetch the frozen_source_schema after we read from the source.
                    # Since we resolve mappings here, how do we pass frozen_source_schema?
                    if getattr(meta, "resolved_mappings", None) is None:
                        meta.resolved_mappings = self.column_mapper.resolve_table_mappings(meta.id, mapping_policy=policy_enum)
                except Exception as e:
                    status = RunStatus.FAILED
                    err_msg = f"Mapping resolution failed: {e}"
                    print(f"Skipping {meta.source_table} - {err_msg}")
                    return status

                print(f"{layer_name.capitalize()} processing: {meta.source_table}...")
                log_id = logger.log_start(
                    source_tag=meta.source_tag,
                    source_table=meta.source_table,
                    target_table=target_table_name,
                    load_type=layer_name,
                    priority=meta.priority,
                    target_tag=meta.target_tag,
                    policy_version_id=getattr(getattr(meta, "table_contract", None), "policy_version_id", None),
                    policy_fingerprint=getattr(getattr(meta, "table_contract", None), "policy_fingerprint", None),
                )
                if not log_id:
                    from src.exceptions import ExecutionOwnershipRequiredError
                    status = RunStatus.SKIPPED
                    err_msg = "EXECUTION_OWNERSHIP_REQUIRED: Failed to acquire active lease for execution."
                    return status

                counts = worker_fn(meta, log_id, logger)
                if counts is None:
                    logger.log(
                        source_tag=meta.source_tag,
                        source_table=meta.source_table,
                        target_table=target_table_name,
                        load_type=layer_name,
                        priority=meta.priority,
                        source_count=0,
                        target_count=0,
                        start_time=start_time,
                        end_time=start_time,
                        status=RunStatus.SKIPPED,
                        error_message="Table is not due for CDC load (delta_days not met)",
                        target_tag=meta.target_tag,
                        log_id=log_id,
                    )
                    status = RunStatus.SKIPPED  # guard the finally block — must be set before return
                    return status

                notes = None
                expected_loss = 0
                if isinstance(counts, tuple):
                    if len(counts) >= 4:
                        source_count, target_count, notes, expected_loss = counts[:4]
                        if expected_loss is None:
                            expected_loss = 0
                        if (
                            isinstance(expected_loss, bool)
                            or not isinstance(expected_loss, Integral)
                            or expected_loss < 0
                        ):
                            raise RuntimeError(
                                f"Invalid certified expected loss for {meta.source_table}: {expected_loss!r}"
                            )
                        expected_loss = int(expected_loss)
                    elif len(counts) == 3:
                        source_count, target_count, notes = counts
                    else:
                        source_count, target_count = counts
                else:
                    source_count, target_count = counts

                status = Reconciler.reconcile(
                    source_count,
                    target_count,
                    layer_name,
                    meta,
                    expected_loss=expected_loss,
                )
                print(f"{layer_name.capitalize()} loaded: {meta.source_table} (source={source_count}, target={target_count})")
                succeeded_tables.append(meta)
                if status in (RunStatus.SUCCESS, RunStatus.RECONCILIATION_WARNING) and on_success:
                    on_success(meta)
            except KeyboardInterrupt:
                status = "CANCELLED"
                err_msg = "Cancelled by operator (KeyboardInterrupt)."
                if log_id:
                    logger.mark_run_cancelled(reason=err_msg)
                raise
            except RuntimeError as e:
                err_msg = str(e)
                if "reconcil" in err_msg.lower() or "certification" in err_msg.lower():
                    status = RunStatus.RECONCILIATION_FAILED
                    print(f"{layer_name.capitalize()} failed for {meta.source_table} (Reconciliation): {err_msg}")
                else:
                    status = RunStatus.FAILED
                    print(f"{layer_name.capitalize()} failed for {meta.source_table}: {err_msg}")
            except Exception as e:
                status = RunStatus.FAILED
                err_msg = str(e)
                print(f"{layer_name.capitalize()} failed for {meta.source_table}: {err_msg}")
            finally:
                if lock_conn:
                    try:
                        with lock_conn.cursor() as unlock_cur:
                            unlock_cur.execute(
                                "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                                [meta.source_tag, meta.source_table]
                            )
                        lock_conn.commit()
                        lock_conn.close()
                    except Exception as unlock_err:
                        log.debug(f"Error unlocking advisory lock for {meta.source_table}: {unlock_err}")

                if status != RunStatus.SKIPPED and not (err_msg and "EXECUTION_OWNERSHIP_REQUIRED" in err_msg) and not (err_msg and "Mapping resolution failed" in err_msg):
                    end_ts = datetime.now()
                    try:
                        logger.log(
                            source_tag=meta.source_tag,
                            source_table=meta.source_table,
                            target_table=target_table_name,
                            load_type=layer_name,
                            priority=meta.priority,
                            source_count=source_count,
                            target_count=target_count,
                            start_time=start_time,
                            end_time=end_ts,
                            status=status,
                            error_message=err_msg,
                            target_tag=meta.target_tag,
                            notes=notes,
                            log_id=log_id
                        )
                    except ExecutionLeaseLostError:
                        raise
                    except Exception as log_err:
                        log.error("Failed writing final batch_log for %s: %s", meta.source_table, log_err)

                    try:
                        if status in (RunStatus.SUCCESS, RunStatus.RECONCILIATION_WARNING):
                            self.metadata_reader.update_table_success(meta.id)
                        else:
                            self.metadata_reader.update_table_failure(meta.id)
                    except Exception as meta_err:
                        log.error("Failed updating metadata success/failure for %s: %s", meta.source_table, meta_err)
            return status

        # Execute sequentially to properly evaluate LineageService dependencies.
        # Future enhancement: Dependency-aware parallel executor.
        for table in tables:
            try:
                exec_status = process_table(table)
                execution_statuses[table.id] = exec_status
            except ExecutionLeaseLostError:
                raise
            except Exception as table_err:
                log.exception(
                    "Table %s execution failed in layer %s: %s; continuing with remaining tables.",
                    getattr(table, "source_table", table),
                    layer_name,
                    table_err,
                )
                execution_statuses[getattr(table, "id", None)] = RunStatus.FAILED

        logger.summarize_and_log_layer(
            target_tag, layer_name,
            target_tag=tables[0].target_tag if tables else None,
            priority=tables[0].priority if tables else None,
            action=self.current_action
        )
        return succeeded_tables

    @staticmethod
    def _validate_eligible_tables(
        tables: Sequence[MetadataRow],
        *,
        argument_name: str = "eligible_tables",
    ) -> None:
        """Reject degraded table identities before a downstream layer runs.

        Layer hand-off must carry the complete ``MetadataRow`` produced by the
        authoritative metadata reader.  A table name alone is ambiguous across
        source schemas/tags and cannot safely drive a subsequent layer.
        """
        if isinstance(tables, (str, bytes)) or not isinstance(tables, Sequence):
            raise TypeError(f"{argument_name} must be a sequence of MetadataRow objects")
        invalid = [type(table).__name__ for table in tables if not isinstance(table, MetadataRow)]
        if invalid:
            raise TypeError(
                f"{argument_name} must contain only MetadataRow objects; "
                f"received {', '.join(invalid)}"
            )

    def _freeze_schema_if_identity(self, meta: MetadataRow, df: DataFrame):
        """Captures the source schema at execution time if policy is IDENTITY."""
        from src.metadata.metadata_models import MappingPolicy
        if meta.resolved_mappings and meta.resolved_mappings.mapping_policy == MappingPolicy.IDENTITY and not meta.resolved_mappings.frozen_source_schema:
            meta.resolved_mappings = meta.resolved_mappings.with_frozen_schema(df.columns)

    def load_with_db_link(self, priority: int, target_tag: str, table_name: Optional[str] = None, run_id: Optional[str] = None, override_target_tag: Optional[str] = None) -> List[MetadataRow]:
        """
        Landing (Bronze) load: exact copy of source table written to parquet.
        """
        run_id = run_id or str(uuid.uuid4())
        logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
        self.last_run_id = run_id
        self.last_logger = logger
        self._register_cancellation_guards(logger)
        if not getattr(self, "current_action", None):
            self.current_action = "run_landing"
        
        tables = self.metadata_reader.get_tables(
            source_tag=target_tag,
            priority=priority,
            table_name=table_name,
            load_type=None
        )
        if override_target_tag:
            for meta in tables:
                meta.target_tag = override_target_tag
        
        if not tables:
            msg = f"No active or eligible tables found for priority tier {priority} (tag={target_tag}, table={table_name or 'all'}). Skipping Landing layer."
            print(f"[Landing Info] {msg}")
            logger.mark_run_skipped(reason=msg)
            return []
            
        print(f"Starting Landing load for {len(tables)} tables (Run ID: {run_id})...")

        # CDC-HYDRATE: Materialize frozen policy contract for CDC tables that
        # arrive via the Airflow/CLI path without a pre-attached table_contract.
        from src.metadata.policy_governance import PolicyGovernanceService
        _governance = PolicyGovernanceService(self.conn_manager)
        for _meta in tables:
            if str(getattr(_meta, "is_cdc", "n")).lower() == "y" and not getattr(_meta, "table_contract", None):
                try:
                    _meta.table_contract = _contract = _governance.materialize_execution_contract(_meta.id)
                    _meta.resolved_mappings = getattr(_contract, "resolved_mappings", None)
                    _approved_key = getattr(_contract, "approved_source_business_key", None)
                    _meta.identity_mode = getattr(_approved_key, "identity_mode", None)
                    _meta.exception_values = tuple(getattr(_approved_key, "exception_values", ()) or ())
                    _cdc_policy = getattr(_contract, "cdc_policy", None)
                    _meta.cdc_activation_state = str(getattr(_cdc_policy, "activation_state", "") or "").upper() or None
                except Exception as _contract_err:
                    log.warning(
                        "[%s] CDC contract hydration failed; "
                        "table will fail at worker execution: %s",
                        _meta.source_table, _contract_err,
                    )

                # ── EXTRACT ───────────────────────────────────────────────────
                # ── BRONZE WRITE ───────────────────────────────────────────────
                # Count verification — logged, not a separate phase
        result = self._execute_table_batch(tables, LayerName.BRONZE, target_tag, logger, self._process_bronze_table)
        self._clear_cancellation_guards()
        return result

    def _process_bronze_table(self, meta: MetadataRow, log_id: Optional[int] = None, logger: Optional[BatchLogger] = None) -> Optional[Tuple[int, int]]:
        cdc_filter = (
            self._build_governed_cdc_filter(meta, self.last_run_id)
            if str(meta.is_cdc).lower() == "y" and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT"
            else None
        )
        sem = self.conn_manager.get_semaphore(meta.source_tag)
        with sem:
            profiler = StageProfiler(
                conn_manager=self.conn_manager,
                run_id=self.last_run_id,
                source_tag=meta.source_tag,
                source_table=meta.source_table,
                layer="BRONZE",
                log_id=log_id,
            )

            source_cfg = self.conn_manager.get(meta.source_tag, expected_role="source")
            _is_spark_connect = bool(os.environ.get("SPARK_CONNECT_URL"))
            _is_local_reachable_db = getattr(source_cfg, "db_type", "").lower() in ("postgres", "postgresql")
            if not _is_spark_connect or _is_local_reachable_db:
                if not test_jdbc_connection(source_cfg, self.spark):
                    raise RuntimeError(f"Source connection test failed for tag '{meta.source_tag}'")
            else:
                log.info(
                    f"[{meta.source_table}] Skipping local JDBC pre-flight for '{meta.source_tag}' "
                    f"(db_type={getattr(source_cfg, 'db_type', '?')}) — "
                    f"Spark Connect will test connectivity during EXTRACT."
                )

            # ── EXTRACT ───────────────────────────────────────────────────
            if logger and log_id:
                logger.update_phase(log_id, "EXTRACTING")
            log.info(f"[{meta.source_table}] Starting EXTRACT from source via JDBC")
            profiler.start_stage(PipelinePhase.EXTRACTING)
            df = self.extractor.read(meta, cdc_filter=cdc_filter)
            self._freeze_schema_if_identity(meta, df)
            source_count = Reconciler.count_and_cache(df)
            profiler.end_stage(PipelinePhase.EXTRACTING, row_count=source_count)

            # ── BRONZE WRITE ───────────────────────────────────────────────
            if logger and log_id:
                logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=source_count, processed_count=0, source_count=source_count)
            SchemaValidator.validate_layer_contract(df, "BRONZE", meta.source_table)
            date_partition = datetime.now().strftime("%Y-%m-%d")
            log.info(f"[{meta.source_table}] Starting BRONZE_WRITE: {source_count:,} rows → Parquet")
            profiler.start_stage(PipelinePhase.BRONZE_WRITE)
            landing_path = self.loader.write_landing(
                df,
                meta,
                date_partition,
                before_persist=lambda: logger.renew_active_lease_or_raise(log_id),
                run_id=self.last_run_id if getattr(self, "current_action", None) == "run_governed" else None,
            )
            profiler.end_stage(PipelinePhase.BRONZE_WRITE, row_count=source_count)

            # Count verification
            target_count = Reconciler.count_parquet(self.spark, landing_path)
            if target_count != source_count:
                log.warning(
                    "[%s] Bronze source drift detected during write: pre_write_count=%s, "
                    "persisted_bronze_count=%s. Using persisted Bronze count for "
                    "Bronze reconciliation.",
                    meta.source_table,
                    source_count,
                    target_count,
                )
                source_count = target_count
                log.info(f"[{meta.source_table}] Parquet count verified: {target_count:,} rows")
            if logger and log_id:
                logger.update_chunk_progress(
                    log_id,
                    actual_chunk_rows=target_count,
                    chunk_number=1,
                    total_chunks=1,
                    stage="BRONZE_FINALIZE",
                )
                logger.update_phase(
                    log_id,
                    "BRONZE_FINALIZE",
                    extracted_count=source_count,
                    processed_count=target_count,
                    source_count=source_count,
                )

            if getattr(self, "current_action", None) == "run_governed":
                meta._bronze_artifact = self._publish_layer_artifact(
                    meta,
                    run_id=self.last_run_id,
                    log_id=log_id,
                    storage_layer="BRONZE",
                    storage_uri=landing_path,
                    row_count=target_count,
                    dataframe=df,
                )

            # Data Quality: Bronze Layer
            dq_engine = DQEngine(self.conn_manager, self.last_run_id)
            action = dq_engine.evaluate(df, meta.id, meta.source_table, meta.source_tag, "BRONZE")
            if action in ("SKIP_TABLE", "FAIL_SOURCE", "FAIL_PIPELINE"):
                raise RuntimeError(f"Data Quality {action} triggered at BRONZE layer.")

            df.unpersist(blocking=False)

        return source_count, target_count

    def load_historyData(self, priority: int, target_tag: str, table_name: Optional[str] = None, run_id: Optional[str] = None, eligible_tables: Optional[Sequence[MetadataRow]] = None, override_target_tag: Optional[str] = None, date_partition: Optional[str] = None) -> List[MetadataRow]:
        """
        History (Silver) load: deduplicated, cast columns, audit columns added.
        """
        run_id = run_id or str(uuid.uuid4())
        logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
        self.last_run_id = run_id
        self.last_logger = logger
        self._register_cancellation_guards(logger)
        if not getattr(self, "current_action", None):
            self.current_action = "run_history"
        if eligible_tables is not None:
            self._validate_eligible_tables(eligible_tables)
        
        tables = self.metadata_reader.get_tables(
            source_tag=target_tag,
            priority=priority,
            table_name=table_name,
            bypass_skip=(eligible_tables is not None or table_name is not None or getattr(self, "current_action", "") == "run_history")
        )
        if eligible_tables is not None:
            eligible_ids = {table.id for table in eligible_tables}
            tables = [table for table in tables if table.id in eligible_ids]
        if override_target_tag:
            for meta in tables:
                meta.target_tag = override_target_tag
        
        if not tables:
            print(f"No tables to process for History layer (priority={priority}, tag={target_tag})")
            return []
            
        print(f"Starting History load for {len(tables)} tables (Run ID: {run_id})...")

        # CDC-HYDRATE: Materialize frozen policy contract for CDC tables that
        # arrive via the Airflow/CLI path without a pre-attached table_contract.
        from src.metadata.policy_governance import PolicyGovernanceService
        _governance = PolicyGovernanceService(self.conn_manager)
        for _meta in tables:
            if str(getattr(_meta, "is_cdc", "n")).lower() == "y" and not getattr(_meta, "table_contract", None):
                try:
                    _meta.table_contract = _contract = _governance.materialize_execution_contract(_meta.id)
                    _meta.resolved_mappings = getattr(_contract, "resolved_mappings", None)
                    _approved_key = getattr(_contract, "approved_source_business_key", None)
                    _meta.identity_mode = getattr(_approved_key, "identity_mode", None)
                    _meta.exception_values = tuple(getattr(_approved_key, "exception_values", ()) or ())
                    _cdc_policy = getattr(_contract, "cdc_policy", None)
                    _meta.cdc_activation_state = str(getattr(_cdc_policy, "activation_state", "") or "").upper() or None
                except Exception as _contract_err:
                    log.warning(
                        "[%s] CDC contract hydration failed; "
                        "table will fail at worker execution: %s",
                        _meta.source_table, _contract_err,
                    )

        def worker(meta: MetadataRow, log_id: Optional[int] = None, logger: Optional[BatchLogger] = None) -> Optional[Tuple[int, int, str, int]]:
            return self._process_silver_table(meta, run_id, log_id, logger, date_partition)

        result = self._execute_table_batch(tables, LayerName.SILVER, target_tag, logger, worker)
        self._clear_cancellation_guards()
        return result

    def _process_silver_table(
        self,
        meta: MetadataRow,
        run_id: str,
        log_id: Optional[int],
        logger: Optional[BatchLogger],
        date_partition: Optional[str],
        bronze_artifact: Any = None,
    ) -> Optional[Tuple[int, int, str, int]]:
        contract = getattr(meta, "table_contract", None)
        has_governed_cdc_policy = bool(contract and getattr(contract, "cdc_policy", None))
        if meta.is_cdc == 'y' and not has_governed_cdc_policy and self.cdc_handler.should_skip(meta):
            return None

        transformer = Transformer(run_id)
        profiler = StageProfiler(
            conn_manager=self.conn_manager,
            run_id=run_id,
            source_tag=meta.source_tag,
            source_table=meta.source_table,
            layer="SILVER",
            log_id=log_id,
        )

        # Schema Drift Pre-Check
        try:
            drift_detector = SchemaDriftDetector(self.spark, self.conn_manager, run_id)
            drift_detector.detect(meta)
        except SchemaDriftException as e:
            raise RuntimeError(str(e))

        # ── BRONZE READ ────────────────────────────────────────────────────
        active_date_partition = date_partition or datetime.now().strftime("%Y-%m-%d")
        if logger and log_id:
            logger.update_phase(log_id, "BRONZE_READ")
        log.info(f"[{meta.source_table}] Starting BRONZE_READ from Parquet landing zone")
        profiler.start_stage(PipelinePhase.BRONZE_READ)
        if bronze_artifact is not None:
            if str(getattr(meta, "is_cdc", "n")).lower() == "y" and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
                self._restore_persisted_cdc_context(meta, bronze_artifact)
            df = self._read_verified_artifact(meta, bronze_artifact)
        else:
            df = self.extractor.read_landing(meta, date_partition=active_date_partition)
        self._freeze_schema_if_identity(meta, df)
        source_count = Reconciler.count_and_cache(df)
        profiler.end_stage(PipelinePhase.BRONZE_READ, row_count=source_count)

        # ── TRANSFORM ─────────────────────────────────────────────────────
        if logger and log_id:
            logger.update_phase(log_id, "TRANSFORM", extracted_count=source_count, source_count=source_count)
        log.info(f"[{meta.source_table}] Starting TRANSFORM: add key + deduplicate")
        profiler.start_stage(PipelinePhase.TRANSFORM)
        df_clean = transformer.sanitize_business_keys(df, meta)
        df_key = transformer.add_source_identification_key(df_clean, meta)
        conditional_key, conditional_exceptions = self._conditional_identity_parts(meta)
        if conditional_key:
            # Preserve the bounded exception multiset exactly.  Only the
            # normal lane is deduplicated by business key; otherwise the
            # exceptional rows would be silently collapsed before Gold.
            normal_df = df_key.filter(~F.col(conditional_key).isin(list(conditional_exceptions)))
            exception_df = df_key.filter(F.col(conditional_key).isin(list(conditional_exceptions)))
            normal_dedup = transformer.deduplicate(normal_df, meta)
            df_dedup = normal_dedup.unionByName(exception_df, allowMissingColumns=True)
        else:
            df_dedup = transformer.deduplicate(df_key, meta)
        dedup_count = Reconciler.count_and_cache(df_dedup)
        profiler.end_stage(PipelinePhase.TRANSFORM, row_count=dedup_count)

        duplicates_removed = max(0, source_count - dedup_count)
        notes = f"Duplicates removed: {duplicates_removed}"

        # ── DEDUP CERTIFICATION ────────────────────────────────────────────
        expected_loss = 0
        if duplicates_removed > 0 and getattr(meta, 'skip_dedup', 'n').lower() != 'y':
            if logger and log_id:
                logger.update_phase(log_id, "DEDUP")
            nat_keys = transformer._parse_natural_keys(df, meta)
            if nat_keys:
                log.info(f"[{meta.source_table}] Starting DEDUP natural key count")
                distinct_nat_count = df.select(*nat_keys).distinct().count()
                log.info(f"[{meta.source_table}] DEDUP: {distinct_nat_count:,} distinct natural keys")
                expected_loss = 0 if distinct_nat_count == source_count else duplicates_removed
                Reconciler.certify_execution(
                    input_count=source_count,
                    output_count=dedup_count,
                    actual_loss=duplicates_removed,
                    expected_loss=expected_loss,
                    layer="SILVER",
                    table_name=meta.source_table,
                    distinct_natural_key_count=distinct_nat_count,
                )

        df_hash = transformer.add_business_hash(df_dedup, meta)
        df_audit = transformer.add_audit_columns(df_hash, meta)
        df_audit = transformer.cast_columns(df_audit, meta)
        df_audit = transformer.select_target_columns(df_audit, meta)

        SchemaValidator.validate_layer_contract(df_audit, "SILVER", meta.source_table)

        # ── SILVER WRITE ───────────────────────────────────────────────────
        if logger and log_id:
            logger.update_phase(log_id, "SILVER_WRITE", extracted_count=source_count, processed_count=0, source_count=source_count)
        log.info(f"[{meta.source_table}] Starting SILVER_WRITE: {dedup_count:,} rows → Delta/Parquet history")
        profiler.start_stage(PipelinePhase.SILVER_WRITE)
        history_result = self.loader.write_history(
            df_audit,
            meta,
            before_persist=lambda: logger.renew_active_lease_or_raise(log_id),
            run_id=run_id if getattr(self, "current_action", None) == "run_governed" else None,
            return_artifact=getattr(self, "current_action", None) == "run_governed",
        )
        if isinstance(history_result, tuple):
            history_path, silver_artifact_path = history_result
        else:
            history_path, silver_artifact_path = history_result, None
        profiler.end_stage(PipelinePhase.SILVER_WRITE, row_count=dedup_count)

        # Count verification.  Governed writes also persist an exact, isolated
        # Parquet artifact; verify that path directly so historical run
        # artifacts can never be interpreted as table partitions.
        if silver_artifact_path:
            target_count = Reconciler.count_parquet(
                self.spark, silver_artifact_path, run_id=run_id
            )
        else:
            try:
                target_count = Reconciler.count_delta(self.spark, history_path, run_id=run_id)
            except Exception:
                target_count = Reconciler.count_parquet(self.spark, history_path, run_id=run_id)
        log.info(f"[{meta.source_table}] History count verified: {target_count:,} rows")
        if logger and log_id:
            logger.update_chunk_progress(log_id, actual_chunk_rows=target_count, chunk_number=1, total_chunks=1, stage="SILVER_FINALIZE")

        if getattr(self, "current_action", None) == "run_governed":
            if not silver_artifact_path:
                raise RuntimeError("GOVERNED_ARTIFACT_MISSING: Silver writer did not return a run artifact")
            silver_artifact = self._publish_layer_artifact(
                meta,
                run_id=run_id,
                log_id=log_id,
                storage_layer="SILVER",
                storage_uri=silver_artifact_path,
                row_count=dedup_count,
                dataframe=df_audit,
            )
            meta._silver_artifact = silver_artifact

        # Data Quality: Silver Layer
        dq_engine = DQEngine(self.conn_manager, run_id)
        action = dq_engine.evaluate(df_audit, meta.id, meta.source_table, meta.source_tag, "SILVER")
        if action in ("SKIP_TABLE", "FAIL_SOURCE", "FAIL_PIPELINE"):
            raise RuntimeError(f"Data Quality {action} triggered at SILVER layer.")

        df.unpersist(blocking=False)
        df_dedup.unpersist(blocking=False)

        return source_count, target_count, notes, expected_loss

    def _is_transient_error(self, err_msg: str) -> bool:
        """Classify if an error is transient and safe to retry.

        Returns False for empty/None errors, or when the error matches a known
        non-transient pattern (schema bugs, syntax errors, missing columns/tables).
        Returns True otherwise — i.e. the error is likely network/lock/timeout related
        and a retry may succeed.
        """
        if not err_msg:
            return False
        err_lower = err_msg.lower()

        # Non-transient errors (definitely code/syntax/schema bugs that will fail again on retry)
        non_transient = [
            "syntax", "undefinedcolumn", "undefinedtable", "no such table",
            "no such column", "column does not exist", "relation does not exist",
            "partition column type should be", "zero columns", "has zero columns",
            "validation error", "cannot resolve", "analysisexception", "classnotfoundexception",
            "driver not found", "division by zero", "datatype mismatch", "data type mismatch",
            "dependency", "skipped due to",
            # FK / reconciliation / unique constraint errors are structural — retrying the same run_id
            # will hit the same constraint until the manifest is resolved.
            "violates foreign key constraint", "reconciliation_row_loss", "business_key_required",
            "violates unique constraint", "duplicate key value", "unq_cdc_commit_attempt",
        ]
        if any(p in err_lower for p in non_transient):
            return False

        return True

    def retry_failed_tables(self, run_id: Optional[str] = None, max_attempts: int = 2, override_target_tag: Optional[str] = None) -> None:
        """Re-run Landing load for tables that failed in the current pipeline run.

        Only retries transient failures and reuses the parent run_id to ensure
        observable tracking and correct execution summaries.
        """
        if not run_id:
            log.info("No run_id specified for retry_failed_tables, skipping.")
            return

        # Fetch failed tables specifically for this run_id from metadata.batch_log
        failed_rows = []
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT source_tag, source_table, load_type, error_message
                        FROM metadata.batch_log
                        WHERE run_id = %s AND status NOT IN ('SUCCESS', 'SKIPPED', 'DEPENDENCY_FAILED')
                    """, [run_id])
                    failed_rows = cur.fetchall()

        except Exception as e:
            log.error(f"Failed to query failed tables for run retry: {e}")
            return

        if not failed_rows:
            log.info("No tables flagged for retry.")
            return

        # Filter out duplicates and classify errors
        seen = set()
        failed = []
        for row in failed_rows:
            if len(row) >= 4:
                tag, table, l_type, err_msg = row[0], row[1], row[2], row[3]
            else:
                tag, table, err_msg = row[0], row[1], row[2]
                l_type = "landing"

            key = (tag, table, (l_type or "").lower())
            if key in seen:
                continue
            seen.add(key)

            if not self._is_transient_error(err_msg):
                log.info(f"Skipping retry for table '{tag}.{table}' ({l_type}) (error is non-transient: '{err_msg[:80] if err_msg else ''}...')")
                continue

            failed.append({"tag": tag, "table": table, "load_type": (l_type or "").lower()})

        if not failed:
            log.info("No transient failures found to retry.")
            return

        log.info(f"Retrying {len(failed)} transient failed table execution(s) for run_id={run_id} (max_attempts={max_attempts}).")
        
        for attempt_num in range(1, max_attempts + 1):
            remaining = []
            for entry in failed:
                tag = entry["tag"]
                table = entry["table"]
                l_type = entry["load_type"]
                meta = self.metadata_reader.get_table_by_schema_and_name(None, table, tag)
                if not meta:
                    log.warning(f"Metadata not found for {table} (tag={tag}), skipping retry.")
                    continue

                log.info(f"[Retry {attempt_num}/{max_attempts}] Re-running {l_type.upper()} for {table}")
                if l_type in ("landing", "bronze"):
                    result = self.load_with_db_link(
                        priority=meta.priority,
                        target_tag=meta.source_tag,
                        table_name=meta.source_table,
                        run_id=run_id,
                        override_target_tag=override_target_tag
                    )
                elif l_type in ("history", "silver"):
                    result = self.load_historyData(
                        priority=meta.priority,
                        target_tag=meta.source_tag,
                        table_name=meta.source_table,
                        run_id=run_id,
                        override_target_tag=override_target_tag
                    )
                elif l_type in ("target", "gold"):
                    result = self.run_target_cdc(
                        target_tag=meta.source_tag,
                        table_name=meta.source_table,
                        priority=meta.priority,
                        run_id=run_id,
                        override_target_tag=override_target_tag
                    )
                else:
                    result = self.load_with_db_link(
                        priority=meta.priority,
                        target_tag=meta.source_tag,
                        table_name=meta.source_table,
                        run_id=run_id,
                        override_target_tag=override_target_tag
                    )
                if not result:
                    remaining.append(entry)
            failed = remaining
            if not failed:
                log.info("All retries succeeded.")
                break

        if failed:
            log.warning(f"{len(failed)} table(s) still failed after {max_attempts} attempts.")
        else:
            # If all retries succeeded, clear retry flags
            self.metadata_reader.clear_retry_flags()

    # ══════════════════════════════════════════════════════════════════════════
    # Target (Gold) Layer — CDC Incremental Load
    # ══════════════════════════════════════════════════════════════════════════

    def run_target_cdc(self, target_tag: str,
                       table_name: Optional[str] = None,
                       priority: Optional[int] = None,
                       run_id: Optional[str] = None,
                       eligible_tables: Optional[Sequence[MetadataRow]] = None,
                       override_target_tag: Optional[str] = None) -> List[MetadataRow]:
        """
        Target (Gold) load: Incremental CDC load using watermarks and Delta merge.
        """
        run_id = run_id or str(uuid.uuid4())
        logger = BatchLogger(self.conn_manager, run_id, self.spark_app_id)
        self.last_run_id = run_id
        self.last_logger = logger
        self._register_cancellation_guards(logger)
        if not getattr(self, "current_action", None):
            self.current_action = "run_target"
        if eligible_tables is not None:
            self._validate_eligible_tables(eligible_tables)

        tables = self.metadata_reader.get_gold_tables(
            source_tag=target_tag,
            table_name=table_name,
            priority=priority,
            bypass_skip=(eligible_tables is not None or table_name is not None or getattr(self, "current_action", "") in ("run_target", "refresh_tables_cdc_12AM"))
        )
        if eligible_tables is not None:
            eligible_ids = {table.id for table in eligible_tables}
            tables = [table for table in tables if table.id in eligible_ids]
        if override_target_tag:
            for meta in tables:
                meta.target_tag = override_target_tag

        if not tables:
            print(f"No active tables to process for Target layer (tag={target_tag})")
            return []

        print(f"Starting Target CDC load for {len(tables)} tables (Run ID: {run_id})...")

        # CDC-HYDRATE: Materialize frozen policy contract for CDC tables that
        # arrive via the Airflow/CLI path without a pre-attached table_contract.
        from src.metadata.policy_governance import PolicyGovernanceService
        _governance = PolicyGovernanceService(self.conn_manager)
        for _meta in tables:
            if str(getattr(_meta, "is_cdc", "n")).lower() == "y" and not getattr(_meta, "table_contract", None):
                try:
                    _meta.table_contract = _contract = _governance.materialize_execution_contract(_meta.id)
                    _meta.resolved_mappings = getattr(_contract, "resolved_mappings", None)
                    _approved_key = getattr(_contract, "approved_source_business_key", None)
                    _meta.identity_mode = getattr(_approved_key, "identity_mode", None)
                    _meta.exception_values = tuple(getattr(_approved_key, "exception_values", ()) or ())
                    _cdc_policy = getattr(_contract, "cdc_policy", None)
                    _meta.cdc_activation_state = str(getattr(_cdc_policy, "activation_state", "") or "").upper() or None
                except Exception as _contract_err:
                    log.warning(
                        "[%s] CDC contract hydration failed; "
                        "table will fail at worker execution: %s",
                        _meta.source_table, _contract_err,
                    )

        def worker(meta: MetadataRow, log_id: Optional[int] = None, logger: Optional[BatchLogger] = None) -> Optional[Tuple[int, int, str, int]]:
            return self._process_gold_table(meta, run_id, log_id, logger)

        result = self._execute_table_batch(tables, LayerName.GOLD, target_tag, logger, worker, None)
        self._clear_cancellation_guards()
        return result

    # Backwards-compatible alias for existing CLI / cron invocations
    refresh_tables_cdc_12AM = run_target_cdc

    def _process_gold_table(
        self,
        meta: MetadataRow,
        run_id: str,
        log_id: Optional[int],
        logger: Optional[BatchLogger],
        *,
        cdc_window_run_id: Optional[str] = None,
        silver_artifact_run_id: Optional[str] = None,
        use_persisted_silver: bool = False,
        silver_artifact: Any = None,
    ) -> Optional[Tuple[int, int, str, int]]:
        transformer = Transformer(run_id)
        # Keep the successful write metrics available for final progress
        # telemetry.  CDC must report rows submitted to this run's atomic
        # upsert, not the total number of rows already present in the target.
        gold_metrics = None
        expected_loss = 0
        if meta.is_cdc == 'y' and not use_persisted_silver and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
            # ── Incremental CDC merge path ──────────────────────────────
            cdc_filter = self._build_governed_cdc_filter(meta, cdc_window_run_id or run_id)

            # EXTRACT
            if logger and log_id:
                logger.update_phase(log_id, "EXTRACTING")
            log.info(f"[{meta.source_table}] Starting EXTRACT (CDC incremental) from source")
            df = self.extractor.read(meta, cdc_filter)
            self._freeze_schema_if_identity(meta, df)
            source_count = Reconciler.count_and_cache(df)
            self._validate_raw_cdc_key_window(meta, df, log_id=log_id, logger=logger)

            # TRANSFORM
            if logger and log_id:
                logger.update_phase(log_id, "GOLD_TRANSFORM", extracted_count=source_count, source_count=source_count)
            log.info(f"[{meta.source_table}] Starting TRANSFORM: add key + deduplicate")
            df_clean = transformer.sanitize_business_keys(df, meta)
            df_key = transformer.add_source_identification_key(df_clean, meta)
            conditional_key, conditional_exceptions = self._conditional_identity_parts(meta)
            if conditional_key:
                # Keep every row for the bounded exception keys.  Only the
                # normal lane is deduplicated before the keyed upsert.
                normal_df = df_key.filter(~F.col(conditional_key).isin(list(conditional_exceptions)))
                exception_df = df_key.filter(F.col(conditional_key).isin(list(conditional_exceptions)))
                df_dedup = transformer.deduplicate(normal_df, meta).unionByName(
                    exception_df,
                    allowMissingColumns=True,
                )
            else:
                df_dedup = transformer.deduplicate(df_key, meta)
            dedup_count = Reconciler.count_and_cache(df_dedup)
            duplicates_removed = max(0, source_count - dedup_count)
            expected_loss = duplicates_removed
            notes = f"Duplicates removed: {duplicates_removed}"

            df_hash = transformer.add_business_hash(df_dedup, meta)
            df = transformer.add_audit_columns(df_hash, meta)
            df = transformer.cast_columns(df, meta)
            df = transformer.select_target_columns(df, meta)
            SchemaValidator.validate_layer_contract(df, "GOLD", meta.source_table)

            # ── TARGET RESOLUTION, CONNECTIVITY & PRE-FLIGHT ──
            if logger and log_id:
                logger.update_phase(log_id, "RESOLVING_TARGET", extracted_count=source_count, source_count=source_count)
            target_cfg = self.conn_manager.get(meta.target_tag, expected_role="target")

            if logger and log_id:
                logger.update_phase(log_id, "CONNECTING_TARGET", extracted_count=source_count, source_count=source_count)

            merge_key_val = (
                ",".join(meta.canonical_source_business_key)
                if meta.canonical_source_business_key
                else meta.natural_key_column or getattr(meta, "normalized_natural_key", None) or "source_identification_key"
            )
            if logger and log_id:
                logger.update_phase(log_id, "TARGET_PREFLIGHT", extracted_count=source_count, source_count=source_count)

            # GOLD WRITE
            if logger and log_id:
                logger.update_phase(log_id, "GOLD_WRITE", extracted_count=source_count, processed_count=0, source_count=source_count)
            if conditional_key:
                target_path, gold_metrics = self.loader.write_target_bounded_exception(
                    df,
                    meta,
                    merge_key=merge_key_val,
                    exception_values=conditional_exceptions,
                    before_target_dml=lambda: logger.renew_active_lease_or_raise(log_id),
                )
            else:
                target_path, gold_metrics = self.loader.write_target(
                    df,
                    meta,
                    merge_key=merge_key_val,
                    before_target_dml=lambda: logger.renew_active_lease_or_raise(log_id),
                )
            if logger and log_id and gold_metrics:
                logger.update_stage_metrics(log_id, {"gold_sub_stages": gold_metrics})

            # RECONCILIATION & AUDIT TELEMETRY
            window = getattr(meta, "resolved_cdc_window", None)
            contract = getattr(meta, "table_contract", None)
            recon_outcome = "CONVERGED"
            target_persisted_rows = int((gold_metrics or {}).get("target_persisted_rows", 0))
            if window and contract and getattr(contract, "cdc_policy", None):
                try:
                    from src.validation.strategy_reconciler import StrategyAwareReconciler
                    from src.metadata.policy_models import compute_deterministic_fingerprint

                    target_schema = getattr(target_cfg, "schema", None) or meta.target_schema
                    if not target_schema:
                        raise RuntimeError(
                            f"TARGET_SCHEMA_REQUIRED: {meta.target_tag} has no configured target schema."
                        )
                    target_table = meta.target_table or meta.source_table
                    target_evidence = (gold_metrics or {}).get("target_reconciliation")
                    if not target_evidence:
                        raise RuntimeError(
                            "TARGET_RECONCILIATION_MISSING: UPSERT returned no physical target evidence."
                        )

                    current_attempt = int(logger._attempt_numbers.get(log_id, 1)) if logger and log_id else 1
                    evidence_fingerprint = compute_deterministic_fingerprint({
                        "proof_mode": target_evidence.get("proof_mode"),
                        "staging_table": target_evidence.get("staging_table"),
                        "run_id": str(run_id),
                        "table_id": meta.id,
                        "attempt_number": current_attempt,
                        "window_id": str(window.window_id),
                        "expected_row_count": dedup_count,
                    })
                    reconciler = StrategyAwareReconciler(self.conn_manager)
                    manifest = reconciler.freeze_persistence_manifest(
                        run_id=run_id,
                        table_id=meta.id,
                        attempt_number=current_attempt,
                        contract=contract,
                        window=window,
                        storage_layer="target",
                        storage_object_identity=f"{target_schema}.{target_table}",
                        expected_keys=None,
                        schema_fingerprint=getattr(contract, "contract_fingerprint", "fp"),
                        expected_row_count=dedup_count,
                        expected_key_fingerprint=evidence_fingerprint,
                    )
                    recon_res = reconciler.reconcile_staged_target_convergence(
                        run_id=run_id,
                        table_id=meta.id,
                        attempt_number=current_attempt,
                        contract=contract,
                        window=window,
                        manifest=manifest,
                        target_evidence=target_evidence,
                    )
                    meta.cdc_reconciliation_id = recon_res.reconciliation_id
                    recon_outcome = recon_res.outcome.value if hasattr(recon_res.outcome, "value") else str(recon_res.outcome)
                except Exception as e:
                    recon_outcome = "REJECTED"
                    if logger and log_id and window:
                        cdc_metrics = {
                            "rendered_filter": cdc_filter,
                            "lower_cursor": list(window.lower_cursor.values) if window.lower_cursor else None,
                            "upper_cursor": list(window.upper_cursor.values) if window.upper_cursor else None,
                            "overlap_days": meta.cdc_p_days,
                            "source_fetched_rows": source_count,
                            "target_persisted_rows": target_persisted_rows,
                            "reconciliation_outcome": recon_outcome,
                        }
                        logger.update_stage_metrics(log_id, {"cdc": cdc_metrics})
                    raise

            if logger and log_id and window:
                cdc_metrics = {
                    "rendered_filter": cdc_filter,
                    "lower_cursor": list(window.lower_cursor.values) if window.lower_cursor else None,
                    "upper_cursor": list(window.upper_cursor.values) if window.upper_cursor else None,
                    "overlap_days": meta.cdc_p_days,
                    "source_fetched_rows": source_count,
                    "target_persisted_rows": target_persisted_rows,
                    "reconciliation_outcome": recon_outcome,
                }
                logger.update_stage_metrics(log_id, {"cdc": cdc_metrics})

        else:
            # ── Persisted Silver path (used by full and Gold-only runs) ──

            if use_persisted_silver:
                if logger and log_id:
                    logger.update_phase(log_id, "SILVER_READ")
                log.info(f"[{meta.source_table}] Starting SILVER_READ for Gold; source will not be queried")
                if silver_artifact is not None:
                    if str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
                        self._restore_persisted_cdc_context(meta, silver_artifact)
                    df = self._read_verified_artifact(meta, silver_artifact)
                else:
                    df = self.extractor.read_history(meta)
                if silver_artifact_run_id and silver_artifact is None:
                    if "pipeline_run_id" not in df.columns:
                        raise RuntimeError(
                            "GOLD_LAYER_ARTIFACT_IDENTITY_MISSING: persisted Silver artifact "
                            "has no pipeline_run_id column."
                        )
                    df = df.filter(df.pipeline_run_id == str(silver_artifact_run_id))
                notes = "Reused persisted Silver artifact"
            else:
                if logger and log_id:
                    logger.update_phase(log_id, "EXTRACTING")
                log.info(
                    "[%s] Starting legacy Gold source read (full snapshot); "
                    "governed execution must supply persisted Silver.",
                    meta.source_table,
                )
                df = self.extractor.read(meta, cdc_filter=None)
                self._freeze_schema_if_identity(meta, df)
                transformer = Transformer(run_id)
                df_clean = transformer.sanitize_business_keys(df, meta)
                df_key = transformer.add_source_identification_key(df_clean, meta)
                df_dedup = transformer.deduplicate(df_key, meta)
                df_hash = transformer.add_business_hash(df_dedup, meta)
                df = transformer.add_audit_columns(df_hash, meta)
                df = transformer.cast_columns(df, meta)
                df = transformer.select_target_columns(df, meta)
                notes = "Legacy Gold source transformation"

            source_count = Reconciler.count_and_cache(df)
            dedup_count = source_count
            SchemaValidator.validate_layer_contract(df, "GOLD", meta.source_table)

            # TARGET RESOLUTION, CONNECTIVITY & PRE-FLIGHT
            if logger and log_id:
                logger.update_phase(log_id, "RESOLVING_TARGET", extracted_count=source_count, source_count=source_count)
            target_cfg = self.conn_manager.get(meta.target_tag, expected_role="target")

            if logger and log_id:
                logger.update_phase(log_id, "CONNECTING_TARGET", extracted_count=source_count, source_count=source_count)

            if logger and log_id:
                logger.update_phase(log_id, "TARGET_PREFLIGHT", extracted_count=source_count, source_count=source_count)

            # GOLD WRITE
            if str(getattr(meta, "landing_mode", "METADATA")).upper() == "DIRECT":
                if logger and log_id:
                    logger.update_phase(log_id, "GOLD_WRITE", extracted_count=source_count, processed_count=0, source_count=source_count)
                log.info(f"[{meta.source_table}] Starting GOLD_WRITE: {dedup_count:,} rows → target (DIRECT OVERWRITE)")
                target_path = self.loader.write_target_overwrite(
                    df,
                    meta,
                    before_target_dml=lambda: logger.renew_active_lease_or_raise(log_id),
                )
                gold_metrics = {"target_persisted_rows": source_count}
                if logger and log_id:
                    logger.update_stage_metrics(log_id, {
                        "action": "DIRECT_OVERWRITE",
                        "source_deletions_propagated": True,
                    })
            elif meta.canonical_source_business_key:
                # FULL_SOURCE_UPSERT: Keyed UPSERT without TRUNCATE (inserts and updates only; no source-delete propagation).
                if logger and log_id:
                    logger.update_phase(log_id, "GOLD_WRITE", extracted_count=source_count, processed_count=0, source_count=source_count)
                log.info(f"[{meta.source_table}] Starting GOLD_WRITE: {dedup_count:,} rows → target (FULL_SOURCE_UPSERT)")
                merge_key_val = ",".join(meta.canonical_source_business_key)
                conditional_key, exception_values = self._conditional_identity_parts(meta)
                if conditional_key:
                    log.info("[%s] Starting GOLD_WRITE: %s rows → target (BOUNDED_EXCEPTION_REPLACE)", meta.source_table, dedup_count)
                    target_path, gold_metrics = self.loader.write_target_bounded_exception(
                        df,
                        meta,
                        merge_key=merge_key_val,
                        exception_values=exception_values,
                        before_target_dml=lambda: logger.renew_active_lease_or_raise(log_id),
                    )
                    action = "BOUNDED_EXCEPTION_REPLACE"
                else:
                    log.info("[%s] Starting GOLD_WRITE: %s rows → target (FULL_SOURCE_UPSERT)", meta.source_table, dedup_count)
                    target_path, gold_metrics = self.loader.write_target(
                        df,
                        meta,
                        merge_key=merge_key_val,
                        before_target_dml=lambda: logger.renew_active_lease_or_raise(log_id),
                    )
                    action = "FULL_SOURCE_UPSERT"
                if logger and log_id:
                    logger.update_stage_metrics(log_id, {
                        "action": action,
                        "source_deletions_propagated": False,
                    })
            else:
                # FULL_SNAPSHOT_OVERWRITE: Exact target replacement for keyless/explicit overwrite tables.
                if logger and log_id:
                    logger.update_phase(log_id, "GOLD_WRITE", extracted_count=source_count, processed_count=0, source_count=source_count)
                log.info(f"[{meta.source_table}] Starting GOLD_WRITE: {dedup_count:,} rows → target (FULL_SNAPSHOT_OVERWRITE)")
                target_path = self.loader.write_target_overwrite(
                    df,
                    meta,
                    before_target_dml=lambda: logger.renew_active_lease_or_raise(log_id),
                )
                if logger and log_id:
                    logger.update_stage_metrics(log_id, {
                        "action": "FULL_SNAPSHOT_OVERWRITE",
                        "source_deletions_propagated": True,
                    })

            if meta.is_cdc == "y" and silver_artifact is not None and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
                self._reconcile_persisted_cdc_target(
                    meta,
                    run_id=run_id,
                    dedup_count=dedup_count,
                    target_cfg=target_cfg,
                    gold_metrics=gold_metrics,
                )

        # Target count verification
        if target_path.startswith("jdbc:"):
            db_table = target_path[5:]
            target_connection = self.conn_manager.get(meta.target_tag, expected_role="target")
            target_cfg = self.conn_manager.resolve_jdbc(target_connection, ExecutionContext.SPARK)
            target_df = (
                apply_jdbc_options(self.spark.read.format("jdbc"), target_cfg)
                .option("dbtable", quote_table_name(db_table, target_cfg.db_type))
                .load()
            )
            if "pipeline_run_id" in target_df.columns:
                target_count = target_df.filter(
                    target_df.pipeline_run_id == (silver_artifact_run_id or run_id)
                ).count()
            else:
                target_count = target_df.count()
        else:
            try:
                target_count = Reconciler.count_delta(self.spark, target_path, run_id=run_id)
            except Exception:
                target_count = Reconciler.count_parquet(self.spark, target_path, run_id=run_id)
        if target_count != source_count and (meta.is_cdc or "").lower() != "y":
            log.warning(
                "[%s] Gold source drift detected during full snapshot write: "
                "pre_write_count=%s, persisted_target_count=%s. Using persisted "
                "target count for Gold reconciliation.",
                meta.source_table,
                source_count,
                target_count,
            )
            source_count = target_count
            expected_loss = 0
        log.info(f"[{meta.source_table}] Target count verified: {target_count} rows")
        reconcile_target_count = target_count
        if (meta.is_cdc or "").lower() == "y" and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
            # A JDBC target count is cumulative.  Progress/reconciliation must
            # report only rows accepted from this frozen CDC artifact.
            reconcile_target_count = int((gold_metrics or {}).get("target_persisted_rows", 0)) or int(dedup_count)
        processed_rows = None
        if (meta.is_cdc or "").lower() == "y" and not use_persisted_silver and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
            # Generic row-count reconciliation must compare the current CDC
            # batch, not the cumulative target table.
            processed_rows = int((gold_metrics or {}).get("target_persisted_rows", 0))
            if processed_rows <= 0:
                processed_rows = int((gold_metrics or {}).get("rows", 0)) or int(dedup_count)
            reconcile_target_count = processed_rows
        if logger and log_id:
            if (meta.is_cdc or "").lower() == "y" and not use_persisted_silver and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
                # The JDBC verification count may be the complete target when
                # audit columns are unavailable.  Use the rows accepted by
                # the successful staging/merge operation for this run.
                # Reuse the current-batch count calculated above.
                processed_rows = reconcile_target_count
            else:
                processed_rows = target_count
            logger.update_chunk_progress(
                log_id,
                actual_chunk_rows=processed_rows,
                chunk_number=1,
                total_chunks=1,
                stage="GOLD_FINALIZE",
            )
        if (meta.is_cdc or "").lower() == "y" and str(getattr(meta, "landing_mode", "METADATA")).upper() != "DIRECT":
            if silver_artifact is not None:
                self._commit_governed_cdc_window(
                    meta,
                    run_id,
                    log_id,
                    expected_state_version=getattr(silver_artifact, "cdc_state_version", None),
                )
            else:
                # Preserve the legacy call shape for direct CDC runs.  No
                # upstream artifact means there is no frozen producer state
                # version to compare, so the committer's existing behavior is
                # unchanged.
                self._commit_governed_cdc_window(meta, run_id, log_id)
        activation_note = self._activate_bounded_exception_after_bootstrap(
            meta,
            gold_metrics=gold_metrics,
            logger=logger,
            log_id=log_id,
        )
        if activation_note:
            notes = f"{notes}; {activation_note}" if notes else activation_note
        return source_count, reconcile_target_count, notes, expected_loss
