from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.constants import LayerName
from src.exceptions import (
    BusinessKeyRequiredError,
    CDCPolicyNotMigratedError,
    ExecutionOwnershipRequiredError,
)
from src.load.loader import Loader, TargetResolutionError
from src.load.target_schema_inspector import GenericSchemaInspector
from src.logging_.batch_logger import ExecutionLeaseLostError
from src.metadata.metadata_reader import MetadataReader
from src.metadata.metadata_models import MetadataRow, MappingPolicy, ResolvedTableMappings
from src.orchestrator.etl import ETL
from src.utils.db_utils import execute_upsert


def _meta(**kwargs):
    d = {
        "id": 17,
        "source_tag": "source-a",
        "source_schema": "public",
        "source_table": "events",
        "source_db_type": "postgres",
        "target_tag": "target-a",
        "target_table": "events",
        "target_schema": "landing",
        "target_database_type": "postgres",
        "landing_table": "events",
        "history_table": "events",
        "priority": 1,
        "is_active": "y",
        "load_type": "full",
        "is_cdc": "n",
        "cdc_strategy": "TIMESTAMP",
        "cdc_columns": None,
        "cdc_p_days": 0,
        "delta_days": 0,
        "last_success_timestamp": None,
        "key_source": "PRIMARY_KEY",
        "key_confidence": 100,
        "natural_key_column": "id",
        "is_reconcile": "n",
        "reconcile_column": None,
        "reconcile_measure": "COUNT",
        "reconcile_threshold": 0.0,
        "reconcile_action": "FAIL",
        "chunk_size": 1000,
        "num_partitions": 1,
        "partition_column": None,
        "source_key_prefix": None,
        "resolved_mappings": ResolvedTableMappings.build(1, [], MappingPolicy.IDENTITY),
        "mapping_policy": "IDENTITY",
        "key_candidate_nullable": False,
        "key_candidate_compatible": True,
        "key_approval_state": "APPROVED",
        "approved_source_business_key": ["id"],
        "approved_key_evidence": "PRIMARY_KEY",
        "approved_key_evidence_fingerprint": "fingerprint",
        "current_key_evidence_fingerprint": "fingerprint",
        "key_approved_by": "test",
        "key_approved_at": __import__("datetime").datetime.now(),
        "key_approval_revision": 1,
    }
    d.update(kwargs)
    if d.get("key_source") != "PRIMARY_KEY":
        d["key_approval_state"] = "PENDING"
    key_columns = d.pop("key_columns", None)
    if key_columns:
        d["natural_key_column"] = key_columns
    known_fields = set(MetadataRow.__dataclass_fields__)
    row = MetadataRow(**{key: value for key, value in d.items() if key in known_fields})
    for key, value in d.items():
        if key not in known_fields:
            setattr(row, key, value)
    return row


def _etl_for_batch():
    etl = ETL.__new__(ETL)
    etl.validate_metadata = MagicMock()
    etl.metadata_reader = MagicMock()
    etl.current_action = "run_pipeline"
    lock_cursor = MagicMock()
    lock_cursor.fetchone.return_value = (True,)
    lock_connection = MagicMock()
    lock_connection.cursor.return_value = lock_cursor
    etl.conn_manager = MagicMock()
    etl.conn_manager.get_raw_metadata_db_connection.return_value = lock_connection
    etl.column_mapper = MagicMock()
    return etl


@patch("src.orchestrator.etl.LineageService")
def test_missing_batch_log_ownership_blocks_worker_persistence(mock_lineage):
    meta = _meta()
    mock_lineage.return_value.build_execution_dag.return_value = [meta]
    mock_lineage.return_value.check_dependencies_status.return_value = "READY"
    etl = _etl_for_batch()
    logger = MagicMock()
    logger.log_start.return_value = None
    worker = MagicMock(return_value=(1, 1))

    etl._execute_table_batch([meta], LayerName.BRONZE, meta.source_tag, logger, worker)

    worker.assert_not_called()
    logger.log.assert_not_called()
    etl.metadata_reader.update_table_success.assert_not_called()
    etl.metadata_reader.update_table_failure.assert_not_called()


@patch("src.orchestrator.etl.LineageService")
def test_legacy_incremental_cdc_policy_blocks_all_layer_writes(mock_lineage):
    meta = _meta(is_cdc="y")
    mock_lineage.return_value.build_execution_dag.return_value = [meta]
    mock_lineage.return_value.check_dependencies_status.return_value = "READY"
    etl = _etl_for_batch()
    logger = MagicMock()
    logger.log_start.return_value = 91
    worker = MagicMock(side_effect=CDCPolicyNotMigratedError("CDC_POLICY_NOT_MIGRATED"))

    etl._execute_table_batch([meta], LayerName.BRONZE, meta.source_tag, logger, worker)

    worker.assert_called_once()
    assert "CDC_POLICY_NOT_MIGRATED" in logger.log.call_args.kwargs["error_message"]


@patch("src.load.target_schema_inspector.get_schema_inspector")
@patch("src.utils.db_utils.execute_ddl")
@patch("src.utils.db_utils.apply_jdbc_options")
def test_profiled_key_cannot_provision_gold_constraint(
    mock_apply_jdbc_options, mock_execute_ddl, mock_inspector_factory
):
    loader = Loader.__new__(Loader)
    loader.spark = MagicMock()
    mock_apply_jdbc_options.return_value.option.return_value.load.return_value = MagicMock()
    mock_inspector_factory.return_value.supports_unique_key.return_value = False
    meta = _meta(is_cdc="y", key_source="PROFILED")
    target_cfg = SimpleNamespace(schema="landing", db_type="postgres")

    with pytest.raises(BusinessKeyRequiredError, match="BUSINESS_KEY_REQUIRED"):
        loader.provision_target_schema_preflight(
            None, meta, target_cfg, "id", before_persist=lambda: None
        )

    mock_execute_ddl.assert_not_called()


@patch("src.load.target_schema_inspector.get_schema_inspector")
@patch("src.utils.db_utils.execute_ddl")
@patch("src.utils.db_utils.apply_jdbc_options")
def test_full_snapshot_without_trusted_key_never_provisions_a_constraint(
    mock_apply_jdbc_options, mock_execute_ddl, mock_inspector_factory
):
    loader = Loader.__new__(Loader)
    loader.spark = MagicMock()
    mock_apply_jdbc_options.return_value.option.return_value.load.return_value = MagicMock()
    meta = _meta(is_cdc="n", key_source="PROFILED", key_columns="email")
    target_cfg = SimpleNamespace(schema="landing", db_type="postgres")

    loader.provision_target_schema_preflight(
        None, meta, target_cfg, "email", before_persist=lambda: None
    )

    mock_inspector_factory.assert_not_called()
    mock_execute_ddl.assert_not_called()


@patch("src.orchestrator.etl.BatchLogger")
def test_stage0_target_execution_has_no_legacy_watermark_callback(mock_batch_logger):
    etl = ETL.__new__(ETL)
    etl.conn_manager = MagicMock()
    etl.metadata_reader = MagicMock()
    etl.metadata_reader.get_gold_tables.return_value = [_meta()]
    etl.spark_app_id = "app"
    etl._register_cancellation_guards = MagicMock()
    etl._clear_cancellation_guards = MagicMock()
    etl._execute_table_batch = MagicMock(return_value=["events"])
    etl.current_action = "run_target"

    etl.run_target_cdc("source-a")

    on_success = etl._execute_table_batch.call_args.args[5]
    assert on_success is None
    etl.metadata_reader.update_watermark.assert_not_called()


@pytest.mark.parametrize(
    ("write_method", "args"),
    [
        ("write_landing", ("2026-08-19",)),
        ("write_history", ()),
        ("write_target_overwrite", ()),
    ],
)
@patch("src.load.loader.SchemaValidator.validate_dataframe")
def test_persistence_boundaries_require_an_active_batch_lease(
    mock_validate_dataframe, write_method, args
):
    loader = Loader.__new__(Loader)
    loader.paths = {
        "landing_base": "landing",
        "history_base": "history",
        "target_base": "target",
    }
    loader.conn_manager = MagicMock()
    loader.conn_manager._raw_config = {}
    loader.conn_manager.get.side_effect = KeyError("no JDBC target")
    loader._delta_available = False
    df = MagicMock()

    with pytest.raises(
        ExecutionOwnershipRequiredError,
        match="EXECUTION_OWNERSHIP_REQUIRED",
    ):
        getattr(loader, write_method)(df, _meta(), *args)

    df.write.mode.assert_not_called()


def test_untrusted_silver_key_uses_overwrite_not_append():
    loader = Loader.__new__(Loader)
    loader.paths = {"history_base": "history"}
    loader._delta_available = False
    df = MagicMock()

    loader.write_history(df, _meta(key_source="PROFILED"), before_persist=lambda: None)

    df.write.mode.assert_called_once_with("overwrite")


def test_governed_silver_artifact_is_a_sidecar_not_a_history_child():
    """A run artifact must never make the history table unreadable as Parquet."""
    loader = Loader.__new__(Loader)
    loader.paths = {"history_base": "history"}
    loader._delta_available = False
    df = MagicMock()

    history_path, artifact_path = loader.write_history(
        df,
        _meta(),
        before_persist=lambda: None,
        run_id="run-123",
        return_artifact=True,
    )

    assert history_path == "history/source-a/events"
    assert artifact_path == "history/artifacts/source-a/events/run_run-123"
    assert not artifact_path.startswith(f"{history_path}/")
    df.write.mode.return_value.format.return_value.save.assert_called_once_with(artifact_path)


def test_direct_preflight_requires_ownership_before_target_probe():
    loader = Loader.__new__(Loader)
    loader.spark = MagicMock()

    with pytest.raises(ExecutionOwnershipRequiredError):
        loader.provision_target_schema_preflight(
            None,
            _meta(),
            SimpleNamespace(schema="landing", db_type="postgres"),
            None,
        )

    loader.spark.read.format.assert_not_called()


def test_execute_upsert_requires_ownership_before_any_spark_action():
    df = MagicMock()

    with pytest.raises(ExecutionOwnershipRequiredError):
        execute_upsert(
            spark=MagicMock(),
            df=df,
            target_cfg={},
            target_table="landing.events",
            merge_key="id",
            columns=["id"],
            db_type="postgres",
        )

    df.count.assert_not_called()


def test_generic_constraint_inspection_fails_closed():
    with pytest.raises(NotImplementedError):
        GenericSchemaInspector().supports_unique_key(
            "landing",
            "events",
            ("id",),
            SimpleNamespace(db_type="mysql"),
        )


def test_metadata_reader_legacy_watermark_api_is_disabled():
    reader = object.__new__(MetadataReader)

    with pytest.raises(CDCPolicyNotMigratedError, match="CDC_POLICY_NOT_MIGRATED"):
        reader.update_watermark(17, "2026-08-19 00:00:00")


@pytest.mark.parametrize("write_method,args", [("write_landing", ("2026-08-19",)), ("write_history", ())])
def test_direct_bronze_and_silver_writes_reject_legacy_cdc(write_method, args):
    loader = Loader.__new__(Loader)
    loader.paths = {"landing_base": "landing", "history_base": "history"}
    loader.conn_manager = MagicMock()
    loader.conn_manager._raw_config = {}
    loader._delta_available = False
    df = MagicMock()

    with pytest.raises(CDCPolicyNotMigratedError):
        getattr(loader, write_method)(
            df,
            _meta(is_cdc="y"),
            *args,
            before_persist=lambda: None,
        )

    df.write.mode.assert_not_called()


def test_keyed_gold_merge_uses_staging_upsert_with_active_lease():
    loader = Loader.__new__(Loader)
    loader.conn_manager = MagicMock()
    target_cfg = SimpleNamespace(schema="landing", db_type="postgres", url="jdbc:postgresql://db/opf", username="u", password="p", driver="org.postgresql.Driver")
    loader.conn_manager.get.return_value = target_cfg
    loader._upsert_via_staging = MagicMock(return_value=("jdbc:landing.events", {"rows": 1}))
    df = MagicMock()

    result = loader.write_target(
        df,
        _meta(is_cdc="y"),
        merge_key="id",
        before_target_dml=lambda: None,
    )

    assert result == ("jdbc:landing.events", {"rows": 1})
    loader._upsert_via_staging.assert_called_once()


def test_direct_gold_delta_merge_is_frozen():
    loader = Loader.__new__(Loader)

    with pytest.raises(CDCPolicyNotMigratedError):
        loader._write_target_delta_merge(
            MagicMock(),
            _meta(is_cdc="n", key_source="PROFILED"),
            merge_key="id",
            before_target_dml=lambda: None,
        )


@patch("src.utils.db_utils._collect_postgres_merge_stats", return_value=None)
@patch(
    "src.utils.db_utils._verify_staging_target_convergence",
    return_value={
        "proof_mode": "STAGING_TARGET_LEFT_JOIN",
        "staged_rows": 1,
        "matched_rows": 1,
        "missing_rows": 0,
        "expected_rows": 1,
    },
)
@patch("src.utils.db_utils.safe_execute_jdbc")
@patch("src.utils.db_utils.apply_jdbc_options")
def test_internal_upsert_executes_with_active_lease(
    mock_apply, mock_safe_execute, mock_reconciliation, _mock_stats
):
    writer = MagicMock()
    mock_apply.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    df = MagicMock()
    df.count.return_value = 1

    metrics = execute_upsert(
        spark=MagicMock(),
        df=df,
        target_cfg={},
        target_table="landing.events",
        merge_key="id",
        columns=["id"],
        db_type="postgres",
        before_target_dml=lambda: None,
    )

    assert metrics["rows"] == 1
    assert metrics["target_persisted_rows"] == 1
    assert metrics["target_reconciliation"]["missing_rows"] == 0
    mock_reconciliation.assert_called_once()
    writer.save.assert_called_once()
    assert mock_safe_execute.call_count >= 1


@patch("src.utils.db_utils._collect_postgres_merge_stats", return_value=None)
@patch(
    "src.utils.db_utils._verify_staging_target_convergence",
    return_value={
        "proof_mode": "STAGING_TARGET_LEFT_JOIN",
        "staged_rows": 1,
        "matched_rows": 1,
        "missing_rows": 0,
        "expected_rows": 1,
    },
)
@patch("src.utils.db_utils.safe_execute_jdbc")
@patch("src.utils.db_utils.apply_jdbc_options")
def test_internal_upsert_keeps_spark_and_driver_target_routes_separate(
    mock_apply, mock_safe_execute, _mock_reconciliation, mock_stats
):
    """Spark writes use the container route; DBAPI merge SQL uses the driver route."""
    writer = MagicMock()
    mock_apply.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    df = MagicMock()
    df.count.return_value = 1
    spark_cfg = {
        "url": "jdbc:postgresql://PGDBBI:5432/land_test",
        "user": "etl",
        "password": "secret",
        "driver": "org.postgresql.Driver",
    }
    driver_cfg = {
        "url": "jdbc:postgresql://192.0.2.1:5432/land_test",
        "user": "etl",
        "password": "secret",
        "driver": "org.postgresql.Driver",
    }

    execute_upsert(
        spark=MagicMock(),
        df=df,
        target_cfg=spark_cfg,
        driver_target_cfg=driver_cfg,
        target_table="landing.events",
        merge_key="id",
        columns=["id"],
        db_type="postgres",
        before_target_dml=lambda: None,
    )

    assert mock_apply.call_args.args[1] is spark_cfg
    assert all(call.args[0] is driver_cfg for call in mock_stats.call_args_list)
    control_urls = [call.kwargs["target_cfg"]["url"] for call in mock_safe_execute.call_args_list]
    assert control_urls
    assert set(control_urls) == {driver_cfg["url"]}


@patch("src.orchestrator.etl.LineageService")
def test_lock_failure_cannot_enter_worker_or_terminal_logging(mock_lineage):
    meta = _meta()
    mock_lineage.return_value.build_execution_dag.return_value = [meta]
    etl = _etl_for_batch()
    etl.conn_manager.get_raw_metadata_db_connection.side_effect = RuntimeError("metadata down")
    logger = MagicMock()
    worker = MagicMock()

    result = etl._execute_table_batch(
        [meta], LayerName.BRONZE, meta.source_tag, logger, worker
    )

    assert result == []
    worker.assert_not_called()
    logger.log_start.assert_not_called()
    logger.log.assert_not_called()


@patch("src.orchestrator.etl.LineageService")
def test_terminal_fence_precedes_scheduler_success_metadata(mock_lineage):
    meta = _meta()
    mock_lineage.return_value.build_execution_dag.return_value = [meta]
    mock_lineage.return_value.check_dependencies_status.return_value = "READY"
    etl = _etl_for_batch()
    logger = MagicMock()
    logger.log_start.return_value = 91
    events = []
    logger.log.side_effect = lambda **kwargs: events.append("terminal")
    etl.metadata_reader.update_table_success.side_effect = lambda table_id: events.append("scheduler")

    result = etl._execute_table_batch(
        [meta], LayerName.BRONZE, meta.source_tag, logger, MagicMock(return_value=(1, 1))
    )

    assert result == [meta]
    assert events == ["terminal", "scheduler"]


@patch("src.orchestrator.etl.LineageService")
def test_lost_terminal_fence_does_not_advance_scheduler_metadata(mock_lineage):
    meta = _meta()
    mock_lineage.return_value.build_execution_dag.return_value = [meta]
    mock_lineage.return_value.check_dependencies_status.return_value = "READY"
    etl = _etl_for_batch()
    logger = MagicMock()
    logger.log_start.return_value = 91
    logger.log.side_effect = ExecutionLeaseLostError("LEASE_FENCED")

    with pytest.raises(ExecutionLeaseLostError):
        etl._execute_table_batch(
            [meta], LayerName.BRONZE, meta.source_tag, logger, MagicMock(return_value=(1, 1))
        )

    etl.metadata_reader.update_table_success.assert_not_called()


@patch("src.utils.db_utils.apply_jdbc_options")
def test_profiled_snapshot_key_is_not_validated_as_execution_identity(mock_apply):
    loader = Loader.__new__(Loader)
    loader.spark = MagicMock()
    mock_apply.return_value.option.return_value.load.return_value = MagicMock()
    df = MagicMock()

    loader.provision_target_schema_preflight(
        df,
        _meta(is_cdc="n", key_source="PROFILED", key_columns="email"),
        SimpleNamespace(schema="landing", db_type="postgres"),
        "email",
        before_persist=lambda: None,
    )

    df.filter.assert_not_called()
    df.groupBy.assert_not_called()


@patch("src.load.loader.SchemaValidator.validate_dataframe")
def test_invalid_configured_target_never_falls_back_to_filesystem(mock_validate):
    loader = Loader.__new__(Loader)
    loader.conn_manager = MagicMock()
    loader.conn_manager.get.side_effect = KeyError("missing target")
    loader.paths = {"target_base": "target"}
    df = MagicMock()

    with pytest.raises(TargetResolutionError):
        loader.write_target_overwrite(
            df,
            _meta(is_cdc="n"),
            before_target_dml=lambda: None,
        )

    df.write.mode.assert_not_called()


@patch("src.orchestrator.etl.SchemaValidator.validate_layer_contract")
@patch("src.orchestrator.etl.apply_jdbc_options")
@patch("src.orchestrator.etl.Reconciler")
@patch("src.orchestrator.etl.Transformer")
def test_gold_snapshot_reconciles_actual_target_count(
    mock_transformer_cls,
    mock_reconciler,
    mock_apply_jdbc,
    mock_validate_contract,
):
    etl = ETL.__new__(ETL)
    etl.extractor = MagicMock()
    source_df = MagicMock()
    write_df = MagicMock()
    etl.extractor.read.return_value = source_df
    mock_reconciler.count_and_cache.return_value = 100
    transformer = mock_transformer_cls.return_value
    transformer.add_business_hash.return_value = source_df
    transformer.add_audit_columns.return_value = write_df
    etl.column_mapper = MagicMock()
    etl.column_mapper.get_mappings.return_value = []
    etl.conn_manager = MagicMock()
    etl.conn_manager.get.return_value = SimpleNamespace(db_type="postgres")
    etl.loader = MagicMock()
    etl.loader.write_target_overwrite.return_value = "jdbc:landing.events"
    etl.spark = MagicMock()
    target_df = MagicMock(columns=["id"])
    target_df.count.return_value = 70
    mock_apply_jdbc.return_value.option.return_value.load.return_value = target_df

    source_count, target_count, _, _ = etl._process_gold_table(
        _meta(is_cdc="n", key_source="PROFILED"),
        "run-1",
        91,
        MagicMock(),
    )

    assert source_count == 70
    assert target_count == 70
