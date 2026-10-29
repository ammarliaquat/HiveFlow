from types import SimpleNamespace
from uuid import uuid4

import pytest
from unittest.mock import MagicMock

from src.constants import ExecutionScope, LayerName
from src.contracts.claim_models import FrozenArtifactReference, FrozenJobExecution, FrozenTableEntry
from src.orchestrator import etl as etl_module
from src.orchestrator.etl import ETL


def _frozen_table():
    return FrozenTableEntry(
        table_id=120,
        source_tag="source_a",
        source_table="orders",
        target_tag="land_test",
        target_schema="landing",
        target_table="orders",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="policy-1",
        policy_fingerprint="p" * 64,
        column_mapping_version_id="mapping-1",
        column_mapping_fingerprint="m" * 64,
        business_key_version_id="key-1",
        business_key_fingerprint="k" * 64,
        business_key_columns=["id"],
        deployment_profile_fingerprint="d" * 64,
    )


def _silver_artifact():
    return FrozenArtifactReference(
        artifact_id="artifact-1",
        producer_run_id="silver-run",
        producer_attempt_number=1,
        storage_layer="SILVER",
        storage_uri="file:///silver/artifact-1",
        storage_format="parquet",
        row_count=1,
        policy_version_id="policy-1",
        policy_fingerprint="p" * 64,
        mapping_fingerprint="m" * 64,
        source_key_fingerprint="k" * 64,
        schema_fingerprint="s" * 64,
        artifact_fingerprint="a" * 64,
    )


def test_execution_scope_is_frozen_and_round_trips_in_contract():
    run_id = str(uuid4())
    contract = FrozenJobExecution.build(
        run_id=run_id,
        action="run_governed",
        deployment_profile_fingerprint="d" * 64,
        tables=[_frozen_table()],
        execution_scope=ExecutionScope.SILVER,
    )

    restored = FrozenJobExecution.from_dict(contract.to_dict())

    assert restored.execution_scope == ExecutionScope.SILVER
    assert restored.parent_contract_fingerprint == contract.parent_contract_fingerprint


def test_execution_scope_rejects_unknown_values():
    with pytest.raises(ValueError, match="invalid execution_scope"):
        FrozenJobExecution.build(
            run_id=str(uuid4()),
            action="run_governed",
            deployment_profile_fingerprint="d" * 64,
            tables=[_frozen_table()],
            execution_scope="LANDING_ONLY",
        )


@pytest.mark.parametrize(
    "scope,expected_layers",
    [
        (ExecutionScope.BRONZE, [LayerName.BRONZE]),
        (ExecutionScope.SILVER, [LayerName.SILVER]),
        (ExecutionScope.GOLD, [LayerName.GOLD]),
    ],
)
def test_partial_execution_scope_runs_only_requested_layer(monkeypatch, scope, expected_layers):
    class FakeLogger:
        def __init__(self, *args, **kwargs):
            pass

        def log_start(self, **kwargs):
            return 1

        def log(self, **kwargs):
            return None

    monkeypatch.setattr(etl_module, "BatchLogger", FakeLogger)

    etl = ETL.__new__(ETL)
    etl.spark_app_id = "spark-test"
    etl.conn_manager = object()
    meta = SimpleNamespace(
        id=120,
        source_tag="source_a",
        source_table="orders",
        target_tag="land_test",
        target_table="orders",
        priority=1,
        is_cdc="y" if scope == ExecutionScope.GOLD else "n",
    )
    etl._metadata_row_from_frozen_table = lambda frozen_table: meta
    etl._register_cancellation_guards = lambda logger: None
    etl._clear_cancellation_guards = lambda: None
    etl._assert_bronze_artifact_exists = lambda current_meta: None
    frozen_table = _frozen_table()
    if scope == ExecutionScope.GOLD:
        frozen_table.upstream_artifact = _silver_artifact()
        meta.upstream_artifact = frozen_table.upstream_artifact
    etl._resolve_latest_verified_silver_run_id = lambda current_meta: (_ for _ in ()).throw(
        AssertionError("governed execution must not resolve latest Silver")
    )
    etl._process_bronze_table = lambda *args, **kwargs: (1, 1, "", 0)
    etl._process_silver_table = lambda *args, **kwargs: (1, 1, "", 0)
    gold_calls = []
    etl._process_gold_table = lambda *args, **kwargs: gold_calls.append(kwargs) or (1, 1, "", 0)
    calls = []

    def fake_batch(tables, layer_name, target_tag, logger, worker_fn, on_success=None):
        calls.append(layer_name)
        worker_fn(tables[0], 1, logger)
        return [tables[0]]

    etl._execute_table_batch = fake_batch

    result = etl.run_frozen_table(
        frozen_table=frozen_table,
        run_id=str(uuid4()),
        execution_scope=scope,
    )

    assert result == [meta]
    assert calls == expected_layers
    if scope == ExecutionScope.GOLD:
        assert gold_calls[0]["cdc_window_run_id"] is None
        assert gold_calls[0]["silver_artifact_run_id"] == "silver-run"
        assert gold_calls[0]["use_persisted_silver"] is True


def test_gold_prerequisite_failure_records_skipped_upstream_layers_first():
    etl = ETL.__new__(ETL)
    etl.spark_app_id = "spark-test"
    etl.conn_manager = object()
    meta = SimpleNamespace(
        id=120,
        source_tag="source_a",
        source_table="orders",
        target_tag="land_test",
        target_table="orders",
        priority=1,
        is_cdc="n",
    )
    etl._metadata_row_from_frozen_table = lambda frozen_table: meta
    etl._register_cancellation_guards = lambda logger: None
    etl._clear_cancellation_guards = lambda: None
    recorded = []
    etl._log_not_requested = lambda current_meta, layer, run_id, logger: recorded.append(layer)
    with pytest.raises(RuntimeError, match="GOVERNED_ARTIFACT_REFERENCE_REQUIRED"):
        etl.run_frozen_table(
            frozen_table=_frozen_table(),
            run_id=str(uuid4()),
            execution_scope=ExecutionScope.GOLD,
        )

    assert recorded == [LayerName.BRONZE, LayerName.SILVER]


def test_governed_worker_reconstructs_mappings_from_frozen_payload():
    frozen_table = _frozen_table()
    frozen_table.contract_payload = {
        "mapping_policy": "EXPLICIT",
        "mappings": [{
            "column_id": 1,
            "column_sequence": 1,
            "source_column": "CUSTOMER_ID",
            "target_column": "customer_id",
            "data_type": "BIGINT",
            "source_column_datatype": "NUMBER",
            "canonical_type": "INT64",
            "transform_expr": None,
        }],
    }
    from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings

    frozen_table.column_mapping_fingerprint = ResolvedTableMappings.build(
        table_id=frozen_table.table_id,
        raw_mappings=frozen_table.contract_payload["mappings"],
        mapping_policy=MappingPolicy.EXPLICIT,
    ).fingerprint

    etl = ETL.__new__(ETL)
    meta = etl._metadata_row_from_frozen_table(frozen_table)

    assert meta.resolved_mappings.fingerprint == frozen_table.column_mapping_fingerprint
    assert meta.resolved_mappings.mappings[0].target_column == "customer_id"


def test_gold_scope_consumes_persisted_silver_for_cdc_without_source_read(monkeypatch):
    etl = ETL.__new__(ETL)
    etl.conn_manager = MagicMock()
    etl.spark = MagicMock()
    etl.extractor = MagicMock()
    etl.extractor.read.side_effect = AssertionError("Gold-only must not read the source")
    persisted_silver = MagicMock()
    persisted_silver.columns = ["pipeline_run_id"]
    persisted_silver.filter.return_value = persisted_silver
    etl.extractor.read_history.return_value = persisted_silver
    etl.loader = MagicMock()
    etl.loader.write_target.return_value = ("file:///target", {"target_persisted_rows": 3})
    meta = SimpleNamespace(
        id=120,
        source_tag="source_a",
        source_table="orders",
        target_tag="land_test",
        target_table="orders",
        target_schema="landing",
        is_cdc="y",
        canonical_source_business_key=("id",),
    )
    etl.conn_manager.get.return_value = SimpleNamespace(schema="landing")

    monkeypatch.setattr(etl_module, "Transformer", lambda _run_id: MagicMock())
    monkeypatch.setattr(etl_module.SchemaValidator, "validate_layer_contract", lambda *_args: None)
    monkeypatch.setattr(etl_module.Reconciler, "count_and_cache", lambda _df: 3)
    monkeypatch.setattr(etl_module.Reconciler, "count_delta", lambda *_args, **_kwargs: 3)

    result = etl._process_gold_table(
        meta,
        run_id="gold-run",
        log_id=None,
        logger=None,
        silver_artifact_run_id="silver-run",
        use_persisted_silver=True,
    )

    assert result[0] == 3
    etl.extractor.read.assert_not_called()
    etl.extractor.read_history.assert_called_once_with(meta)
    etl.loader.write_target.assert_called_once()
