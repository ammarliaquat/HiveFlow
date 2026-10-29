"""Offline checks for run-scoped layer artifact governance."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from src.constants import compute_artifact_fingerprint, normalize_execution_scope
from src.contracts.claim_models import FrozenArtifactReference
from src.constants import LayerName, RunStatus
from gui.services.execution_service import _assert_artifact_cdc_state
from src.validation.layer_artifacts import (
    dataframe_schema_fingerprint,
    load_verified_artifact,
    publish_verified_artifact,
)
import src.orchestrator.etl as etl_module
from src.orchestrator.etl import ETL


class _Cursor:
    def __init__(self, row):
        self.row = row
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.sql.append((str(statement), params))

    def fetchone(self):
        return self.row


class _SequenceCursor(_Cursor):
    def __init__(self, rows):
        super().__init__(None)
        self.rows = list(rows)

    def fetchone(self):
        return self.rows.pop(0)


class _Connection:
    def __init__(self, cursor):
        self.cursor_obj = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj


def _artifact_row():
    return {
        "manifest_id": "00000000-0000-0000-0000-000000000001",
        "run_id": "00000000-0000-0000-0000-000000000002",
        "attempt_number": 1,
        "storage_layer": "BRONZE",
        "storage_uri": "landing/src/table/run_2",
        "storage_format": "parquet",
        "expected_row_count": 2,
        "policy_version_id": "00000000-0000-0000-0000-000000000003",
        "policy_fingerprint": "p" * 64,
        "mapping_fingerprint": "m" * 64,
        "source_key_fingerprint": "k" * 64,
        "schema_fingerprint": "s" * 64,
        "artifact_fingerprint": "a" * 64,
        "window_id": None,
        "cdc_lower_cursor": None,
        "cdc_upper_cursor": None,
        "cdc_state_version": None,
        "batch_log_id": 99,
        "status": "SUCCESS",
    }


def _artifact(layer: str, producer_run_id: str = "parent-run") -> FrozenArtifactReference:
    return FrozenArtifactReference(
        artifact_id=f"artifact-{layer.lower()}",
        producer_run_id=producer_run_id,
        producer_attempt_number=1,
        storage_layer=layer,
        storage_uri=f"{layer.lower()}/run_{producer_run_id}",
        storage_format="parquet",
        row_count=3,
        policy_version_id="policy",
        policy_fingerprint="p" * 64,
        mapping_fingerprint="m" * 64,
        source_key_fingerprint="k" * 64,
        schema_fingerprint="s" * 64,
        artifact_fingerprint="a" * 64,
    )


class _Schema:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        import json
        return json.dumps(self._payload)


class _DataFrameWithSchema:
    def __init__(self, payload):
        self.schema = _Schema(payload)


def test_artifact_schema_fingerprint_ignores_parquet_unstable_details():
    before_write = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": False, "metadata": {"origin": "oracle"}},
            {"name": "email", "type": "string", "nullable": False, "metadata": {}},
        ],
    }
    after_parquet_read = {
        "type": "struct",
        "fields": [
            {"name": "email", "type": "string", "nullable": True, "metadata": {}},
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
        ],
    }

    assert dataframe_schema_fingerprint(_DataFrameWithSchema(before_write)) == dataframe_schema_fingerprint(
        _DataFrameWithSchema(after_parquet_read)
    )


def test_artifact_schema_fingerprint_rejects_semantic_type_change():
    string_schema = {"type": "struct", "fields": [{"name": "email", "type": "string", "nullable": True}]}
    binary_schema = {"type": "struct", "fields": [{"name": "email", "type": "binary", "nullable": True}]}

    assert dataframe_schema_fingerprint(_DataFrameWithSchema(string_schema)) != dataframe_schema_fingerprint(
        _DataFrameWithSchema(binary_schema)
    )


def test_execution_scope_is_canonical_and_blank_is_rejected():
    assert normalize_execution_scope(None) == "FULL"
    assert normalize_execution_scope(" silver ") == "SILVER"
    with pytest.raises(ValueError, match="must not be empty"):
        normalize_execution_scope("  ")
    with pytest.raises(ValueError, match="invalid execution_scope"):
        normalize_execution_scope("LANDING_ONLY")


def test_frozen_artifact_rejects_missing_producer_run_lineage():
    with pytest.raises(ValueError, match="producer_run_id"):
        FrozenArtifactReference(
            artifact_id="artifact-1",
            producer_run_id="",
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


def test_artifact_reference_is_immutable_and_round_trips():
    artifact = FrozenArtifactReference(
        artifact_id="a",
        producer_run_id="r",
        producer_attempt_number=1,
        storage_layer="BRONZE",
        storage_uri="landing/run_r",
        storage_format="parquet",
        row_count=2,
        policy_version_id="p",
        policy_fingerprint="pf",
        mapping_fingerprint="mf",
        source_key_fingerprint="kf",
        schema_fingerprint="sf",
        artifact_fingerprint="af",
    )
    assert FrozenArtifactReference.from_dict(artifact.to_dict()) == artifact
    with pytest.raises((AttributeError, TypeError)):
        artifact.row_count = 3


def test_artifact_fingerprint_is_deterministic():
    kwargs = dict(
        storage_layer="bronze",
        storage_uri="landing/run_r",
        storage_format="parquet",
        row_count=2,
        schema_fingerprint="s",
        policy_fingerprint="p",
        mapping_fingerprint="m",
        source_key_fingerprint="k",
        cdc_window_id=None,
    )
    assert compute_artifact_fingerprint(**kwargs) == compute_artifact_fingerprint(**kwargs)
    with_window = dict(kwargs)
    with_window["cdc_window_id"] = "window"
    assert compute_artifact_fingerprint(**with_window) != compute_artifact_fingerprint(**kwargs)


def test_verified_artifact_loader_accepts_mapping_rows():
    cursor = _Cursor(_artifact_row())
    manager = SimpleNamespace(get_metadata_db_connection=lambda: _Connection(cursor))
    loaded = load_verified_artifact(manager, artifact_id="artifact")
    assert loaded.storage_layer == "BRONZE"
    assert loaded.row_count == 2
    assert "artifact_status = 'VERIFIED'" in cursor.sql[0][0]
    assert "b.run_id = m.run_id" in cursor.sql[0][0]
    assert "LOWER(b.load_type) = LOWER(m.storage_layer)" in cursor.sql[0][0]


def test_artifact_publish_rejects_conflicting_existing_identity():
    existing = (
        "manifest-id", "different/path", "parquet", 2, "policy", "p" * 64,
        "m" * 64, "k" * 64, "s" * 64, "a" * 64, None, None, None, None, 99, "VERIFIED"
    )
    cursor = _SequenceCursor([None, existing])
    manager = SimpleNamespace(get_metadata_db_connection=lambda: _Connection(cursor))
    with pytest.raises(RuntimeError, match="different immutable fields"):
        publish_verified_artifact(
            manager,
            run_id="run",
            table_id=1,
            attempt_number=1,
            source_tag="source",
            target_tag="target",
            source_table="source_table",
            target_table="target_table",
            storage_layer="BRONZE",
            storage_uri="expected/path",
            storage_format="parquet",
            row_count=2,
            policy_version_id="policy",
            policy_fingerprint="p" * 64,
            mapping_fingerprint="m" * 64,
            source_key_fingerprint="k" * 64,
            schema_fingerprint="s" * 64,
            batch_log_id=99,
        )


def test_submission_rejects_stale_cdc_artifact_state():
    artifact = FrozenArtifactReference(
        artifact_id="a",
        producer_run_id="r",
        producer_attempt_number=1,
        storage_layer="SILVER",
        storage_uri="history/run_r",
        storage_format="parquet",
        row_count=2,
        policy_version_id="p",
        policy_fingerprint="pf",
        mapping_fingerprint="mf",
        source_key_fingerprint="kf",
        schema_fingerprint="sf",
        artifact_fingerprint="af",
        cdc_window_id="window",
        cdc_lower_cursor={"values": [10]},
        cdc_state_version=3,
    )
    matching = _Cursor({"committed_cursor": {"values": [10]}, "state_version": 3})
    _assert_artifact_cdc_state(matching, table_id=1, policy_version_id="p", artifact=artifact)
    stale = _Cursor({"committed_cursor": {"values": [11]}, "state_version": 4})
    with pytest.raises(ValueError, match="stale"):
        _assert_artifact_cdc_state(stale, table_id=1, policy_version_id="p", artifact=artifact)


def test_submission_detects_already_committed_cdc_artifact():
    from src.exceptions import ArtifactAlreadyCommittedError

    artifact = FrozenArtifactReference(
        artifact_id="a",
        producer_run_id="r",
        producer_attempt_number=1,
        storage_layer="SILVER",
        storage_uri="history/run_r",
        storage_format="parquet",
        row_count=2,
        policy_version_id="p",
        policy_fingerprint="pf",
        mapping_fingerprint="mf",
        source_key_fingerprint="kf",
        schema_fingerprint="sf",
        artifact_fingerprint="af",
        cdc_window_id="window",
        cdc_lower_cursor={"values": [10]},
        cdc_upper_cursor={"values": [20]},
        cdc_state_version=3,
    )
    committed_cur = _SequenceCursor([
        {"committed_cursor": {"values": [20]}, "state_version": 4},
        (1,),
    ])
    with pytest.raises(ArtifactAlreadyCommittedError, match="already been published"):
        _assert_artifact_cdc_state(committed_cur, table_id=1, policy_version_id="p", artifact=artifact)


def test_047_extends_existing_manifest_and_does_not_create_new_table():
    migration = Path("migrations/047_layer_artifact_manifest.sql").read_text(encoding="utf-8").upper()
    assert "ALTER TABLE METADATA.ATTEMPT_PERSISTENCE_MANIFEST" in migration
    assert "CREATE TABLE" not in migration
    assert "TO_JSONB(NEW) - 'ARTIFACT_STATUS' - 'VERIFIED_AT'" in migration


def test_governed_retry_resumes_silver_after_successful_parent_bronze(monkeypatch):
    etl = object.__new__(ETL)
    meta = SimpleNamespace()
    monkeypatch.setattr(
        etl,
        "_parent_layer_statuses",
        lambda *_args: {
            LayerName.BRONZE: RunStatus.SUCCESS,
            LayerName.SILVER: RunStatus.FAILED,
            LayerName.GOLD: RunStatus.FAILED,
        },
    )

    assert etl._governed_retry_layer(meta, "parent-run") == LayerName.SILVER


def test_full_retry_from_silver_skips_bronze_and_uses_exact_parent_artifact(monkeypatch):
    bronze_artifact = _artifact("BRONZE")
    silver_artifact = _artifact("SILVER", producer_run_id="child-run")
    meta = SimpleNamespace(
        id=1,
        source_tag="source",
        source_table="table",
        target_tag="target",
        target_table="table",
        is_cdc="n",
        upstream_artifact=bronze_artifact,
    )
    frozen_table = SimpleNamespace(authority_mode="GOVERNED")
    etl = object.__new__(ETL)
    etl.conn_manager = object()
    etl.spark_app_id = "spark"
    calls = []

    monkeypatch.setattr(etl_module, "BatchLogger", lambda *_args: object())
    monkeypatch.setattr(etl, "_metadata_row_from_frozen_table", lambda _table: meta)
    monkeypatch.setattr(etl, "_governed_retry_layer", lambda *_args: LayerName.SILVER)
    monkeypatch.setattr(etl, "_register_cancellation_guards", lambda *_args: None)
    monkeypatch.setattr(etl, "_clear_cancellation_guards", lambda: None)
    monkeypatch.setattr(
        etl,
        "_process_bronze_table",
        lambda *_args, **_kwargs: pytest.fail("Bronze must not run during a Silver retry"),
    )

    def process_silver(table_meta, *_args, bronze_artifact=None, **_kwargs):
        calls.append("silver")
        assert bronze_artifact == meta.upstream_artifact
        table_meta._silver_artifact = silver_artifact
        return 3, 3, "", 0

    def process_gold(_table_meta, *_args, silver_artifact=None, **_kwargs):
        calls.append("gold")
        assert silver_artifact == silver_artifact_ref
        return 3, 3, "", 0

    silver_artifact_ref = silver_artifact
    monkeypatch.setattr(etl, "_process_silver_table", process_silver)
    monkeypatch.setattr(etl, "_process_gold_table", process_gold)

    def execute_batch(tables, layer, _source_tag, _logger, worker):
        worker(tables[0], log_id=1, logger=object())
        return tables

    monkeypatch.setattr(etl, "_execute_table_batch", execute_batch)

    assert etl.run_frozen_table(
        frozen_table=frozen_table,
        run_id="child-run",
        resume_from_run_id="parent-run",
        execution_scope="FULL",
    ) == [meta]
    assert calls == ["silver", "gold"]
