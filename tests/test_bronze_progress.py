from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.load.loader import Loader
from src.orchestrator import etl as etl_module
from src.orchestrator.etl import ETL


class _Writer:
    def __init__(self, writes, fail=False):
        self._writes = writes
        self._fail = fail
        self._mode = None

    def mode(self, mode):
        self._mode = mode
        return self

    def parquet(self, path):
        if self._fail:
            raise RuntimeError("simulated parquet write failure")
        self._writes.append((self._mode, path))


class _Frame:
    """Deliberately lacks RDD and partition APIs: Bronze owns one Spark write."""

    def __init__(self, writes, fail=False):
        self.write = _Writer(writes, fail=fail)


def _loader(tmp_path):
    loader = Loader.__new__(Loader)
    loader.paths = {"landing_base": str(tmp_path)}
    loader.conn_manager = SimpleNamespace(_raw_config={})
    return loader


def _meta():
    return SimpleNamespace(
        source_tag="source",
        landing_table="events",
        source_table="events",
        is_cdc="n",
    )


def test_bronze_write_is_one_fenced_spark_overwrite(tmp_path):
    writes = []
    ownership_checks = []

    path = _loader(tmp_path).write_landing(
        _Frame(writes),
        _meta(),
        "2026-09-14",
        before_persist=lambda: ownership_checks.append(True),
    )

    assert ownership_checks == [True]
    assert writes == [("overwrite", path)]


def test_bronze_write_does_not_credit_a_failed_write(tmp_path):
    ownership_checks = []

    with pytest.raises(RuntimeError, match="simulated parquet write failure"):
        _loader(tmp_path).write_landing(
            _Frame([], fail=True),
            _meta(),
            "2026-09-14",
            before_persist=lambda: ownership_checks.append(True),
        )

    assert ownership_checks == [True]


def test_governed_bronze_progress_uses_verified_parquet_rows(monkeypatch):
    events = []
    logger = MagicMock()
    logger.update_phase.side_effect = lambda _log_id, phase, **_kwargs: events.append(f"phase:{phase}")
    logger.update_chunk_progress.side_effect = lambda *_args, **_kwargs: events.append("progress")

    etl = ETL.__new__(ETL)
    etl.last_run_id = "run-1"
    etl.spark = object()
    etl.conn_manager = SimpleNamespace(
        get_semaphore=lambda *_args, **_kwargs: nullcontext(),
        get=lambda *_args, **_kwargs: SimpleNamespace(db_type="oracle"),
    )
    dataframe = SimpleNamespace(unpersist=lambda **_kwargs: None)
    etl.extractor = SimpleNamespace(read=lambda *_args, **_kwargs: dataframe)
    etl._freeze_schema_if_identity = lambda *_args, **_kwargs: None
    etl.loader = MagicMock()
    etl.loader.write_landing.side_effect = lambda *_args, **_kwargs: events.append("write") or "landing-path"

    profiler = MagicMock()
    monkeypatch.setenv("SPARK_CONNECT_URL", "sc://test")
    monkeypatch.setattr(etl_module, "StageProfiler", lambda **_kwargs: profiler)
    monkeypatch.setattr(
        etl_module.Reconciler,
        "count_and_cache",
        staticmethod(lambda _df: events.append("source_count") or 100),
    )
    monkeypatch.setattr(
        etl_module.Reconciler,
        "count_parquet",
        staticmethod(lambda _spark, _path: events.append("verified_count") or 100),
    )
    monkeypatch.setattr(
        etl_module.SchemaValidator,
        "validate_layer_contract",
        staticmethod(lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(
        etl_module,
        "DQEngine",
        lambda *_args, **_kwargs: SimpleNamespace(evaluate=lambda *_args, **_kwargs: "CONTINUE"),
    )

    meta = SimpleNamespace(
        id=1,
        is_cdc="n",
        source_tag="source",
        source_table="events",
    )
    result = etl._process_bronze_table(meta, log_id=7, logger=logger)

    assert result == (100, 100)
    assert events.index("write") < events.index("verified_count") < events.index("progress")
    assert "on_chunk_persisted" not in etl.loader.write_landing.call_args.kwargs
    assert "total_rows" not in etl.loader.write_landing.call_args.kwargs
    logger.update_chunk_progress.assert_called_once_with(
        7,
        actual_chunk_rows=100,
        chunk_number=1,
        total_chunks=1,
        stage="BRONZE_FINALIZE",
    )
