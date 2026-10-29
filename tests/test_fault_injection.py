"""Deterministic, mock-only Phase 4A reaper safety checks.

Live DB/process faults belong in a separately marked, disposable-infrastructure suite.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from gui.services.execution_service import ExecutionService
from src.logging_.batch_logger import BatchHeartbeatThread, BatchLogger
from src.orchestrator import lifecycle
from src.orchestrator.lifecycle import transition_job


def _engine_for_rows(rows, update_result):
    engine = MagicMock()
    read_conn = MagicMock()
    read_conn.execute.return_value.mappings.return_value.fetchall.return_value = rows
    engine.connect.return_value.__enter__.return_value = read_conn
    write_conn = MagicMock()
    write_conn.execute.return_value = update_result
    engine.begin.return_value.__enter__.return_value = write_conn
    return engine, write_conn


def test_healthy_heartbeat_is_never_reaped_by_a_second_control_plane():
    row = {
        "run_id": "process-a",
        "source_tag": "source",
        "status": "RUNNING",
        "start_time": datetime.now(),
        "last_heartbeat": datetime.now(),
    }
    # The database-side heartbeat predicate rejects the conditional claim.
    engine, write_conn = _engine_for_rows([row], MagicMock(rowcount=0))

    with patch("gui.services.execution_service.get_engine", return_value=engine), \
         patch("gui.services.execution_service.JobManager.get_job_status", return_value={"exists": False}), \
         patch("psutil.pid_exists", return_value=False):
        assert ExecutionService.check_and_recover_orphaned_runs() == 0

    update_sql = str(write_conn.execute.call_args.args[0])
    assert "CURRENT_TIMESTAMP" in update_sql
    assert "RETURNING" in update_sql


def test_stale_run_is_marked_failed_exactly_when_conditional_claim_succeeds():
    row = {
        "run_id": "dead-process-a",
        "source_tag": "source",
        "status": "RUNNING",
        "start_time": datetime(2020, 1, 1),
        "last_heartbeat": datetime(2020, 1, 1),
    }
    engine, write_conn = _engine_for_rows([row], MagicMock(rowcount=1))

    with patch("gui.services.execution_service.get_engine", return_value=engine), \
         patch("gui.services.execution_service.JobManager.get_job_status", return_value={"exists": False}), \
         patch("psutil.pid_exists", return_value=False):
        assert ExecutionService.check_and_recover_orphaned_runs() == 1

    update_sql = str(write_conn.execute.call_args.args[0])
    assert "last_heartbeat" in update_sql
    assert "RETURNING" in update_sql


def test_racing_reaper_does_not_claim_a_run_when_heartbeat_was_refreshed():
    row = {
        "run_id": "process-a",
        "source_tag": "source",
        "status": "RUNNING",
        "start_time": datetime(2020, 1, 1),
        "last_heartbeat": datetime(2020, 1, 1),
    }
    engine, _ = _engine_for_rows([row], MagicMock(rowcount=0))

    with patch("gui.services.execution_service.get_engine", return_value=engine), \
         patch("gui.services.execution_service.JobManager.get_job_status", return_value={"exists": False}), \
         patch("psutil.pid_exists", return_value=False):
        assert ExecutionService.check_and_recover_orphaned_runs() == 0


def test_reaper_rejects_a_non_positive_heartbeat_timeout():
    with pytest.raises(ValueError, match="positive"):
        ExecutionService.check_and_recover_orphaned_runs(heartbeat_timeout_sec=0)


def test_heartbeat_updates_parent_and_batch_leases_together():
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    heartbeat = BatchHeartbeatThread(
        manager,
        run_id="run-17",
        log_id=17,
        start_time=datetime.now(),
        attempt_number=3,
    )

    def stop_after_batch_update(*_args):
        if cursor.execute.call_count == 2:
            heartbeat.stop()

    cursor.execute.side_effect = stop_after_batch_update

    with patch("src.logging_.batch_logger.time.sleep", return_value=None):
        heartbeat.run()

    batch_statement, batch_params = cursor.execute.call_args_list[0].args
    parent_statement, parent_params = cursor.execute.call_args_list[1].args
    assert "metadata.gui_job_registry" in parent_statement
    assert "status = 'RUNNING'" in parent_statement
    assert parent_params == ("run-17",)
    assert "status = 'RUNNING'" in batch_statement
    assert "attempt_number = %s" in batch_statement
    assert batch_params[-2:] == (17, 3)


def test_reaper_terminal_cas_rechecks_the_heartbeat_lease():
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = None

    claimed = transition_job(
        conn,
        run_id="run-17",
        from_status="RUNNING",
        to_status="FAILED",
        heartbeat_expired_timeout_sec=120,
    )

    assert claimed is False
    statement, params = conn.execute.call_args.args
    assert "COALESCE(last_heartbeat, started_at, submitted_at)" in str(statement)
    assert "CURRENT_TIMESTAMP" in str(statement)
    assert params["heartbeat_timeout_sec"] == 120


def test_reaper_does_not_fence_batches_when_the_heartbeat_cas_loses_the_race(monkeypatch):
    executed_sql = []
    transition_calls = []

    class Result:
        def __init__(self, rows=None):
            self._rows = rows or []

        def mappings(self):
            return self

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            sql_text = str(sql)
            executed_sql.append(sql_text)
            if "status = 'DISPATCHED'" in sql_text:
                return Result([])
            if "status = 'RUNNING'" in sql_text and "FROM metadata.gui_job_registry" in sql_text:
                return Result([{"run_id": "run-17", "worker_attempt_number": 1}])
            return Result([])

    class FakeEngine:
        def connect(self):
            return FakeConnection()

        def begin(self):
            return FakeConnection()

    def lose_race(*_args, **kwargs):
        transition_calls.append(kwargs)
        return False

    monkeypatch.setattr(lifecycle, "transition_job", lose_race)

    assert lifecycle.reap_stale_job_leases(FakeEngine(), worker_heartbeat_timeout_sec=120) == (0, 0)
    assert transition_calls[0]["heartbeat_expired_timeout_sec"] == 120
    assert not any("UPDATE metadata.batch_log" in sql for sql in executed_sql)


def test_batch_logger_initialization_does_not_execute_schema_ddl():
    manager = MagicMock()

    BatchLogger(manager, run_id="run-1")

    manager.get_metadata_db_connection.assert_not_called()


def test_phase_and_progress_updates_are_fenced_by_the_active_lease():
    cursor = MagicMock()
    now = datetime.now()
    cursor.fetchone.return_value = (now, 10, 0, None, 3)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    logger = object.__new__(BatchLogger)
    logger._conn_manager = manager

    logger.update_phase(log_id=17, phase="GOLD_WRITE", processed_count=1)
    logger.update_chunk_progress(log_id=17, actual_chunk_rows=1)

    update_statements = [
        str(call.args[0])
        for call in cursor.execute.call_args_list
        if str(call.args[0]).lstrip().startswith("UPDATE")
    ]
    assert len(update_statements) == 2
    for statement in update_statements:
        assert "status = 'RUNNING'" in statement
        assert "attempt_number = %s" in statement
        assert "last_progress_update = CURRENT_TIMESTAMP" in statement


def test_progress_rate_uses_database_time_not_worker_local_time():
    """A UTC metadata clock must not be mixed with the worker's local clock."""
    db_start = datetime(2026, 9, 16, 10, 0, 0)
    db_now = datetime(2026, 9, 16, 10, 0, 10)
    cursor = MagicMock()
    cursor.fetchone.return_value = (db_start, 1_000, 0, 50.0, 1)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    logger = BatchLogger(manager, run_id="run-db-clock")

    with patch("src.logging_.batch_logger.datetime") as mock_dt:
        mock_dt.now.return_value = db_now
        logger.update_phase(log_id=17, phase="BRONZE_WRITE", processed_count=200)
        logger.update_chunk_progress(log_id=17, actual_chunk_rows=200)

    select_call = cursor.execute.call_args_list[0]
    assert "SELECT start_time" in str(select_call.args[0])
    update_calls = [
        call for call in cursor.execute.call_args_list
        if str(call.args[0]).lstrip().startswith("UPDATE")
    ]
    # 200 confirmed rows / 10 database seconds; not / five local-time-offset hours.
    assert update_calls[0].args[1][5] == 20.0
    assert update_calls[1].args[1][3] == 20.0
    # EWMA must persist the calculation, not the instantaneous value.
    assert update_calls[0].args[1][6] == 41.0
    assert update_calls[1].args[1][4] == 41.0


def test_progress_rate_normalizes_legacy_naive_start_against_aware_db_time():
    """Legacy UTC timestamps without tzinfo must remain compatible with aware PostgreSQL time."""
    from datetime import timezone

    db_start = datetime(2026, 9, 16, 10, 0, 0)
    db_now = datetime(2026, 9, 16, 10, 0, 10, tzinfo=timezone.utc)
    cursor = MagicMock()
    cursor.fetchone.return_value = (db_start, 1_000, 0, None, 1)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    logger = BatchLogger(manager, run_id="run-aware-db-clock")

    with patch("src.logging_.batch_logger.datetime") as mock_dt:
        mock_dt.now.return_value = db_now.replace(tzinfo=None)
        logger.update_phase(log_id=17, phase="BRONZE_WRITE", processed_count=200)
        logger.update_chunk_progress(log_id=17, actual_chunk_rows=200)

    update_calls = [
        call for call in cursor.execute.call_args_list
        if str(call.args[0]).lstrip().startswith("UPDATE")
    ]
    assert len(update_calls) == 2
    assert update_calls[0].args[1][5] == 20.0
    assert update_calls[1].args[1][3] == 20.0


def test_terminal_logging_preserves_confirmed_layer_progress():
    """A terminal write must not replace current-run progress with target history."""
    cursor = MagicMock()
    cursor.fetchone.return_value = ("RUNNING",)
    cursor.rowcount = 1
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    logger = object.__new__(BatchLogger)
    logger._conn_manager = manager
    logger.run_id = "run-progress"
    logger._active_heartbeats = {}
    logger._attempt_numbers = {19: 1}
    logger._progress_counts = {19: 42}

    now = datetime.now()
    with patch("src.logging_.batch_logger.NotificationDeriver.on_batch_log_written"):
        logger.log(
            source_tag="source",
            source_table="events",
            target_table="events",
            load_type="gold",
            priority=1,
            source_count=100,
            target_count=1_000_000,
            start_time=now,
            end_time=now,
            status="SUCCESS",
            log_id=19,
        )

    terminal_updates = [
        str(call.args[0])
        for call in cursor.execute.call_args_list
        if str(call.args[0]).lstrip().startswith("UPDATE")
    ]
    assert terminal_updates
    statement = terminal_updates[0]
    assert "processed_count = COALESCE(processed_count, %s)" in statement
    assert "progress_pct = COALESCE(progress_pct, %s)" in statement


def test_terminal_logging_preserves_zero_row_cdc_progress():
    """An empty CDC window stays at zero even when target history is non-empty."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (datetime.now(), 100, 0, None, 1, datetime.now()),
        ("RUNNING",),
    ]
    cursor.rowcount = 1
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    logger = object.__new__(BatchLogger)
    logger._conn_manager = manager
    logger.run_id = "run-empty-cdc"
    logger._active_heartbeats = {}
    logger._attempt_numbers = {20: 1}
    logger._progress_counts = {}

    logger.update_phase(log_id=20, phase="GOLD_WRITE", processed_count=0, source_count=100)
    now = datetime.now()
    with patch("src.logging_.batch_logger.NotificationDeriver.on_batch_log_written"):
        logger.log(
            source_tag="source",
            source_table="events",
            target_table="events",
            load_type="gold",
            priority=1,
            source_count=100,
            target_count=1_000_000,
            start_time=now,
            end_time=now,
            status="SUCCESS",
            log_id=20,
        )

    update_calls = [
        call for call in cursor.execute.call_args_list
        if str(call.args[0]).lstrip().startswith("UPDATE metadata.batch_log")
    ]
    statement = str(update_calls[-1].args[0])
    assert "processed_count = COALESCE(processed_count, %s)" in statement


def test_slow_progress_detection_uses_database_time_without_changing_run_state():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("run-1",)]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn

    stalled = BatchLogger.detect_and_mark_stalled_runs(
        manager,
        heartbeat_timeout_sec=60,
        progress_timeout_sec=300,
    )

    statement = str(cursor.execute.call_args.args[0])
    assert stalled == ["run-1"]
    assert "CURRENT_TIMESTAMP" in statement
    assert "SET status" not in statement
    assert "last_progress_update" in statement


def test_late_finalizer_and_telemetry_are_fenced_after_a_reaper_claim():
    cursor = MagicMock()
    cursor.rowcount = 1
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    logger = object.__new__(BatchLogger)
    logger._conn_manager = manager
    logger.run_id = "run-1"
    logger._active_heartbeats = {}
    logger._attempt_numbers = {17: 3}
    now = datetime.now()

    logger.log(
        source_tag="source",
        source_table="orders",
        target_table="orders",
        load_type="gold",
        priority=1,
        source_count=1,
        target_count=1,
        start_time=now,
        end_time=now,
        status="SUCCESS",
        log_id=17,
    )
    logger.update_stage_metrics(17, {"perf_01": {"total_sec": 1.0}})

    update_statements = [
        str(call.args[0])
        for call in cursor.execute.call_args_list
        if str(call.args[0]).lstrip().startswith("UPDATE")
    ]
    assert len(update_statements) == 2
    for statement in update_statements:
        assert "status = 'RUNNING'" in statement
        assert "attempt_number = %s" in statement


def test_reaper_counts_one_run_when_a_shared_run_id_has_multiple_table_rows():
    row = {
        "run_id": "shared-run",
        "source_tag": "source",
        "status": "RUNNING",
        "start_time": datetime(2020, 1, 1),
        "last_heartbeat": datetime(2020, 1, 1),
    }
    engine, _ = _engine_for_rows([row], MagicMock(rowcount=2))

    with patch("gui.services.execution_service.get_engine", return_value=engine), \
         patch("gui.services.execution_service.JobManager.get_job_status", side_effect=AssertionError("PID state is not a lease")):
        assert ExecutionService.check_and_recover_orphaned_runs() == 1
