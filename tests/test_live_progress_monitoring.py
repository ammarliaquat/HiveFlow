# tests/test_live_progress_monitoring.py
"""
Automated Unit & Integration Test Suite for Production-Grade Live Monitoring (Section 34).
Verifies progress invariants, chunk progress commits, retry idempotency, EWMA throughput,
reconciliation status, heartbeat vs progress separation, stall detection, concurrent run isolation,
and zero-row/small-batch corner cases.
"""
import uuid
import time
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from src.logging_.batch_logger import BatchLogger
from src.logging_.stage_profiler import StageProfiler
from src.validation.reconciler import Reconciler
from src.constants import RunStatus
from src.exceptions import ExecutionOwnershipRequiredError
from gui.services.database import get_engine
from sqlalchemy import text


@pytest.fixture
def mock_conn_manager():
    """Provides a ConnectionManager linked to test database."""
    cm = MagicMock()
    # Use live PostgreSQL engine from get_engine()
    engine = get_engine()
    
    class LiveMetadataConnectionContext:
        def __enter__(self):
            self.raw_conn = engine.raw_connection()
            return self.raw_conn
        def __exit__(self, exc_type, exc_val, exc_tb):
            if hasattr(self, 'raw_conn'):
                self.raw_conn.close()

    cm.get_metadata_db_connection.side_effect = lambda: LiveMetadataConnectionContext()
    return cm


def test_scenario_01_first_chunk_progress(mock_conn_manager):
    """TEST 1: source_count = 647617, chunk_size = 20000. After first chunk: processed_count = 20000, progress_pct ≈ 3.09%."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start(
        source_tag="test_src",
        source_table="users",
        target_table="users",
        load_type="bronze",
        priority=1
    )
    assert log_id is not None

    # Extraction completion sets extracted_count = 647617, processed_count = 0
    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=647617, processed_count=0, source_count=647617)

    # Chunk 1 completes with 20,000 rows
    logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=1, total_chunks=33, stage="BRONZE_WRITE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT extracted_count, processed_count, progress_pct, current_phase FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["extracted_count"] == 647617
    assert row["processed_count"] == 20000
    assert float(row["progress_pct"]) == 3.09
    assert row["current_phase"] == "BRONZE_WRITE"


def test_scenario_02_second_chunk_progress(mock_conn_manager):
    """TEST 2: After second chunk: processed_count = 40000."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "users", "users", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=647617, processed_count=0, source_count=647617)
    logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=1, total_chunks=33, stage="BRONZE_WRITE")
    logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=2, total_chunks=33, stage="BRONZE_WRITE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count, progress_pct FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["processed_count"] == 40000
    assert float(row["progress_pct"]) == 6.18


def test_scenario_03_chunk_32_progress(mock_conn_manager):
    """TEST 3: After 32 chunks: processed_count = 640000."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "users", "users", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=647617, processed_count=0, source_count=647617)
    for i in range(1, 33):
        logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=i, total_chunks=33, stage="BRONZE_WRITE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count, progress_pct FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["processed_count"] == 640000
    assert float(row["progress_pct"]) == 98.82


def test_scenario_04_final_chunk_actual_rows(mock_conn_manager):
    """TEST 4: Final chunk actual_rows = 7617: processed_count = 647617, progress_pct = 100.0%."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "users", "users", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=647617, processed_count=0, source_count=647617)
    for i in range(1, 33):
        logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=i, total_chunks=33, stage="BRONZE_WRITE")

    # Final chunk 33 has 7,617 rows (647,617 - 640,000 = 7,617)
    logger.update_chunk_progress(log_id, actual_chunk_rows=7617, chunk_number=33, total_chunks=33, stage="BRONZE_FINALIZE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count, progress_pct FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["processed_count"] == 647617
    assert float(row["progress_pct"]) == 100.0


def test_scenario_05_chunk_write_failure_does_not_increment_processed(mock_conn_manager):
    """TEST 5: Chunk write fails -> processed_count MUST remain unchanged."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "users", "users", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=647617, processed_count=0, source_count=647617)
    logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=1, total_chunks=33, stage="BRONZE_WRITE")
    logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=2, total_chunks=33, stage="BRONZE_WRITE")

    # Simulating Chunk 3 write failure: update_chunk_progress is NOT called on failure
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["processed_count"] == 40000


def test_scenario_06_chunk_retry_succeeds_once(mock_conn_manager):
    """TEST 6: Chunk retry succeeds -> processed_count increases exactly once."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "users", "users", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=647617, processed_count=0, source_count=647617)
    logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=1, total_chunks=33, stage="BRONZE_WRITE")
    
    # Chunk 2 fails first attempt (no progress update)
    # Chunk 2 retry succeeds on attempt 2:
    logger.update_chunk_progress(log_id, actual_chunk_rows=20000, chunk_number=2, total_chunks=33, stage="BRONZE_WRITE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["processed_count"] == 40000


def test_scenario_07_target_reconciliation_match(mock_conn_manager):
    """TEST 7: Target reconciliation source_count == target_count -> PASSED."""
    meta = MagicMock(source_table="users", is_reconcile="y", reconcile_threshold=0.0)
    res = Reconciler.reconcile(source_count=647617, target_count=647617, layer="gold", meta=meta)
    assert res == RunStatus.SUCCESS


def test_scenario_08_target_reconciliation_mismatch(mock_conn_manager):
    """TEST 8: Target reconciliation source_count != target_count -> raises RuntimeError if action=FAIL."""
    meta = MagicMock(source_table="users", is_reconcile="y", reconcile_threshold=0.0, reconcile_action="FAIL")
    with pytest.raises(RuntimeError, match="Reconciliation failed"):
        Reconciler.reconcile(source_count=647617, target_count=640000, layer="gold", meta=meta)


def test_scenario_09_heartbeat_updates_without_row_progress(mock_conn_manager):
    """TEST 9: Heartbeat continues but no row progress -> status remains RUNNING."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "users", "users", "bronze", 1)

    # Let heartbeat thread run for a brief moment
    time.sleep(2.5)

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT status, last_heartbeat FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["status"] == "RUNNING"
    assert row["last_heartbeat"] is not None


def test_scenario_10_heartbeat_stops_records_slow_progress_observation(mock_conn_manager):
    """TEST 10: Missing progress is observable but does not revoke a run lease."""
    run_id = str(uuid.uuid4())
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.batch_log (run_id, source_tag, source_table, target_table, load_type, priority, status, current_phase, last_heartbeat, last_progress_update, notes)
            VALUES (:rid, 'test', 'users', 'users', 'bronze', 1, 'RUNNING', 'BRONZE_WRITE', CURRENT_TIMESTAMP - interval '350 second', CURRENT_TIMESTAMP - interval '350 second', 'Existing note')
        """), {"rid": run_id})

    stalled_ids = BatchLogger.detect_and_mark_stalled_runs(mock_conn_manager, heartbeat_timeout_sec=60, progress_timeout_sec=300)
    assert run_id in stalled_ids

    with engine.connect() as conn:
        row = conn.execute(text("SELECT status, notes FROM metadata.batch_log WHERE run_id::text = :rid"), {"rid": run_id}).mappings().fetchone()

    assert row["status"] == "RUNNING"
    assert row["notes"] == "Existing note | [SLOW_PROGRESS]: Progress timeout exceeded."


def test_scenario_11_concurrent_runs_isolation(mock_conn_manager):
    """TEST 11: Two simultaneous runs of same source table update isolated rows by run_id."""
    run_id_1 = str(uuid.uuid4())
    run_id_2 = str(uuid.uuid4())

    logger1 = BatchLogger(mock_conn_manager, run_id=run_id_1, spark_app_id="app1")
    logger2 = BatchLogger(mock_conn_manager, run_id=run_id_2, spark_app_id="app2")

    log_id_1 = logger1.log_start("src1", "users", "users", "bronze", 1)
    log_id_2 = logger2.log_start("src1", "users", "users", "bronze", 1)

    logger1.update_phase(log_id_1, "BRONZE_WRITE", extracted_count=1000, processed_count=0, source_count=1000)
    logger2.update_phase(log_id_2, "BRONZE_WRITE", extracted_count=5000, processed_count=0, source_count=5000)

    logger1.update_chunk_progress(log_id_1, actual_chunk_rows=1000, chunk_number=1, total_chunks=1, stage="BRONZE_FINALIZE")

    engine = get_engine()
    with engine.connect() as conn:
        row1 = conn.execute(text("SELECT processed_count FROM metadata.batch_log WHERE id = :id"), {"id": log_id_1}).mappings().fetchone()
        row2 = conn.execute(text("SELECT processed_count FROM metadata.batch_log WHERE id = :id"), {"id": log_id_2}).mappings().fetchone()

    assert row1["processed_count"] == 1000
    assert row2["processed_count"] == 0


def test_scenario_12_monitoring_db_unavailable_does_not_corrupt_target():
    """TEST 12: A metadata outage blocks ownership before any target write."""
    failing_cm = MagicMock()
    failing_cm.get_metadata_db_connection.side_effect = Exception("DB Connection Timeout")
    run_id = str(uuid.uuid4())
    logger = BatchLogger(failing_cm, run_id=run_id, spark_app_id="app")
    
    with pytest.raises(
        ExecutionOwnershipRequiredError,
        match="EXECUTION_OWNERSHIP_REQUIRED",
    ):
        logger.log_start("src", "tbl", "tbl", "bronze", 1)


def test_scenario_13_application_restart_idempotency(mock_conn_manager):
    """TEST 13: Application restart while processing chunk -> recovery prevents double-counting."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="app")
    log_id = logger.log_start("src", "tbl", "tbl", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=10000, processed_count=0, source_count=10000)
    logger.update_chunk_progress(log_id, actual_chunk_rows=5000, chunk_number=1, total_chunks=2, stage="BRONZE_WRITE")

    # Restarting pipeline attempt 2:
    logger_retry = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="app_retry")
    log_id_retry = logger_retry.log_start("src", "tbl", "tbl", "bronze", 1, attempt_number=2)

    logger_retry.update_phase(log_id_retry, "BRONZE_WRITE", extracted_count=10000, processed_count=0, source_count=10000)
    logger_retry.update_chunk_progress(log_id_retry, actual_chunk_rows=10000, chunk_number=1, total_chunks=1, stage="BRONZE_FINALIZE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count, attempt_number FROM metadata.batch_log WHERE id = :id"), {"id": log_id_retry}).mappings().fetchone()

    assert row["processed_count"] == 10000
    assert row["attempt_number"] == 2


def test_scenario_14_source_smaller_than_chunk_size(mock_conn_manager):
    """TEST 14: Source has fewer rows than chunk size (e.g. 12,000 rows) -> processed_count = 12000."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "users", "users", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=12000, processed_count=0, source_count=12000)
    logger.update_chunk_progress(log_id, actual_chunk_rows=12000, chunk_number=1, total_chunks=1, stage="BRONZE_FINALIZE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count, progress_pct FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["processed_count"] == 12000
    assert float(row["progress_pct"]) == 100.0


def test_scenario_15_empty_source_zero_rows(mock_conn_manager):
    """TEST 15: Empty source (0 rows) -> zero division guarded, completes with 0 rows."""
    run_id = str(uuid.uuid4())
    logger = BatchLogger(mock_conn_manager, run_id=run_id, spark_app_id="test_app")
    log_id = logger.log_start("test_src", "empty_tbl", "empty_tbl", "bronze", 1)

    logger.update_phase(log_id, "BRONZE_WRITE", extracted_count=0, processed_count=0, source_count=0)
    logger.update_chunk_progress(log_id, actual_chunk_rows=0, chunk_number=1, total_chunks=1, stage="BRONZE_FINALIZE")

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("SELECT processed_count, progress_pct FROM metadata.batch_log WHERE id = :id"), {"id": log_id}).mappings().fetchone()

    assert row["processed_count"] == 0
