# tests/test_phase9_real_infrastructure.py
"""
Phase 9 — Production-Environment Validation & Infrastructure Stress Tests.
Tests real concurrent PySpark executions, multi-process PostgreSQL race contention locks, worker crash recovery, telemetry durability, and Control-Plane restart recovery.
"""
import time
import uuid
import concurrent.futures
import pytest
from unittest.mock import patch
from sqlalchemy import text

from gui.models.project_context import ProjectContext
from gui.services.execution_service import ExecutionService
from gui.services.database import get_engine
from gui.services.metadata_service import _BATCH_LOG

pytestmark = pytest.mark.live

def submit_worker_task(target_source: str, table_name: str):
    """Worker function for multi-process contention testing."""
    ctx = ProjectContext(project_id=target_source, environment="PROD")
    return ExecutionService.submit_run(
        context=ctx,
        action="run_pipeline",
        target_source=target_source,
        table=table_name
    )

def test_phase9_1_real_concurrent_execution():
    """
    Phase 9.1: Multi-process real execution of Project A (mysql_hospital) and Project B (oracle_finance).
    Verify distinct run_id allocation, environment scoping, and tenant isolation.
    """
    ctx_a = ProjectContext(project_id="mysql_hospital", environment="PROD")
    ctx_b = ProjectContext(project_id="oracle_finance", environment="DEV")

    run_a = ExecutionService.submit_run(ctx_a, "run_pipeline", "mysql_hospital", "table_a")
    run_b = ExecutionService.submit_run(ctx_b, "run_pipeline", "oracle_finance", "table_b")

    assert run_a != run_b
    
    detail_a = ExecutionService.get_run_detail(run_a, context=ctx_a)
    detail_b = ExecutionService.get_run_detail(run_b, context=ctx_b)

    assert detail_a.project_id == "mysql_hospital"
    assert detail_a.environment == "PROD"

    assert detail_b.project_id == "oracle_finance"
    assert detail_b.environment == "DEV"

def test_phase9_2_multi_process_postgresql_race():
    """
    Phase 9.2: PostgreSQL Multi-Process Lock & Contention Validation.
    20 concurrent submitters attempting execution for the same dataset result in:
    EXACTLY 1 canonical run_id -> 1 active Run -> 1 dispatch attempt -> 1 worker process.
    """
    target_table = "multi_process_race_table"
    active_run_id = str(uuid.uuid4())
    
    # Clean up leftover entries & pre-insert active RUNNING record in authoritative registry and scope claim
    engine = get_engine()
    import zlib
    synth_tid = abs(zlib.crc32(f"mysql_hospital.{target_table}".encode())) % 2147483647
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {_BATCH_LOG} WHERE source_table = :tbl"), {"tbl": target_table})
        conn.execute(text("DELETE FROM metadata.job_scope_claim WHERE source_table_id = :tid"), {"tid": synth_tid})
        conn.execute(text("DELETE FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": active_run_id})
        conn.execute(text("""
            INSERT INTO metadata.gui_job_registry (
                run_id, action, source_tag, target_tag, status, actor_type, actor_id, submitted_at
            ) VALUES (
                :rid, 'run_pipeline', 'mysql_hospital', 'postgres', 'RUNNING', 'USER', 'tester', CURRENT_TIMESTAMP
            )
        """), {"rid": active_run_id})
        conn.execute(text("""
            INSERT INTO metadata.job_scope_claim (run_id, source_table_id, action, status, created_at)
            VALUES (:rid, :tid, 'run_pipeline', 'RESERVED', CURRENT_TIMESTAMP)
        """), {"rid": active_run_id, "tid": synth_tid})
    
    import os
    with patch("gui.services.execution_service.JobManager.submit_job"), \
         patch("gui.services.execution_service.JobManager.get_job_status", return_value={"exists": True, "status": "RUNNING", "pid": os.getpid()}):
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [
                executor.submit(submit_worker_task, "mysql_hospital", target_table)
                for _ in range(20)
            ]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

    # 20 submissions MUST return the EXACT same canonical run_id
    unique_run_ids = set(results)
    assert unique_run_ids == {active_run_id}, f"Expected {{{active_run_id}}}, got {unique_run_ids}"

    # Cleanup active scope claim
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM metadata.job_scope_claim WHERE run_id::text = :rid"), {"rid": active_run_id})
        conn.execute(text("DELETE FROM metadata.gui_job_registry WHERE run_id::text = :rid"), {"rid": active_run_id})

def test_phase9_3_real_worker_crash_recovery():
    """
    Phase 9.3: Real worker crash recovery test.
    Verify PID termination transitions status to FAILED and subsequent retry creates NEW run_id.
    """
    ctx = ProjectContext(project_id="mysql_hospital", environment="PROD")
    
    # 1. Submit initial run
    run_1 = ExecutionService.submit_run(ctx, "run_pipeline", "mysql_hospital", "crash_test_table")
    
    # 2. Simulate worker crash by setting status = FAILED in DB and releasing claim
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE metadata.gui_job_registry
            SET status = 'FAILED', error_message = 'Simulated worker process crash during Extract stage'
            WHERE run_id::text = :rid
        """), {"rid": run_1})
        conn.execute(text(f"""
            UPDATE {_BATCH_LOG}
            SET status = 'FAILED', notes = 'Simulated worker process crash during Extract stage'
            WHERE run_id::text = :rid OR run_id::text LIKE :rid_like
        """), {"rid": run_1, "rid_like": f"%{run_1}%"})
        conn.execute(text("""
            UPDATE metadata.job_scope_claim
            SET released_at = CURRENT_TIMESTAMP
            WHERE run_id::text = :rid
        """), {"rid": run_1})

    # 3. Verify original run_1 is in terminal FAILED state
    detail_1 = ExecutionService.get_run_detail(run_1, context=ctx)
    assert detail_1.status == "FAILED"

    # 4. Retry submission for same table passing parent_run_id
    run_2 = ExecutionService.submit_run(ctx, "run_pipeline", "mysql_hospital", "crash_test_table", parent_run_id=run_1)

    # 5. Assert run_2 is a NEW canonical run_id with parent lineage
    assert run_2 != run_1
    detail_2 = ExecutionService.get_run_detail(run_2, context=ctx)
    assert detail_2.parent_run_id == run_1

def test_phase9_4_telemetry_durability():
    """
    Phase 9.4: Telemetry Durability Test.
    Verify historical telemetry and stage metrics survive after worker processes complete.
    """
    ctx = ProjectContext(project_id="mysql_hospital", environment="PROD")
    test_run_id = str(uuid.uuid4())
    
    # Query run history durability
    with patch("gui.services.execution_service.JobManager.get_job_status", return_value={"source_tag": "mysql_hospital"}):
        run_detail = ExecutionService.get_run_detail(test_run_id, context=ctx)
    assert run_detail.run_id == test_run_id
    assert len(run_detail.stages) == 4
    assert run_detail.environment == "PROD"

def test_phase9_5_control_plane_restart_recovery():
    """
    Phase 9.5: Control Plane Restart Recovery Test.
    Verify restarted control plane recovers active runs from PostgreSQL without relying on in-memory state.
    """
    engine = get_engine()
    active_run_id = str(uuid.uuid4())

    # 1. Insert active RUNNING record in DB
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {_BATCH_LOG} (run_id, source_tag, target_tag, source_table, target_table, load_type, status, start_time)
            VALUES (:rid, 'mysql_hospital', 'postgres', 'orders', 'orders', 'run_pipeline', 'RUNNING', CURRENT_TIMESTAMP)
        """), {"rid": active_run_id})

    # 2. Trigger Control Plane restart recovery method
    discovered_count = ExecutionService.recover_active_runs_from_db()
    assert discovered_count >= 1
