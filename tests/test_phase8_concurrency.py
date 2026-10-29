# tests/test_phase8_concurrency.py
"""
Phase 8.1 — Multi-Thread Concurrency & Idempotency Lock Integration Tests.
Verifies DB-enforced idempotency locks prevent rapid duplicate process spawning per ADR-006.
"""
import concurrent.futures
import pytest
from gui.models.project_context import ProjectContext
from gui.services.execution_service import ExecutionService

pytestmark = pytest.mark.live

def test_concurrent_different_project_runs():
    """Verify concurrent submissions for different projects execute independently."""
    ctx_a = ProjectContext(project_id="mysql_hospital", environment="PROD")
    ctx_b = ProjectContext(project_id="oracle_finance", environment="DEV")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f_a = executor.submit(ExecutionService.submit_run, ctx_a, "run_pipeline", "mysql_hospital", "dataset_a")
        f_b = executor.submit(ExecutionService.submit_run, ctx_b, "run_pipeline", "oracle_finance", "dataset_b")

        run_a = f_a.result()
        run_b = f_b.result()

    assert run_a is not None
    assert run_b is not None
    assert run_a != run_b

def test_idempotency_lock_prevents_duplicate_process_spawning():
    """Verify rapid duplicate submissions for identical dataset return active run_id without duplicate process spawning."""
    ctx = ProjectContext(project_id="mysql_hospital", environment="PROD")

    # Submit first run
    run_1 = ExecutionService.submit_run(ctx, "run_pipeline", "mysql_hospital", "idempotency_test_table")
    
    # Rapid second submission for same table
    run_2 = ExecutionService.submit_run(ctx, "run_pipeline", "mysql_hospital", "idempotency_test_table")

    # Idempotency lock should return the active run_id
    assert run_1 == run_2
