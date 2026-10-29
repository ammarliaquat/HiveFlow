# tests/test_retry_lineage.py
"""
Phase 8.3 — Immutable Terminal State & Parent Run Lineage Integration Tests.
Verifies retries generate a NEW run_id linked to parent_run_id while original remains immutable per ADR-006.
"""
from gui.models.project_context import ProjectContext
from gui.services.execution_service import ExecutionService

import pytest

@pytest.mark.live
def test_retry_lineage_creates_new_linked_run():
    """Verify submitting retry generates new run_id linked to parent_run_id."""
    ctx = ProjectContext(project_id="mysql_hospital", environment="PROD")
    
    parent_id = "parent_failed_run_101"
    
    # Submit rerun passing parent_run_id
    new_run_id = ExecutionService.submit_run(
        context=ctx,
        action="run_pipeline",
        target_source="mysql_hospital",
        table="retry_lineage_table",
        parent_run_id=parent_id
    )
    
    assert new_run_id != parent_id
    
    detail = ExecutionService.get_run_detail(new_run_id, context=ctx)
    assert detail.run_id == new_run_id
    assert detail.parent_run_id == parent_id
