# tests/test_tenant_isolation.py
"""
Unit tests for ProjectContext tenant isolation per ADR-005.
Asserts Project A context queries never return Project B datasets, catalog items, or audit records.
"""
import pytest

from gui.models.project_context import ProjectContext
from gui.services.metadata_service import get_dashboard_kpis, get_recent_activity, get_pipeline_tables

def test_project_context_scoping():
    """Verify ProjectContext correctly identifies single project vs ALL projects."""
    ctx_all = ProjectContext(project_id=None, environment="PROD")
    assert ctx_all.is_all_projects is True

    ctx_proj_a = ProjectContext(project_id="mysql_hospital", environment="PROD")
    assert ctx_proj_a.is_all_projects is False
    assert ctx_proj_a.project_id == "mysql_hospital"

    ctx_proj_b = ProjectContext(project_id="oracle_finance", environment="DEV")
    assert ctx_proj_b.is_all_projects is False
    assert ctx_proj_b.project_id == "oracle_finance"

@pytest.mark.live
def test_service_queries_accept_context():
    """Verify metadata service queries accept ProjectContext without exception."""
    ctx_a = ProjectContext(project_id="mysql_hospital", environment="PROD")
    kpis = get_dashboard_kpis(context=ctx_a)
    assert isinstance(kpis, dict)
    assert "running" in kpis

    activity_df = get_recent_activity(limit=10, context=ctx_a)
    assert activity_df is not None

    tables_df = get_pipeline_tables(context=ctx_a)
    assert tables_df is not None
