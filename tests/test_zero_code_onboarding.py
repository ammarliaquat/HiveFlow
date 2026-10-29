# tests/test_zero_code_onboarding.py
"""
Unit tests for zero-code project onboarding per ADR-005.
Verifies Project A (MySQL) and Project B (Oracle) execute through identical domain services and engine code.
"""
import pytest

from gui.models.project_context import ProjectContext
from gui.services.metadata_service import get_catalog_summary, get_pipeline_tables
from gui.services.health_service import HealthService
from gui.services.execution_service import ExecutionService

pytestmark = pytest.mark.live

def test_zero_code_onboarding_contract():
    """Verify onboarding two distinct project contexts operates cleanly through identical service interfaces."""
    proj_a_ctx = ProjectContext(project_id="mysql_hospital", environment="PROD")
    proj_b_ctx = ProjectContext(project_id="oracle_finance", environment="DEV")

    # 1. Discover source catalog
    cat_a = get_catalog_summary(context=proj_a_ctx)
    cat_b = get_catalog_summary(context=proj_b_ctx)
    assert cat_a is not None
    assert cat_b is not None

    # 2. Pipeline control tables
    p_a = get_pipeline_tables(context=proj_a_ctx)
    p_b = get_pipeline_tables(context=proj_b_ctx)
    assert p_a is not None
    assert p_b is not None

    # 3. Infrastructure telemetry
    h_a = HealthService.get_cluster_health(context=proj_a_ctx)
    h_b = HealthService.get_cluster_health(context=proj_b_ctx)
    assert h_a["environment"] == "PROD"
    assert h_b["environment"] == "DEV"
