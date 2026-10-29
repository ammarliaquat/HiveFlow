# tests/test_rbac_context.py
"""
Phase 8.4 — Service-Wide RBAC Context Authorization Integration Tests.
Verifies domain services reject cross-tenant operations even if explicit target source or run_id is supplied.
"""
import pytest
from gui.models.project_context import ProjectContext
from gui.services.execution_service import ExecutionService, AuthorizationError

def test_rbac_rejects_cross_tenant_run_submission():
    """Verify ExecutionService rejects submitting run for source tag outside active ProjectContext."""
    ctx_hospital = ProjectContext(project_id="mysql_hospital", environment="PROD")

    with pytest.raises(AuthorizationError, match="RBAC Authorization Violation"):
        ExecutionService.submit_run(
            context=ctx_hospital,
            action="run_pipeline",
            target_source="oracle_finance",  # Cross-tenant violation
            table="orders"
        )

def test_rbac_rejects_cross_tenant_run_inspection(monkeypatch):
    """Verify ExecutionService rejects viewing detail for run belonging to another tenant context."""
    ctx_hospital = ProjectContext(project_id="mysql_hospital", environment="PROD")

    class FakeResult:
        def __init__(self, row):
            self.row = row

        def mappings(self):
            return self

        def fetchone(self):
            return self.row

    class FakeConnection:
        def __init__(self):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return FakeResult({
                    "run_id": "run_belonging_to_oracle_finance",
                    "action": "run_pipeline",
                    "source_tag": "oracle_finance",
                    "target_tag": "land_test",
                    "priority": 1,
                    "status": "RUNNING",
                    "pid": None,
                    "submitted_at": None,
                    "dispatched_at": None,
                    "started_at": None,
                    "last_heartbeat": None,
                    "completed_at": None,
                    "error_code": None,
                    "error_message": None,
                    "parameters": None,
                })
            return FakeResult(None)

    fake_conn = FakeConnection()

    class FakeEngine:
        def connect(self):
            return fake_conn

    monkeypatch.setattr("gui.services.execution_service.get_engine", lambda: FakeEngine())

    # Attempt to view detail of run belonging to oracle_finance
    with pytest.raises(AuthorizationError, match="RBAC Violation"):
        ExecutionService.get_run_detail("run_belonging_to_oracle_finance", context=ctx_hospital)
