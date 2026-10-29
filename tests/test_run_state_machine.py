# tests/test_run_state_machine.py
"""
Unit tests for ExecutionService Run state machine, lifecycle transitions, and pre-execution persistence.
Enforces ADR-006 state machine invariants without requiring live network infrastructure.
"""
import pytest
from unittest.mock import patch, MagicMock
from gui.services.execution_service import ExecutionService, VALID_TRANSITIONS
from gui.models.project_context import ProjectContext

def test_valid_state_transitions():
    """Verify valid state transitions allowed by state machine."""
    assert ExecutionService.validate_status_transition("QUEUED", "RUNNING") is True
    assert ExecutionService.validate_status_transition("QUEUED", "FAILED") is True
    assert ExecutionService.validate_status_transition("QUEUED", "CANCELLED") is True
    assert ExecutionService.validate_status_transition("RUNNING", "SUCCESS") is True
    assert ExecutionService.validate_status_transition("RUNNING", "FAILED") is True
    assert ExecutionService.validate_status_transition("RUNNING", "RECONCILIATION_FAILED") is True

def test_idempotent_same_state_reassertion():
    """Verify same-state transition is valid and idempotent."""
    assert ExecutionService.validate_status_transition("RUNNING", "RUNNING") is True
    assert ExecutionService.validate_status_transition("SUCCESS", "SUCCESS") is True

def test_invalid_terminal_state_transitions():
    """Verify terminal states (SUCCESS, FAILED, RECONCILIATION_FAILED) are immutable."""
    with pytest.raises(ValueError, match="Invalid state transition"):
        ExecutionService.validate_status_transition("SUCCESS", "RUNNING")

    with pytest.raises(ValueError, match="Invalid state transition"):
        ExecutionService.validate_status_transition("FAILED", "QUEUED")

    with pytest.raises(ValueError, match="Invalid state transition"):
        ExecutionService.validate_status_transition("RECONCILIATION_FAILED", "RUNNING")

@patch("gui.services.execution_service.get_psycopg2_conn")
def test_pre_execution_db_failure_prevents_dispatch(mock_get_conn):
    """Verify DB pre-registration failure prevents Spark/JobManager dispatch per ADR-006."""
    mock_conn = MagicMock()
    mock_conn.__enter__.side_effect = Exception("Database connection failure")
    mock_get_conn.return_value = mock_conn

    ctx = ProjectContext(project_id="test_proj", environment="DEV")

    with patch("src.orchestrator.job_manager.JobManager.submit_job") as mock_dispatch:
        with pytest.raises(RuntimeError, match="Job registration persistence failed"):
            ExecutionService.submit_run(context=ctx, action="run_pipeline", target_source="test_proj", table="users")
        
        # Verify JobManager was NEVER called
        mock_dispatch.assert_not_called()
