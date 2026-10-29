# tests/test_api_milestone2.py
"""
Milestone 2 Automated Test Suite: Governed Job Triggering, Cancellation, and Log Streaming.
Verifies RBAC permission gating, CSRF protection, scope-claim concurrency rejections,
state transitions, and execution dispatching.
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from datetime import datetime, timezone, timedelta

from api.main import app
from api.models.auth import UserResponse
from gui.models.submission import SubmissionResult
from gui.models.run import RunModel

client = TestClient(app)

ADMIN_USER = UserResponse(
    username="admin",
    role="ADMIN",
    permissions=["*"],
    allowed_projects=["source_erp_db", "opf_oracle_housing"],
)

OPERATOR_USER = UserResponse(
    username="operator_opforg",
    role="OPERATOR",
    permissions=["VIEW_TABLES", "VIEW_RUNS", "TRIGGER_RUNS"],
    allowed_projects=["source_erp_db"],
)

VIEWER_USER = UserResponse(
    username="viewer_user",
    role="VIEWER",
    permissions=["VIEW_TABLES", "VIEW_RUNS"],
    allowed_projects=["source_erp_db"],
)

def setup_function():
    app.dependency_overrides.clear()
    client.cookies.clear()

def teardown_function():
    app.dependency_overrides.clear()
    client.cookies.clear()


# -------------------------------------------------------------------------
# 1. Trigger Run Security & RBAC Tests
# -------------------------------------------------------------------------
def test_unauthenticated_trigger_rejected():
    """Verify triggering run without credentials returns 401."""
    res = client.post("/api/v1/runs/trigger", json={"source_tag": "source_erp_db"})
    assert res.status_code == 401
    assert res.json()["success"] is False


def test_csrf_missing_on_trigger_rejected():
    """Verify session cookie without CSRF header is rejected with 403."""
    client.cookies.set("etl_session", "valid.mock.token")
    with patch("api.security.auth.decode_session_token") as mock_dec, \
         patch("api.security.auth.is_user_active_in_db", return_value=True):
        mock_dec.return_value = MagicMock(username="admin", role="ADMIN", permissions=["*"])
        res = client.post("/api/v1/runs/trigger", json={"source_tag": "source_erp_db"})
        assert res.status_code == 403
        assert "CSRF validation failed" in res.json()["error"]["message"]


@patch("api.security.auth.is_user_active_in_db", return_value=True)
def test_viewer_without_trigger_permission_rejected(mock_active):
    """Verify user with VIEWER role lacking TRIGGER_RUNS is rejected with 403."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    headers = {"X-CSRF-Token": "test-csrf-token"}

    with patch("api.security.auth.decode_session_token") as mock_dec:
        mock_dec.return_value = MagicMock(username="viewer_user", role="VIEWER", permissions=["VIEW_TABLES", "VIEW_RUNS"])
        res = client.post("/api/v1/runs/trigger", json={"source_tag": "source_erp_db"}, headers=headers)
        assert res.status_code == 403
        assert "lacks execution permission" in res.json()["error"]["message"]


@patch("api.security.auth.is_user_active_in_db", return_value=True)
def test_cross_project_trigger_rejected_for_operator(mock_active):
    """Verify operator cannot trigger a run for an unassigned project."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    headers = {"X-CSRF-Token": "test-csrf-token"}

    with patch("api.security.auth.decode_session_token") as mock_dec, \
         patch("api.security.auth.get_all_source_tags", return_value=["source_erp_db"]):
        mock_dec.return_value = MagicMock(
            username="operator_opforg",
            role="OPERATOR",
            permissions=["VIEW_TABLES", "VIEW_RUNS", "TRIGGER_RUNS"],
        )
        # Attempt to trigger for opf_oracle_housing which is not in operator's allowed list
        res = client.post(
            "/api/v1/runs/trigger",
            json={"source_tag": "opf_oracle_housing"},
            headers=headers
        )
        assert res.status_code == 403
        assert "RBAC Violation" in res.json()["error"]["message"]


# -------------------------------------------------------------------------
# 2. Trigger Execution & Concurrency Governance Tests
# -------------------------------------------------------------------------
@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.submit_run")
def test_successful_trigger_returns_201(mock_submit, mock_active):
    """Verify valid trigger dispatches to ExecutionService and returns 201 with run_id."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    headers = {"X-CSRF-Token": "test-csrf-token"}

    mock_submit.return_value = SubmissionResult(
        accepted=True,
        request_id="req-1234",
        run_id="run-uuid-9999",
        status="QUEUED",
        submitted_at=datetime.now(timezone.utc),
    )

    with patch("api.security.auth.decode_session_token") as mock_dec, \
         patch("api.security.auth.get_all_source_tags", return_value=["source_erp_db"]):
        mock_dec.return_value = MagicMock(
            username="operator_opforg",
            role="OPERATOR",
            permissions=["VIEW_TABLES", "VIEW_RUNS", "TRIGGER_RUNS"],
        )
        res = client.post(
            "/api/v1/runs/trigger",
            json={
                "source_tag": "source_erp_db",
                "execution_scope": "FULL",
                "request_id": "req-1234",
            },
            headers=headers,
        )
        assert res.status_code == 201
        body = res.json()
        assert body["success"] is True
        assert body["data"]["accepted"] is True
        assert body["data"]["run_id"] == "run-uuid-9999"
        assert body["data"]["status"] == "QUEUED"
        mock_submit.assert_called_once()


@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.submit_run")
def test_successful_trigger_with_selected_dataset(mock_submit, mock_active):
    """Verify triggering an individual dataset passes source_table to ExecutionService."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    headers = {"X-CSRF-Token": "test-csrf-token"}

    mock_submit.return_value = SubmissionResult(
        accepted=True,
        request_id="req-dataset-1",
        run_id="run-uuid-single-table",
        status="QUEUED",
        submitted_at=datetime.now(timezone.utc),
    )

    with patch("api.security.auth.decode_session_token") as mock_dec, \
         patch("api.security.auth.get_all_source_tags", return_value=["school_system"]):
        mock_dec.return_value = MagicMock(
            username="admin",
            role="ADMIN",
            permissions=["*"],
        )
        res = client.post(
            "/api/v1/runs/trigger",
            json={
                "source_tag": "school_system",
                "source_table": "tblstudent",
                "execution_scope": "FULL",
                "request_id": "req-dataset-1",
            },
            headers=headers,
        )
        assert res.status_code == 201
        body = res.json()
        assert body["success"] is True
        assert body["data"]["run_id"] == "run-uuid-single-table"
        mock_submit.assert_called_once()
        called_sub_req = mock_submit.call_args[0][0]
        assert called_sub_req.source_tag == "school_system"
        assert called_sub_req.source_table == "tblstudent"


@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.submit_run")
def test_trigger_with_mismatched_topbar_context_header_succeeds(mock_submit, mock_active):
    """Verify stale X-Project-Context header from Topbar does NOT cause RBAC rejection when triggering an authorized source."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    # Topbar header set to msopforg_bureau, but user is triggering school_system
    headers = {
        "X-CSRF-Token": "test-csrf-token",
        "X-Project-Context": "msopforg_bureau",
    }

    mock_submit.return_value = SubmissionResult(
        accepted=True,
        request_id="req-ctx-mismatch",
        run_id="run-uuid-mismatch-resolved",
        status="QUEUED",
        submitted_at=datetime.now(timezone.utc),
    )

    with patch("api.security.auth.decode_session_token") as mock_dec, \
         patch("api.security.auth.get_all_source_tags", return_value=["school_system", "msopforg_bureau"]):
        mock_dec.return_value = MagicMock(
            username="operator_multi",
            role="OPERATOR",
            allowed_projects=["school_system", "msopforg_bureau"],
            permissions=["VIEW_TABLES", "VIEW_RUNS", "TRIGGER_RUNS"],
        )
        res = client.post(
            "/api/v1/runs/trigger",
            json={
                "source_tag": "school_system",
                "source_table": "tblfee",
                "execution_scope": "FULL",
                "request_id": "req-ctx-mismatch",
            },
            headers=headers,
        )
        assert res.status_code == 201
        body = res.json()
        assert body["success"] is True
        mock_submit.assert_called_once()
        called_ctx = mock_submit.call_args[1].get("context")
        assert called_ctx.project_id == "school_system"



@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.submit_run")
def test_concurrency_conflict_rejection_returns_409(mock_submit, mock_active):
    """Verify scope-claim overlap returns 409 Conflict with rejection metadata."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    headers = {"X-CSRF-Token": "test-csrf-token"}

    mock_submit.return_value = SubmissionResult(
        accepted=False,
        request_id="req-conflict",
        rejection_code="SCOPE_CLAIM_CONFLICT",
        rejection_reason="Active run already holds exclusive scope lock on table 'customers'.",
        existing_active_run_id="run-active-0001",
    )

    with patch("api.security.auth.decode_session_token") as mock_dec, \
         patch("api.security.auth.get_all_source_tags", return_value=["source_erp_db"]):
        mock_dec.return_value = MagicMock(
            username="admin",
            role="ADMIN",
            permissions=["*"],
        )
        res = client.post(
            "/api/v1/runs/trigger",
            json={"source_tag": "source_erp_db", "request_id": "req-conflict"},
            headers=headers,
        )
        assert res.status_code == 409
        body = res.json()
        assert body["success"] is False
        assert body["error"]["code"] == "SCOPE_CLAIM_CONFLICT"
        assert "Active run already holds" in body["error"]["message"]
        assert body["data"]["existing_active_run_id"] == "run-active-0001"


# -------------------------------------------------------------------------
# 3. Governed Run Cancellation Tests
# -------------------------------------------------------------------------
@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.get_run_detail")
def test_cancel_terminal_run_returns_400(mock_detail, mock_active):
    """Verify attempting to cancel an already completed run returns 400 Bad Request."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    headers = {"X-CSRF-Token": "test-csrf-token"}

    mock_detail.return_value = MagicMock(run_id="run-completed", status="SUCCESS")

    with patch("api.security.auth.decode_session_token") as mock_dec:
        mock_dec.return_value = MagicMock(username="admin", role="ADMIN", permissions=["*"])
        res = client.post("/api/v1/runs/run-completed/cancel", headers=headers)
        assert res.status_code == 400
        assert "already in terminal state" in res.json()["error"]["message"]


@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.get_run_detail")
@patch("api.routers.runs.ExecutionService.cancel_run", return_value=True)
def test_cancel_active_run_returns_200(mock_cancel, mock_detail, mock_active):
    """Verify cancelling an active run succeeds and updates state."""
    client.cookies.set("etl_session", "valid.mock.token")
    client.cookies.set("etl_csrf", "test-csrf-token")
    headers = {"X-CSRF-Token": "test-csrf-token"}

    mock_detail.return_value = MagicMock(run_id="run-running", status="RUNNING")

    with patch("api.security.auth.decode_session_token") as mock_dec:
        mock_dec.return_value = MagicMock(username="admin", role="ADMIN", permissions=["*"])
        res = client.post("/api/v1/runs/run-running/cancel", headers=headers)
        assert res.status_code == 200
        body = res.json()
        assert body["success"] is True
        assert body["data"]["success"] is True
        assert body["data"]["run_id"] == "run-running"
        mock_cancel.assert_called_once()


# -------------------------------------------------------------------------
# 4. Run Execution Log Streaming Tests
# -------------------------------------------------------------------------
@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.get_run_detail")
@patch("api.routers.runs.ExecutionService.stream_run_logs", return_value="[INFO] Extracted 500 rows\n[INFO] Gold upsert completed\n")
def test_stream_run_logs_returns_200(mock_stream, mock_detail, mock_active):
    """Verify streaming logs returns lines and content in standard envelope."""
    client.cookies.set("etl_session", "valid.mock.token")

    mock_detail.return_value = MagicMock(run_id="run-with-logs", status="RUNNING")

    with patch("api.security.auth.decode_session_token") as mock_dec:
        mock_dec.return_value = MagicMock(username="admin", role="ADMIN", permissions=["*"])
        res = client.get("/api/v1/runs/run-with-logs/logs?max_lines=50")
        assert res.status_code == 200
        body = res.json()
        assert body["success"] is True
        assert body["data"]["run_id"] == "run-with-logs"
        assert body["data"]["lines_returned"] == 2
        assert "Gold upsert completed" in body["data"]["logs"]


# -------------------------------------------------------------------------
# 5. Operational Runtime Telemetry & Rows Per Minute Tests
# -------------------------------------------------------------------------
@patch("api.security.auth.is_user_active_in_db", return_value=True)
@patch("api.routers.runs.ExecutionService.get_run_detail")
def test_run_detail_returns_operational_telemetry_and_rows_per_minute(mock_detail, mock_active):
    """Verify run detail accurately computes rows_per_minute, chunk_progress, and stall detection."""
    client.cookies.set("etl_session", "valid.mock.token")

    # Stalled heartbeat: 60s ago
    past_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=60)
    mock_run = MagicMock()
    mock_run.run_id = "run-telemetry-001"
    mock_run.parent_run_id = None
    mock_run.retry_attempt = 0
    mock_run.project_id = "school_system"
    mock_run.environment = "PROD"
    mock_run.pipeline_id = "run_pipeline"
    mock_run.dataset_id = "tblstudent"
    mock_run.status = "RUNNING"
    mock_run.started_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    mock_run.ended_at = None
    mock_run.last_heartbeat_at = past_heartbeat
    mock_run.elapsed_seconds = 120.0
    mock_run.rows_read = 10000
    mock_run.extracted_count = 10000
    mock_run.rows_written = 4000
    mock_run.throughput_rows_sec = 100.0
    mock_run.rolling_rows_per_second = 100.0
    mock_run.current_chunk = 4
    mock_run.total_chunks = 10
    mock_run.chunks_completed = 4
    mock_run.reconciliation_status = "PENDING"
    mock_run.is_stalled = False
    mock_run.stages = [
        MagicMock(stage_name="Bronze", status="SUCCESS", rows_processed=10000, throughput_rows_sec=100.0),
        MagicMock(stage_name="Silver", status="RUNNING", rows_processed=4000, throughput_rows_sec=100.0),
    ]
    mock_run.error_message = None

    mock_detail.return_value = mock_run

    with patch("api.security.auth.decode_session_token") as mock_dec:
        mock_dec.return_value = MagicMock(username="admin", role="ADMIN", permissions=["*"])
        res = client.get("/api/v1/runs/run-telemetry-001")
        assert res.status_code == 200
        body = res.json()
        assert body["success"] is True
        data = body["data"]

        # 100 rows/s * 60 = 6,000 rows/minute
        assert data["rows_per_minute"] == 6000.0
        assert data["rolling_rows_per_minute"] == 6000.0
        # 4 out of 10 chunks = 40%
        assert data["chunk_progress_pct"] == 40.0
        # Heartbeat > 45s => flagged is_stalled = True
        assert data["is_stalled"] is True
        assert data["last_heartbeat_age_seconds"] >= 55.0
        # Estimated remaining: (10000 - 4000) / 100 rps = 60s
        assert data["estimated_remaining_seconds"] == 60.0
        # Stage throughput
        assert data["stages"][0]["throughput_rows_min"] == 6000.0


@patch("api.security.auth.is_user_active_in_db", return_value=True)
def test_get_system_integrations_returns_governed_connectors(mock_active):
    """Verify GET /api/v1/system/integrations returns scrubbed source/target connection inventory."""
    client.cookies.set("etl_session", "valid.mock.token")
    with patch("api.security.auth.decode_session_token") as mock_dec, \
         patch("src.services.connection_service.ConnectionService.list_connections") as mock_conns:
        mock_dec.return_value = MagicMock(username="admin", role="ADMIN", permissions=["*"])
        mock_conns.return_value = [
            {
                "tag": "school_system",
                "connection_type": "DATABASE",
                "supports_source": True,
                "supports_target": False,
                "environment": "PROD",
                "db_type": "oracle",
                "server": "192.0.2.10",
                "port": 1521,
                "database_name": "xe",
                "schema_name": "SCOTT",
                "username": "c##school",
                "credential_reference": "SECRET_REF",
                "is_active": "y",
                "purpose": "Primary School Management System",
                "last_tested_at": "2026-10-19 12:00:00",
                "last_test_status": "SUCCESS",
                "last_latency_ms": 14.2,
            },
            {
                "tag": "land_prod",
                "connection_type": "DATABASE",
                "supports_source": False,
                "supports_target": True,
                "environment": "PROD",
                "db_type": "postgres",
                "server": "192.0.2.1",
                "port": 5432,
                "database_name": "land_prod",
                "schema_name": "public",
                "username": "etl_writer",
                "credential_reference": "SECRET_REF",
                "is_active": "y",
                "purpose": "Target Gold Medallion Store",
                "last_tested_at": "2026-10-19 12:00:00",
                "last_test_status": "SUCCESS",
                "last_latency_ms": 2.1,
            },
        ]
        res = client.get("/api/v1/system/integrations")
        assert res.status_code == 200
        body = res.json()
        assert body["success"] is True
        data = body["data"]
        assert data["total_count"] == 2
        assert data["source_count"] == 1
        assert data["target_count"] == 1
        assert data["active_count"] == 2

        # Invariant: Secret reference or raw password must NEVER be in response
        assert "credential_reference" not in data["integrations"][0]
        assert data["integrations"][0]["tag"] == "school_system"
        assert data["integrations"][0]["db_type"] == "oracle"
        assert data["integrations"][1]["tag"] == "land_prod"
        assert data["integrations"][1]["supports_target"] is True

