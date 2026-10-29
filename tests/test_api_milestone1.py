# tests/test_api_milestone1.py
import os
import secrets
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
import pandas as pd
import pytest
from fastapi.testclient import TestClient

# Ensure test secret key is present
os.environ.setdefault("API_SECRET_KEY", "test-secret-key-32-characters-long-1234567890")

from api.main import app
from api.config import COOKIE_NAME, CSRF_COOKIE_NAME, get_secret_key
from api.models.auth import UserResponse
from api.security.auth import (
    get_current_user,
    create_session_token,
    decode_session_token,
    is_user_active_in_db,
)
from gui.models.run import RunModel, RunStageModel

client = TestClient(app)

ADMIN_USER = UserResponse(
    username="admin",
    role="ADMIN",
    permissions=["ALL_PERMISSIONS", "MANAGE_USERS"],
    allowed_projects=["source_erp_db", "opf_oracle_housing"],
)

OPERATOR_USER = UserResponse(
    username="operator_opforg",
    role="OPERATOR",
    permissions=["VIEW_TABLES", "VIEW_RUNS"],
    allowed_projects=["source_erp_db"],
)

def setup_function():
    """Ensure clean dependency overrides and cookies before each test."""
    app.dependency_overrides.clear()
    client.cookies.clear()

def teardown_function():
    """Clean up dependency overrides and cookies after each test."""
    app.dependency_overrides.clear()
    client.cookies.clear()


# -------------------------------------------------------------------------
# 1. Root & Discovery Tests
# -------------------------------------------------------------------------
def test_api_root():
    """Verify /api/v1 root returns service metadata in standard envelope."""
    response = client.get("/api/v1")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "OPERATIONAL"
    assert "HiveFlow" in body["data"]["service"]
    assert "timestamp" in body["meta"]


# -------------------------------------------------------------------------
# 2. Authentication, Secret Handling, and CSRF Tests
# -------------------------------------------------------------------------
@patch("api.routers.auth.authenticate_credentials")
def test_login_success_sets_cookies(mock_auth):
    """Verify login sets both HttpOnly session cookie and readable CSRF cookie."""
    mock_auth.return_value = ADMIN_USER

    response = client.post(
        "/api/v1/auth/login",
        json={"username": "admin", "password": "admin123"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["username"] == "admin"
    assert body["data"]["role"] == "ADMIN"
    assert COOKIE_NAME in response.cookies
    assert CSRF_COOKIE_NAME in response.cookies


@patch("api.routers.auth.authenticate_credentials")
def test_login_failure(mock_auth):
    """Verify bad credentials return 401 with standard error envelope."""
    mock_auth.return_value = None

    response = client.post(
        "/api/v1/auth/login",
        json={"username": "admin", "password": "wrongpassword999"},
    )
    assert response.status_code == 401
    body = response.json()
    assert body["success"] is False
    assert body["error"]["code"] == "UNAUTHORIZED"
    assert "Invalid username or password" in body["error"]["message"]


def test_unauthenticated_protected_routes():
    """Verify protected endpoints return 401 when unauthenticated."""
    assert client.get("/api/v1/dashboard/kpis").status_code == 401
    assert client.get("/api/v1/pipelines").status_code == 401
    assert client.get("/api/v1/runs").status_code == 401
    assert client.get("/api/v1/system/health").status_code == 401


@patch("api.routers.auth.authenticate_credentials")
@patch("api.security.auth.is_user_active_in_db", return_value=True)
def test_csrf_protection_and_token_revocation_on_logout(mock_active, mock_auth):
    """Verify CSRF token is required on logout when cookie-authenticated, and token is revoked."""
    mock_auth.return_value = ADMIN_USER

    # 1. Login to receive session and CSRF cookies
    login_res = client.post(
        "/api/v1/auth/login",
        json={"username": "admin", "password": "admin123"},
    )
    assert login_res.status_code == 200
    csrf_token = login_res.cookies[CSRF_COOKIE_NAME]
    session_token = login_res.cookies[COOKIE_NAME]

    # 2. Logout without CSRF header must fail with 403
    fail_logout = client.post("/api/v1/auth/logout")
    assert fail_logout.status_code == 403
    assert "CSRF validation failed" in fail_logout.json()["error"]["message"]

    # 3. Logout with matching CSRF header succeeds
    ok_logout = client.post("/api/v1/auth/logout", headers={"X-CSRF-Token": csrf_token})
    assert ok_logout.status_code == 200
    assert ok_logout.json()["data"]["logged_out"] is True

    # 4. Attempting to reuse revoked session token must fail with 401
    client.cookies.set(COOKIE_NAME, session_token)
    me_res = client.get("/api/v1/auth/me")
    assert me_res.status_code == 401


@patch("api.security.auth.is_user_active_in_db", return_value=True)
def test_session_token_expiry_rejected(mock_active):
    """Verify expired session token is rejected with 401."""
    expired_token = create_session_token(
        {"username": "admin", "role": "ADMIN", "permissions": []},
        expires_delta=timedelta(seconds=-10),
    )
    client.cookies.set(COOKIE_NAME, expired_token)
    res = client.get("/api/v1/auth/me")
    assert res.status_code == 401
    assert "Invalid or expired session token" in res.json()["error"]["message"]



def test_missing_application_secret_fails_closed():
    """Verify system fails closed when API_SECRET_KEY is absent in production."""
    with patch.dict(os.environ, {"API_SECRET_KEY": "", "APP_ENV": "production"}, clear=False):
        with pytest.raises(RuntimeError) as exc_info:
            get_secret_key()
        assert "API_SECRET_KEY" in str(exc_info.value)


@patch("api.security.auth.get_connection_manager")
def test_db_outage_fails_closed_in_active_check(mock_get_cm):
    """Verify database unavailability during active user check fails closed (returns False)."""
    mock_get_cm.side_effect = Exception("Database connection pool exhausted")
    # Must return False, not True
    assert is_user_active_in_db("some_user") is False


# -------------------------------------------------------------------------
# 3. Cross-Project Isolation & RBAC Denial Tests
# -------------------------------------------------------------------------
def test_cross_project_access_denied_via_header():
    """Verify non-admin cannot access unauthorized project via X-Project-Context header."""
    app.dependency_overrides[get_current_user] = lambda: OPERATOR_USER

    # Operator is only allowed 'source_erp_db', tries to access 'opf_oracle_housing'
    res = client.get(
        "/api/v1/pipelines",
        headers={"X-Project-Context": "opf_oracle_housing"},
    )
    assert res.status_code == 403
    assert res.json()["error"]["code"] == "FORBIDDEN"
    assert "RBAC Violation" in res.json()["error"]["message"]


def test_cross_project_access_denied_via_query_param():
    """Verify non-admin cannot access unauthorized project via source_tag query param."""
    app.dependency_overrides[get_current_user] = lambda: OPERATOR_USER

    # Pipeline listing attempt
    res_pipe = client.get("/api/v1/pipelines?source_tag=opf_oracle_housing")
    assert res_pipe.status_code == 403
    assert "RBAC Violation" in res_pipe.json()["error"]["message"]

    # Runs listing attempt
    res_runs = client.get("/api/v1/runs?source_tag=opf_oracle_housing")
    assert res_runs.status_code == 403
    assert "RBAC Violation" in res_runs.json()["error"]["message"]


@patch("api.routers.runs.ExecutionService.get_run_detail")
def test_cross_project_run_detail_denied(mock_get_run):
    """Verify non-admin cannot read run details belonging to another project."""
    app.dependency_overrides[get_current_user] = lambda: OPERATOR_USER

    # Run belongs to opf_oracle_housing
    mock_get_run.return_value = RunModel(
        run_id="run-housing-789",
        project_id="opf_oracle_housing",
        environment="PROD",
        status="SUCCESS",
        started_at=datetime.now(timezone.utc),
        ended_at=datetime.now(timezone.utc),
        elapsed_seconds=50.0,
        stages=[],
        reconciliation_status="CONVERGED",
    )

    res = client.get("/api/v1/runs/run-housing-789")
    assert res.status_code == 403
    assert "RBAC Violation" in res.json()["error"]["message"]


@patch("api.routers.pipelines.get_pipeline_tables")
def test_pipeline_not_found_returns_404(mock_tables):
    """Verify non-existent pipeline table returns 404 in standard error envelope."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    mock_tables.return_value = pd.DataFrame()  # empty

    res = client.get("/api/v1/pipelines/source_erp_db/non_existent_table")
    assert res.status_code == 404
    body = res.json()
    assert body["success"] is False
    assert body["error"]["code"] == "NOT_FOUND"


# -------------------------------------------------------------------------
# 4. Service Boundaries & Telemetry Tests
# -------------------------------------------------------------------------
@patch("api.routers.dashboard.get_dashboard_kpis")
@patch("api.routers.dashboard.get_recent_activity")
def test_authenticated_dashboard_flow(mock_act, mock_kpis):
    """Verify authenticated session reads dashboard KPIs and activity."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    mock_kpis.return_value = {
        "running": 1,
        "success_today": 42,
        "failed_today": 0,
        "last_run_end": "2026-10-16 12:00:00",
        "stalled_count": 0,
    }
    mock_act.return_value = pd.DataFrame([
        {
            "run_id": "test-run-123",
            "layer": "gold",
            "source": "source_erp_db",
            "target_tag": "eland",
            "table": "tbltasks",
            "target_table": "tbltasks",
            "status": "SUCCESS",
            "source_count": 500,
            "target_count": 500,
            "elapsed_sec": 4.2,
            "end_time": "2026-10-16 12:00:00",
        }
    ])

    kpi_res = client.get("/api/v1/dashboard/kpis")
    assert kpi_res.status_code == 200
    assert kpi_res.json()["data"]["running"] == 1

    act_res = client.get("/api/v1/dashboard/activity?limit=5")
    assert act_res.status_code == 200
    assert len(act_res.json()["data"]["activities"]) == 1


@patch("api.routers.pipelines.get_pipeline_tables")
@patch("api.routers.pipelines.get_table_columns")
def test_authenticated_pipelines_flow(mock_cols, mock_tables):
    """Verify pipeline catalogue endpoints with authenticated user."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    mock_tables.return_value = pd.DataFrame([
        {
            "table_id": 101,
            "source_tag": "source_erp_db",
            "target_tag": "eland",
            "priority": 1,
            "source_table": "users",
            "target_table": "users",
            "is_active": "y",
            "is_cdc": "y",
            "cdc_strategy": "TIMESTAMP_HIGH_WATERMARK",
            "columns_unique_key": "user_id",
            "execution_authority_mode": "GOVERNED",
            "last_success_date": "2026-10-16",
            "last_success_timestamp": "2026-10-16 10:00:00",
            "landing_mode": "METADATA",
        }
    ])
    mock_cols.return_value = pd.DataFrame([
        {"column_id": 1, "column_name": "user_id", "column_type": "INTEGER", "is_primary_key": True}
    ])

    pipe_res = client.get("/api/v1/pipelines")
    assert pipe_res.status_code == 200
    assert pipe_res.json()["data"][0]["source_table"] == "users"
    assert pipe_res.json()["data"][0]["is_active"] is True
    assert pipe_res.json()["data"][0]["is_cdc"] is True

    detail_res = client.get("/api/v1/pipelines/source_erp_db/users")
    assert detail_res.status_code == 200
    assert detail_res.json()["data"]["pipeline"]["source_table"] == "users"
    assert detail_res.json()["data"]["pipeline"]["is_active"] is True
    assert detail_res.json()["data"]["pipeline"]["is_cdc"] is True



@patch("api.routers.runs.get_run_history")
@patch("api.routers.runs.ExecutionService.get_run_detail")
def test_authenticated_runs_flow(mock_detail, mock_history):
    """Verify execution runs history and detail endpoint."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    mock_history.return_value = pd.DataFrame([
        {
            "run_id": "run-abc-123",
            "layer": "run_pipeline",
            "source_tag": "source_erp_db",
            "target_tag": "eland",
            "source_table": "users",
            "target_table": "users",
            "priority": 1,
            "status": "SUCCESS",
            "start_time": "2026-10-16 11:00:00",
            "end_time": "2026-10-16 11:02:00",
            "elapsed_sec": 120.0,
            "source_count": 1000,
            "target_count": 1000,
            "error_message": None,
        }
    ])
    mock_detail.return_value = RunModel(
        run_id="run-abc-123",
        project_id="source_erp_db",
        environment="PROD",
        status="SUCCESS",
        started_at=datetime.now(timezone.utc),
        ended_at=datetime.now(timezone.utc),
        elapsed_seconds=120.0,
        stages=[RunStageModel("Extract", status="SUCCESS", rows_processed=1000)],
        reconciliation_status="CONVERGED",
    )

    runs_res = client.get("/api/v1/runs?limit=5")
    assert runs_res.status_code == 200
    assert runs_res.json()["data"][0]["run_id"] == "run-abc-123"

    detail_res = client.get("/api/v1/runs/run-abc-123")
    assert detail_res.status_code == 200
    assert detail_res.json()["data"]["reconciliation_status"] == "CONVERGED"


@patch("api.routers.system.get_connection_manager")
@patch("api.routers.system.HealthChecker")
@patch("src.connections.schema_certifier.SchemaCertifier")
def test_system_health_flow_accurate_probes(mock_cert_cls, mock_checker_cls, mock_get_cm):
    """Verify health endpoint declares partial scope and does not fake Spark health."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    mock_get_cm.return_value = MagicMock()

    mock_checker_instance = MagicMock()
    mock_checker_instance.check_repository_db.return_value = (True, "OK")
    mock_checker_cls.return_value = mock_checker_instance

    mock_cert_instance = MagicMock()
    mock_cert_instance.certify.return_value = None
    mock_cert_cls.return_value = mock_cert_instance

    health_res = client.get("/api/v1/system/health")
    assert health_res.status_code == 200
    body = health_res.json()
    assert body["success"] is True
    assert body["data"]["overall_status"] == "PARTIAL_HEALTHY"
    assert "Partial:" in body["data"]["scope_notice"]

    probes = {p["target_component"]: p for p in body["data"]["probes"]}
    assert probes["METADATA_DB"]["status"] == "REACHABLE"
    assert probes["MIGRATION_LEDGER"]["status"] == "CERTIFIED"
    assert probes["COMPUTE_ENGINE"]["status"] == "UNMONITORED"


# -------------------------------------------------------------------------
# 5. SPA Routing & Client Navigation Tests
# -------------------------------------------------------------------------
def test_spa_direct_route_navigation():
    """Verify direct browser navigation / refresh to SPA client routes returns 200 index.html."""
    for route in ["/", "/dashboard", "/pipelines", "/runs", "/health"]:
        res = client.get(route)
        assert res.status_code == 200
        assert "<!doctype html>" in res.text.lower()
        assert "HiveFlow" in res.text

    # Verify invalid API route still returns 404 JSON, NOT the SPA index.html
    api_res = client.get("/api/v1/invalid_endpoint_xyz")
    assert api_res.status_code == 404
    assert api_res.json()["success"] is False
    assert api_res.json()["error"]["code"] == "NOT_FOUND"

