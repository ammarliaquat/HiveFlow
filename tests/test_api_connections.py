# tests/test_api_connections.py
"""
Automated Test Suite for HiveFlow Governed Connection Onboarding API.
Verifies RBAC protection, pre-save connectivity testing, secret hygiene,
and single-authority persistence through ConnectionService.
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from api.main import app
from api.models.auth import UserResponse
from api.security.auth import get_current_user

client = TestClient(app)

ADMIN_USER = UserResponse(
    username="admin",
    role="ADMIN",
    permissions=["*"],
    allowed_projects=["*"],
)

VIEWER_USER = UserResponse(
    username="viewer_user",
    role="VIEWER",
    permissions=["VIEW_TABLES"],
    allowed_projects=["source_erp_db"],
)


def setup_function():
    app.dependency_overrides.clear()
    client.cookies.clear()


def teardown_function():
    app.dependency_overrides.clear()
    client.cookies.clear()


SAMPLE_PROFILE = {
    "tag": "billing_mysql",
    "connection_type": "DATABASE",
    "supports_source": True,
    "supports_target": False,
    "environment": "PROD",
    "db_type": "mysql",
    "server": "192.0.2.10",
    "port": 3306,
    "database_name": "billing_db",
    "schema_name": "",
    "username": "billing_reader",
    "credential_reference": "${BILLING_DB_PASSWORD}",
    "purpose": "Billing Transaction Database",
    "owner": "Admin",
    "team": "Data Engineering",
}


def test_unauthenticated_test_and_save_rejected():
    """Verify testing or saving connections without auth token returns 401."""
    res_test = client.post("/api/v1/system/connections/test", json=SAMPLE_PROFILE)
    assert res_test.status_code == 401
    assert res_test.json()["success"] is False

    res_save = client.post("/api/v1/system/connections", json=SAMPLE_PROFILE)
    assert res_save.status_code == 401
    assert res_save.json()["success"] is False


def test_non_admin_save_connection_forbidden():
    """Verify viewer/operator cannot onboard new connections (403 Forbidden)."""
    app.dependency_overrides[get_current_user] = lambda: VIEWER_USER
    res = client.post("/api/v1/system/connections", json=SAMPLE_PROFILE)
    assert res.status_code == 403
    assert "Forbidden" in res.json()["error"]["message"]


def test_admin_test_connection_success():
    """Verify pre-save connectivity probe returns latency and SUCCESS."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    with patch("src.services.connection_service.ConnectionService.test_connection", return_value={"status": "SUCCESS", "latency_ms": 14, "error": ""}):
        res = client.post("/api/v1/system/connections/test", json=SAMPLE_PROFILE)
        assert res.status_code == 200
        data = res.json()["data"]
        assert data["status"] == "SUCCESS"
        assert data["latency_ms"] == 14
        assert data["error"] is None


def test_admin_test_connection_failure():
    """Verify pre-save probe returns FAILED with scrubbed error message."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    with patch("src.services.connection_service.ConnectionService.test_connection", return_value={"status": "FAILED", "latency_ms": 0, "error": "Connection refused (192.0.2.10:3306)"}):
        res = client.post("/api/v1/system/connections/test", json=SAMPLE_PROFILE)
        assert res.status_code == 200
        data = res.json()["data"]
        assert data["status"] == "FAILED"
        assert "Connection refused" in data["error"]


def test_admin_save_connection_persists_successfully():
    """Verify admin can save valid connection profile to catalog."""
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    with patch("src.services.connection_service.ConnectionService.save_connection", return_value=True) as mock_save, \
         patch("src.services.connection_service.ConnectionService.clear_cache") as mock_clear:
        res = client.post("/api/v1/system/connections", json=SAMPLE_PROFILE)
        assert res.status_code == 200
        data = res.json()["data"]
        assert data["tag"] == "billing_mysql"
        assert data["status"] == "SAVED"
        assert mock_save.called
        assert mock_clear.called
