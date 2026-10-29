# tests/test_api_discovery.py
import os
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("API_SECRET_KEY", "test-secret-key-32-characters-long-1234567890")

from api.main import app
from api.models.auth import UserResponse
from api.security.auth import get_current_user, get_connection_manager
from gui.models.submission import SubmissionResult


client = TestClient(app)

ADMIN_USER = UserResponse(
    username="admin",
    role="ADMIN",
    permissions=["ALL_PERMISSIONS", "MANAGE_USERS", "TRIGGER_RUNS"],
    allowed_projects=["source_erp_db", "opf_oracle_housing"],
)

OPERATOR_USER = UserResponse(
    username="operator",
    role="OPERATOR",
    permissions=["VIEW_TABLES", "VIEW_RUNS", "TRIGGER_RUNS"],
    allowed_projects=["source_erp_db"],
)

VIEWER_USER = UserResponse(
    username="viewer",
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


# -------------------------------------------------------------------------
# 1. Schema Discovery Crawler Tests
# -------------------------------------------------------------------------

@patch("gui.services.execution_service.ExecutionService.submit_run")
def test_crawl_endpoint_success(mock_submit):
    mock_submit.return_value = SubmissionResult(
        request_id="req-1234",
        accepted=True,
        run_id="crawl-run-1234",
        rejection_code=None,
        rejection_reason=None,
    )
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER


    response = client.post(
        "/api/v1/discovery/crawl",
        json={"source_tag": "source_erp_db"},
        headers={"X-CSRF-Token": "test-csrf"},
        cookies={"etl_csrf": "test-csrf"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["run_id"] == "crawl-run-1234"
    assert body["data"]["status"] == "DISPATCHED"


def test_crawl_endpoint_forbidden_for_viewer():
    app.dependency_overrides[get_current_user] = lambda: VIEWER_USER

    response = client.post(
        "/api/v1/discovery/crawl",
        json={"source_tag": "source_erp_db"},
        headers={"X-CSRF-Token": "test-csrf"},
        cookies={"etl_csrf": "test-csrf"},
    )
    assert response.status_code == 403


@patch("gui.services.execution_service.ExecutionService.get_run_status")
def test_crawl_status_endpoint(mock_status):
    mock_status.return_value = {
        "status": "RUNNING",
        "elapsed_seconds": 12.5,
        "error_message": None,
    }
    app.dependency_overrides[get_current_user] = lambda: OPERATOR_USER

    response = client.get("/api/v1/discovery/crawl/run-abc/status")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "RUNNING"
    assert body["data"]["elapsed_seconds"] == 12.5


@patch("gui.services.execution_service.ExecutionService.stream_run_logs")
def test_crawl_logs_endpoint(mock_logs):
    mock_logs.return_value = "Starting crawler introspection for source_erp_db...\nDiscovered 25 tables."
    app.dependency_overrides[get_current_user] = lambda: OPERATOR_USER

    response = client.get("/api/v1/discovery/crawl/run-abc/logs")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert "Discovered 25 tables" in body["data"]["logs"]


# -------------------------------------------------------------------------
# 2. Tier-1 Master Catalog & Columns Tests
# -------------------------------------------------------------------------

@patch("api.routers.discovery.get_catalog_summary")
def test_get_catalog_summary(mock_catalog):
    mock_df = pd.DataFrame([
        {
            "table_master_key": 101,
            "source_tag": "source_erp_db",
            "database_type": "mysql",
            "schema_name": "opforg",
            "source_table": "tbl_users",
            "table_type": "BASE TABLE",
            "row_count": 5000,
            "column_count": 12,
            "business_key": "user_id",
            "key_source": "PRIMARY KEY",
            "key_confidence": 100.0,
            "cdc_columns": "updated_at",
            "cdc_strategy": "TIMESTAMP_HIGH_WATERMARK",
            "cdc_confidence": 95.0,
            "cdc_health": "HEALTHY",
            "cdc_health_details": "Valid index",
            "insert_date": "2026-10-23 10:00:00",
        }
    ])
    mock_catalog.return_value = mock_df
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    response = client.get("/api/v1/discovery/catalog?source_tag=source_erp_db")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["total_tables"] == 1
    assert body["data"]["total_rows"] == 5000
    assert body["data"]["total_columns"] == 12
    assert body["data"]["tables"][0]["source_table"] == "tbl_users"


@patch("api.routers.discovery.get_catalog_columns")
def test_get_catalog_columns_endpoint(mock_cols):
    mock_df = pd.DataFrame([
        {
            "Column Name": "user_id",
            "Source Datatype": "int",
            "Max Length": 4,
            "Precision": 10,
            "Scale": 0,
            "Sequence": 1,
            "Nullable": "NO",
        },
        {
            "Column Name": "username",
            "Source Datatype": "varchar",
            "Max Length": 50,
            "Precision": None,
            "Scale": None,
            "Sequence": 2,
            "Nullable": "NO",
        },
    ])
    mock_cols.return_value = mock_df
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    response = client.get("/api/v1/discovery/catalog/columns/101")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert len(body["data"]) == 2
    assert body["data"][0]["column_name"] == "user_id"
    assert body["data"][1]["is_nullable"] == "NO"


# -------------------------------------------------------------------------
# 3. Source-to-Target Promotion & Subscriptions Tests
# -------------------------------------------------------------------------

@patch("api.routers.discovery.get_target_tags")
def test_get_targets(mock_targets):
    mock_targets.return_value = ["postgres_gold", "mssql_reporting"]
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    response = client.get("/api/v1/discovery/targets")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert "postgres_gold" in body["data"]


@patch("api.routers.discovery.get_catalog_tables")
@patch("api.routers.discovery.get_pipeline_tables")
def test_get_unpromoted_tables(mock_pipeline, mock_catalog):
    mock_catalog.return_value = ["table_a", "table_b", "table_c"]
    mock_pipeline.return_value = pd.DataFrame([
        {"source_table": "table_a", "target_tag": "postgres_gold"}
    ])
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    response = client.get("/api/v1/discovery/unpromoted?source_tag=source_erp_db&target_tag=postgres_gold")
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["unpromoted_tables"] == ["table_b", "table_c"]
    assert body["data"]["promoted_tables"] == ["table_a"]


@patch("gui.services.promotion_service.PromotionService.promote_catalog_datasets")
def test_promote_datasets_success(mock_promote):
    mock_promote.return_value = {
        "successful": ["table_b", "table_c"],
        "failed": [],
        "total": 2,
        "priority": 1,
        "priority_updated": 2,
        "policy_refresh": {
            "updated": ["table_b"],
            "unchanged": ["table_c"],
            "skipped": [],
            "failed": [],
        },
    }
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    response = client.post(
        "/api/v1/discovery/promote",
        json={
            "source_tag": "source_erp_db",
            "target_tag": "postgres_gold",
            "table_names": ["table_b", "table_c"],
            "priority": 1,
            "steward_confirmed": True,
        },
        headers={"X-CSRF-Token": "test-csrf"},
        cookies={"etl_csrf": "test-csrf"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["successful"] == ["table_b", "table_c"]
    assert "Successfully promoted 2 dataset(s)" in body["data"]["message"]


def test_promote_datasets_requires_steward_confirmation():
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    response = client.post(
        "/api/v1/discovery/promote",
        json={
            "source_tag": "source_erp_db",
            "target_tag": "postgres_gold",
            "table_names": ["table_b"],
            "priority": 1,
            "steward_confirmed": False,
        },
        headers={"X-CSRF-Token": "test-csrf"},
        cookies={"etl_csrf": "test-csrf"},
    )
    assert response.status_code == 400
    assert "Steward declaration confirmation is required" in response.json()["error"]["message"]


def test_promote_datasets_requires_admin_role():
    app.dependency_overrides[get_current_user] = lambda: OPERATOR_USER

    response = client.post(
        "/api/v1/discovery/promote",
        json={
            "source_tag": "source_erp_db",
            "target_tag": "postgres_gold",
            "table_names": ["table_b"],
            "priority": 1,
            "steward_confirmed": True,
        },
        headers={"X-CSRF-Token": "test-csrf"},
        cookies={"etl_csrf": "test-csrf"},
    )
    assert response.status_code == 403


# -------------------------------------------------------------------------
# 4. Schema Drift & System Cache Clear Tests
# -------------------------------------------------------------------------

def test_drift_check_endpoint():
    mock_cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = [
        {
            "source_tag": "source_erp_db",
            "source_table": "tbl_orders",
            "etl_key": "order_id",
            "catalog_key": "id",
            "alert": "[WARN] BUSINESS KEY MISMATCH",
        }
    ]
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get_metadata_db_connection.return_value = mock_conn
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    app.dependency_overrides[get_connection_manager] = lambda: mock_cm

    response = client.post(
        "/api/v1/discovery/drift-check?source_tag=source_erp_db",
        headers={"X-CSRF-Token": "test-csrf"},
        cookies={"etl_csrf": "test-csrf"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["total_mismatches"] == 1
    assert body["data"]["status"] == "DRIFT_DETECTED"
    assert body["data"]["mismatches"][0]["source_table"] == "tbl_orders"



def test_clear_system_cache_endpoint():
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER

    response = client.post(
        "/api/v1/system/cache/clear",
        headers={"X-CSRF-Token": "test-csrf"},
        cookies={"etl_csrf": "test-csrf"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "CLEARED"
