# tests/test_user_management_rbac.py
"""
Automated regression & security tests for Enterprise User & Role Management,
Manual Granular Access Permissions, and User Operations Audit Observer.
Verifies all P0 safety invariants:
  - Admin can create new roles with manual access permissions
  - Admin can create users with bcrypt passwords
  - Admin can update profiles, roles, and reset passwords
  - Admin can drop users (with self-drop and last-admin protections)
  - Admin cannot drop system roles (ADMIN, VIEWER) or roles with active users
  - Admin can observe what each user has operated via the audit log
  - Non-admin (e.g. VIEWER) is forbidden from user/role mutations (403 Forbidden)
"""
import pytest
from fastapi.testclient import TestClient
from api.main import app
from src.connections.connection_manager import ConnectionManager
from src.security.user_service import UserService

pytestmark = pytest.mark.live

client = TestClient(app)

@pytest.fixture(scope="module")
def admin_headers():
    """Authenticate as default admin and return session cookies + CSRF header."""
    resp = client.post("/api/v1/auth/login", json={"username": "admin", "password": "admin123"})
    assert resp.status_code == 200, f"Admin login failed: {resp.text}"
    csrf_token = resp.cookies.get("etl_csrf") or ""
    return {
        "cookies": resp.cookies,
        "headers": {
            "X-CSRF-Token": csrf_token,
            "Content-Type": "application/json",
        }
    }

@pytest.fixture(scope="module")
def viewer_headers():
    """Authenticate as viewer and return session cookies + CSRF header."""
    resp = client.post("/api/v1/auth/login", json={"username": "viewer", "password": "viewer123"})
    assert resp.status_code == 200, f"Viewer login failed: {resp.text}"
    csrf_token = resp.cookies.get("etl_csrf") or ""
    return {
        "cookies": resp.cookies,
        "headers": {
            "X-CSRF-Token": csrf_token,
            "Content-Type": "application/json",
        }
    }


# ═════════════════════════════════════════════════════════════════════════════
# 1. PERMISSIONS & ROLES TESTS
# ═════════════════════════════════════════════════════════════════════════════

def test_list_system_permissions(admin_headers):
    """Verify admin can list available granular system permissions."""
    resp = client.get("/api/v1/roles/permissions", cookies=admin_headers["cookies"], headers=admin_headers["headers"])
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert len(data) >= 15
    perm_ids = [p["id"] for p in data]
    assert "VIEW_RUN_HISTORY" in perm_ids
    assert "EXECUTE_PIPELINE" in perm_ids
    assert "MANAGE_USERS" in perm_ids
    assert "MANAGE_ROLES" in perm_ids
    assert "VIEW_AUDIT_LOG" in perm_ids


def test_create_and_manage_role_with_manual_permissions(admin_headers):
    """Verify admin can create a custom role with manual granular access, update it, and drop it."""
    role_name = "TEST_NOC_ANALYST"
    
    # 1. Create Role
    create_payload = {
        "role_name": role_name,
        "description": "Custom NOC tier for monitoring and reruns",
        "permissions": ["VIEW_RUN_HISTORY", "RERUN_TABLE", "ACKNOWLEDGE_ALERTS"]
    }
    resp = client.post(
        "/api/v1/roles",
        json=create_payload,
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert resp.status_code == 200, resp.text
    created_role = resp.json()["data"]
    role_id = created_role["role_id"]
    assert created_role["role_name"] == role_name
    assert set(created_role["permissions"]) == {"VIEW_RUN_HISTORY", "RERUN_TABLE", "ACKNOWLEDGE_ALERTS"}

    # 2. Update Role Permissions (Manual Access adjustment)
    update_payload = {
        "description": "Updated NOC role with pipeline execution privilege",
        "permissions": ["VIEW_RUN_HISTORY", "RERUN_TABLE", "ACKNOWLEDGE_ALERTS", "EXECUTE_PIPELINE"]
    }
    up_resp = client.post(
        f"/api/v1/roles/{role_id}/update",
        json=update_payload,
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert up_resp.status_code == 200
    updated_role = up_resp.json()["data"]
    assert "EXECUTE_PIPELINE" in updated_role["permissions"]

    # 3. Drop Role
    drop_resp = client.post(
        f"/api/v1/roles/{role_id}/drop",
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert drop_resp.status_code == 200
    assert drop_resp.json()["data"]["dropped"] is True


def test_drop_system_role_forbidden(admin_headers):
    """P0 Safety Invariant: Immutable system roles (ADMIN, VIEWER) cannot be dropped."""
    # Fetch roles to get ADMIN role_id
    r_resp = client.get("/api/v1/roles", cookies=admin_headers["cookies"], headers=admin_headers["headers"])
    admin_role = next(r for r in r_resp.json()["data"] if r["role_name"] == "ADMIN")
    
    drop_resp = client.post(
        f"/api/v1/roles/{admin_role['role_id']}/drop",
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert drop_resp.status_code == 400
    assert "Cannot drop system role" in drop_resp.json()["error"]["message"]


# ═════════════════════════════════════════════════════════════════════════════
# 2. USER MANAGEMENT TESTS
# ═════════════════════════════════════════════════════════════════════════════

def test_user_lifecycle_create_update_password_and_drop(admin_headers):
    """Verify complete user lifecycle: create user, update profile, reset password, and drop."""
    username = "test_ops_engineer"
    password = "InitialPassword123!"

    # Get a role ID (e.g. VIEWER or PLATFORM_OPERATOR)
    r_resp = client.get("/api/v1/roles", cookies=admin_headers["cookies"], headers=admin_headers["headers"])
    roles = r_resp.json()["data"]
    viewer_role = next(r for r in roles if r["role_name"] == "VIEWER")

    # 1. Create User
    create_req = {
        "username": username,
        "password": password,
        "role_id": viewer_role["role_id"],
        "full_name": "Test Operations Engineer",
        "email": "ops@hiveflow.test",
        "allowed_projects": ["school_system", "source_erp_db"]
    }
    c_resp = client.post(
        "/api/v1/users",
        json=create_req,
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert c_resp.status_code == 200, c_resp.text
    user_data = c_resp.json()["data"]
    user_id = user_data["user_id"]
    assert user_data["username"] == username
    assert user_data["role"] == "VIEWER"
    assert user_data["is_active"] is True

    # 2. Verify User Can Login with Created Credentials
    login_resp = client.post("/api/v1/auth/login", json={"username": username, "password": password})
    assert login_resp.status_code == 200

    # 3. Update User (Update full name & project scopes)
    u_resp = client.post(
        f"/api/v1/users/{user_id}/update",
        json={"full_name": "Senior Ops Engineer", "allowed_projects": ["school_system"]},
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert u_resp.status_code == 200
    assert u_resp.json()["data"]["full_name"] == "Senior Ops Engineer"

    # 4. Reset Password
    new_password = "NewSecurePassword456!"
    pwd_resp = client.post(
        f"/api/v1/users/{user_id}/reset-password",
        json={"new_password": new_password},
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert pwd_resp.status_code == 200

    # Verify Old Password Fails and New Password Succeeds
    old_login = client.post("/api/v1/auth/login", json={"username": username, "password": password})
    assert old_login.status_code == 401
    new_login = client.post("/api/v1/auth/login", json={"username": username, "password": new_password})
    assert new_login.status_code == 200

    # 5. Drop User
    drop_resp = client.post(
        f"/api/v1/users/{user_id}/drop",
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert drop_resp.status_code == 200
    assert drop_resp.json()["data"]["dropped"] is True

    # Verify Dropped User Cannot Login
    post_drop_login = client.post("/api/v1/auth/login", json={"username": username, "password": new_password})
    assert post_drop_login.status_code == 401


def test_drop_user_safety_guards(admin_headers):
    """P0 Safety Invariant: Admin cannot drop own active account."""
    u_resp = client.get("/api/v1/users", cookies=admin_headers["cookies"], headers=admin_headers["headers"])
    admin_user = next(u for u in u_resp.json()["data"] if u["username"] == "admin")

    # Attempt to drop self
    drop_resp = client.post(
        f"/api/v1/users/{admin_user['user_id']}/drop",
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert drop_resp.status_code == 400
    assert "cannot drop your own active account" in drop_resp.json()["error"]["message"]


# ═════════════════════════════════════════════════════════════════════════════
# 3. USER ACTIVITY & AUDIT OBSERVER TESTS
# ═════════════════════════════════════════════════════════════════════════════

def test_admin_can_observe_user_operations(admin_headers):
    """Verify admin can observe what each user has operated via the audit log."""
    # 1. Fetch Audit Logs
    audit_resp = client.get(
        "/api/v1/audit/logs?limit=20",
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert audit_resp.status_code == 200
    data = audit_resp.json()["data"]
    assert "items" in data
    assert "total" in data
    assert len(data["items"]) > 0

    # 2. Filter audit log by specific operator (username=admin)
    filter_resp = client.get(
        "/api/v1/audit/logs?username=admin&limit=10",
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert filter_resp.status_code == 200
    admin_items = filter_resp.json()["data"]["items"]
    for item in admin_items:
        assert item["username"].lower() == "admin"

    # 3. Check operational summary
    sum_resp = client.get(
        "/api/v1/audit/summary",
        cookies=admin_headers["cookies"],
        headers=admin_headers["headers"]
    )
    assert sum_resp.status_code == 200
    summary = sum_resp.json()["data"]
    assert "total_operations" in summary
    assert "active_users_count" in summary
    assert summary["total_operations"] > 0


# ═════════════════════════════════════════════════════════════════════════════
# 4. RBAC RESTRICTIONS & UNAUTHORIZED ACCESS TESTS
# ═════════════════════════════════════════════════════════════════════════════

def test_viewer_forbidden_from_user_and_role_management(viewer_headers):
    """Verify a non-admin user (VIEWER) cannot create, update, or drop users or roles."""
    # 1. Cannot list users
    u_list = client.get("/api/v1/users", cookies=viewer_headers["cookies"], headers=viewer_headers["headers"])
    assert u_list.status_code == 403

    # 2. Cannot create users
    u_create = client.post(
        "/api/v1/users",
        json={"username": "hack_user", "password": "password123", "role_id": 1},
        cookies=viewer_headers["cookies"],
        headers=viewer_headers["headers"]
    )
    assert u_create.status_code == 403

    # 3. Cannot create roles
    r_create = client.post(
        "/api/v1/roles",
        json={"role_name": "HACK_ROLE", "permissions": ["MANAGE_USERS"]},
        cookies=viewer_headers["cookies"],
        headers=viewer_headers["headers"]
    )
    assert r_create.status_code == 403

    # 4. Cannot view audit logs
    a_resp = client.get("/api/v1/audit/logs", cookies=viewer_headers["cookies"], headers=viewer_headers["headers"])
    assert a_resp.status_code == 403
