# tests/test_user_service_offline.py
"""
Offline Unit Tests for UserService domain logic.
Tests validation rules, self-drop protections, last-admin protections,
role creation, and permission lookups with mocked database connections.
"""
import pytest
from unittest.mock import MagicMock, patch
from src.security.user_service import UserService, AVAILABLE_SYSTEM_PERMISSIONS


@pytest.fixture
def mock_cm():
    cm = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    return cm, conn, cur


def test_list_permissions_offline():
    service = UserService(MagicMock())
    perms = service.list_permissions()
    assert len(perms) >= 15
    perm_ids = {p["id"] for p in perms}
    assert "MANAGE_USERS" in perm_ids
    assert "MANAGE_ROLES" in perm_ids
    assert "VIEW_AUDIT_LOG" in perm_ids


def test_create_user_empty_username_fails(mock_cm):
    cm, _, _ = mock_cm
    service = UserService(cm)
    with pytest.raises(ValueError, match="Username cannot be empty"):
        service.create_user(username="", password="password123", role_id=1)


def test_create_user_short_password_fails(mock_cm):
    cm, _, _ = mock_cm
    service = UserService(cm)
    with pytest.raises(ValueError, match="Password must be at least 6 characters"):
        service.create_user(username="validuser", password="short", role_id=1)


def test_drop_user_self_lockout_rejected(mock_cm):
    cm, _, cur = mock_cm
    service = UserService(cm)
    # Mock user exists
    cur.fetchone.return_value = (
        1, "admin", 1, "ADMIN", "Y", "Admin User", "admin@hiveflow.test", [], None, None
    )
    with pytest.raises(ValueError, match="cannot drop your own active account"):
        service.drop_user(user_id=1, actor_id="admin")


def test_drop_user_last_admin_rejected(mock_cm):
    cm, _, cur = mock_cm
    service = UserService(cm)
    # First fetchone: user exists (user_id=2, username="secondary_admin", role="ADMIN")
    # Second fetchone: other admins count = 0
    cur.fetchone.side_effect = [
        (2, "secondary_admin", 1, "ADMIN", "Y", "Second Admin", "sec@test.com", [], None, None),
        (0,),  # admin_count = 0
    ]
    with pytest.raises(ValueError, match="Cannot drop the last remaining ADMIN user"):
        service.drop_user(user_id=2, actor_id="primary_admin")


def test_drop_system_role_rejected(mock_cm):
    cm, _, cur = mock_cm
    service = UserService(cm)
    cur.fetchone.return_value = (1, "ADMIN", "System administrator", ["ALL"], 5)
    with pytest.raises(ValueError, match="Cannot drop system role"):
        service.drop_role(role_id=1, actor_id="admin")


def test_drop_role_with_active_users_rejected(mock_cm):
    cm, _, cur = mock_cm
    service = UserService(cm)
    # user_count is 3
    cur.fetchone.return_value = (10, "CUSTOM_ANALYST", "Analyst role", ["VIEW_RUN_HISTORY"], 3)
    with pytest.raises(ValueError, match="Referential Conflict"):
        service.drop_role(role_id=10, actor_id="admin")
