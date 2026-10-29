# src/security/user_service.py
"""
UserService — Domain service for User, Role, and Audit Trail Management.
Provides secure CRUD operations over metadata.gui_users, metadata.gui_roles,
metadata.gui_role_permissions, and metadata.gui_audit_log with bcrypt hashing,
safety invariants (cannot drop self or last admin), and forensic audit emission.
"""
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
import bcrypt
from datetime import datetime, timezone

from src.connections.connection_manager import ConnectionManager
from src.security.audit_logger import AuditLogger

logger = logging.getLogger("UserService")

SYSTEM_ROLES = {"ADMIN", "VIEWER"}

AVAILABLE_SYSTEM_PERMISSIONS = [
    # Observability & Monitoring
    {"id": "VIEW_RUN_HISTORY", "category": "Observability", "description": "View execution logs, run metrics, and pipeline status"},
    {"id": "VIEW_SENSITIVE_LOGS", "category": "Observability", "description": "View detailed stack traces and debugging logs"},
    {"id": "VIEW_AUDIT_LOG", "category": "Observability", "description": "View user activity and audit trail of platform operations"},
    {"id": "ACKNOWLEDGE_ALERTS", "category": "Observability", "description": "Acknowledge system alerts and drift notifications"},
    
    # Execution & Operations
    {"id": "EXECUTE_PIPELINE", "category": "Execution", "description": "Trigger pipeline runs across Bronze, Silver, and Gold"},
    {"id": "CANCEL_PIPELINE", "category": "Execution", "description": "Cancel currently running or queued pipeline jobs"},
    {"id": "RERUN_TABLE", "category": "Execution", "description": "Queue table reruns and overrides"},
    {"id": "RESET_WATERMARK", "category": "Execution", "description": "Reset CDC watermarks for full re-extraction"},
    
    # Metadata & Governance
    {"id": "PROMOTE_METADATA", "category": "Governance", "description": "Promote discovered catalog tables into active ETL pipeline"},
    {"id": "MANAGE_METADATA", "category": "Governance", "description": "Edit table priorities, business keys, and column mappings"},
    {"id": "APPROVE_SCHEMA_DRIFT", "category": "Governance", "description": "Approve schema drift recommendations"},
    {"id": "REJECT_SCHEMA_DRIFT", "category": "Governance", "description": "Reject schema drift recommendations"},
    {"id": "EDIT_DQ_RULES", "category": "Governance", "description": "Configure data quality checks and thresholds"},
    {"id": "PURGE_RETENTION_DATA", "category": "Governance", "description": "Purge historical staging and audit records"},
    
    # Integrations & Connectors
    {"id": "VIEW_CONNECTION_DETAILS", "category": "Integrations", "description": "View database host, port, and connection configuration"},
    {"id": "EDIT_CONNECTIONS", "category": "Integrations", "description": "Create, edit, and delete database connection profiles"},
    {"id": "TEST_CONNECTIONS", "category": "Integrations", "description": "Perform live connectivity tests against database sources"},
    
    # Administration & Access Control
    {"id": "MANAGE_USERS", "category": "Administration", "description": "Create, update, reset password, and drop user accounts"},
    {"id": "MANAGE_ROLES", "category": "Administration", "description": "Create, configure manual access permissions, and drop roles"},
]


class UserService:
    def __init__(self, conn_manager: Optional[ConnectionManager] = None):
        self.cm = conn_manager or ConnectionManager("config/config.yaml")

    # ═════════════════════════════════════════════════════════════════════════
    # 1. USER MANAGEMENT
    # ═════════════════════════════════════════════════════════════════════════

    def list_users(self) -> List[Dict[str, Any]]:
        """List all users with their assigned role and profile info (omitting password_hash)."""
        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        u.user_id,
                        u.username,
                        u.role_id,
                        r.role_name,
                        u.is_active,
                        u.full_name,
                        u.email,
                        u.allowed_projects,
                        u.created_at,
                        u.created_by,
                        u.last_login
                    FROM metadata.gui_users u
                    LEFT JOIN metadata.gui_roles r ON u.role_id = r.role_id
                    ORDER BY u.user_id ASC
                """)
                rows = cur.fetchall()
                users = []
                for r in rows:
                    users.append({
                        "user_id": r[0],
                        "username": r[1],
                        "role_id": r[2],
                        "role": r[3] or "UNKNOWN",
                        "is_active": r[4] == "Y",
                        "full_name": r[5] or "",
                        "email": r[6] or "",
                        "allowed_projects": list(r[7] or []),
                        "created_at": r[8].isoformat() if r[8] else None,
                        "created_by": r[9] or "SYSTEM",
                        "last_login": r[10].isoformat() if r[10] else None,
                    })
                return users

    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Fetch single user detail by user_id."""
        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        u.user_id, u.username, u.role_id, r.role_name, u.is_active,
                        u.full_name, u.email, u.allowed_projects, u.created_at, u.last_login
                    FROM metadata.gui_users u
                    LEFT JOIN metadata.gui_roles r ON u.role_id = r.role_id
                    WHERE u.user_id = %s
                """, (user_id,))
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "user_id": r[0],
                    "username": r[1],
                    "role_id": r[2],
                    "role": r[3] or "UNKNOWN",
                    "is_active": r[4] == "Y",
                    "full_name": r[5] or "",
                    "email": r[6] or "",
                    "allowed_projects": list(r[7] or []),
                    "created_at": r[8].isoformat() if r[8] else None,
                    "last_login": r[9].isoformat() if r[9] else None,
                }

    def create_user(
        self,
        username: str,
        password: str,
        role_id: int,
        full_name: Optional[str] = None,
        email: Optional[str] = None,
        allowed_projects: Optional[List[str]] = None,
        actor_id: str = "admin",
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new user with bcrypt-hashed password and audit log."""
        username_clean = username.strip().lower()
        if not username_clean:
            raise ValueError("Username cannot be empty.")
        if len(password) < 6:
            raise ValueError("Password must be at least 6 characters long.")

        pwd_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(12)).decode("utf-8")
        projects = allowed_projects or []

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # Check duplicate username
                cur.execute("SELECT user_id FROM metadata.gui_users WHERE LOWER(username) = %s", (username_clean,))
                if cur.fetchone():
                    raise ValueError(f"User '{username_clean}' already exists.")

                # Check role exists
                cur.execute("SELECT role_name FROM metadata.gui_roles WHERE role_id = %s", (role_id,))
                role_row = cur.fetchone()
                if not role_row:
                    raise ValueError(f"Role ID {role_id} does not exist.")
                role_name = role_row[0]

                cur.execute("""
                    INSERT INTO metadata.gui_users (
                        username, password_hash, role_id, is_active, full_name, email,
                        allowed_projects, created_by, created_at
                    ) VALUES (%s, %s, %s, 'Y', %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING user_id, created_at
                """, (username_clean, pwd_hash, role_id, full_name or "", email or "", projects, actor_id))
                new_row = cur.fetchone()
                new_user_id = new_row[0]
                created_at = new_row[1]

            conn.commit()

        AuditLogger.log_event(
            username=actor_id,
            session_id="USER_MGMT",
            action="CREATE_USER",
            resource_type="USER",
            resource_id=str(new_user_id),
            status="SUCCESS",
            new_value={
                "username": username_clean,
                "role": role_name,
                "role_id": role_id,
                "full_name": full_name,
                "email": email,
                "allowed_projects": projects,
            },
            client_ip=client_ip or "UNKNOWN",
            user_agent=user_agent or "UNKNOWN",
        )

        return {
            "user_id": new_user_id,
            "username": username_clean,
            "role": role_name,
            "role_id": role_id,
            "is_active": True,
            "full_name": full_name or "",
            "email": email or "",
            "allowed_projects": projects,
            "created_at": created_at.isoformat() if created_at else None,
        }

    def update_user(
        self,
        user_id: int,
        role_id: Optional[int] = None,
        is_active: Optional[bool] = None,
        full_name: Optional[str] = None,
        email: Optional[str] = None,
        allowed_projects: Optional[List[str]] = None,
        actor_id: str = "admin",
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update user properties (role, active state, profile, allowed projects)."""
        existing = self.get_user_by_id(user_id)
        if not existing:
            raise KeyError(f"User ID {user_id} not found.")

        # Invariant: If deactivating or changing role of an ADMIN, verify there's at least one other active admin
        if existing["role"] == "ADMIN":
            if (is_active is False) or (role_id is not None and role_id != existing["role_id"]):
                with self.cm.get_metadata_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT COUNT(*) FROM metadata.gui_users u
                            JOIN metadata.gui_roles r ON u.role_id = r.role_id
                            WHERE r.role_name = 'ADMIN' AND u.is_active = 'Y' AND u.user_id != %s
                        """, (user_id,))
                        admin_count = cur.fetchone()[0]
                        if admin_count < 1:
                            raise ValueError("Cannot deactivate or change role of the last active ADMIN user.")

        new_role_name = existing["role"]
        updates = []
        params = []

        if role_id is not None:
            with self.cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT role_name FROM metadata.gui_roles WHERE role_id = %s", (role_id,))
                    rr = cur.fetchone()
                    if not rr:
                        raise ValueError(f"Role ID {role_id} does not exist.")
                    new_role_name = rr[0]
            updates.append("role_id = %s")
            params.append(role_id)

        if is_active is not None:
            updates.append("is_active = %s")
            params.append("Y" if is_active else "N")
            if not is_active:
                updates.append("deactivated_at = CURRENT_TIMESTAMP")
            else:
                updates.append("deactivated_at = NULL")

        if full_name is not None:
            updates.append("full_name = %s")
            params.append(full_name)

        if email is not None:
            updates.append("email = %s")
            params.append(email)

        if allowed_projects is not None:
            updates.append("allowed_projects = %s")
            params.append(allowed_projects)

        if not updates:
            return existing

        params.append(user_id)
        query = f"UPDATE metadata.gui_users SET {', '.join(updates)} WHERE user_id = %s"

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
            conn.commit()

        AuditLogger.log_event(
            username=actor_id,
            session_id="USER_MGMT",
            action="UPDATE_USER",
            resource_type="USER",
            resource_id=str(user_id),
            status="SUCCESS",
            old_value=existing,
            new_value={
                "role_id": role_id if role_id is not None else existing["role_id"],
                "role": new_role_name,
                "is_active": is_active if is_active is not None else existing["is_active"],
                "full_name": full_name if full_name is not None else existing["full_name"],
                "email": email if email is not None else existing["email"],
                "allowed_projects": allowed_projects if allowed_projects is not None else existing["allowed_projects"],
            },
            client_ip=client_ip or "UNKNOWN",
            user_agent=user_agent or "UNKNOWN",
        )

        return self.get_user_by_id(user_id)

    def reset_password(
        self,
        user_id: int,
        new_password: str,
        actor_id: str = "admin",
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> bool:
        """Reset a user's password with a secure bcrypt hash."""
        existing = self.get_user_by_id(user_id)
        if not existing:
            raise KeyError(f"User ID {user_id} not found.")

        if len(new_password) < 6:
            raise ValueError("Password must be at least 6 characters long.")

        pwd_hash = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt(12)).decode("utf-8")

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE metadata.gui_users
                    SET password_hash = %s,
                        token_version = token_version + 1,
                        failed_login_attempts = 0,
                        locked_until = NULL
                    WHERE user_id = %s
                """, (pwd_hash, user_id))
            conn.commit()

        AuditLogger.log_event(
            username=actor_id,
            session_id="USER_MGMT",
            action="RESET_PASSWORD",
            resource_type="USER",
            resource_id=str(user_id),
            status="SUCCESS",
            new_value={"username": existing["username"], "action": "Password reset successfully"},
            client_ip=client_ip or "UNKNOWN",
            user_agent=user_agent or "UNKNOWN",
        )
        return True

    def drop_user(
        self,
        user_id: int,
        actor_id: str,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> bool:
        """
        Permanently drop a user account.
        Invariants:
          - Cannot drop self (actor_id cannot delete own user).
          - Cannot drop the last active ADMIN.
        """
        existing = self.get_user_by_id(user_id)
        if not existing:
            raise KeyError(f"User ID {user_id} not found.")

        # Invariant 1: Self-deletion guard
        if existing["username"].lower() == actor_id.lower():
            raise ValueError("Security Violation: You cannot drop your own active account.")

        # Invariant 2: Last Admin guard
        if existing["role"] == "ADMIN":
            with self.cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM metadata.gui_users u
                        JOIN metadata.gui_roles r ON u.role_id = r.role_id
                        WHERE r.role_name = 'ADMIN' AND u.is_active = 'Y' AND u.user_id != %s
                    """, (user_id,))
                    admin_count = cur.fetchone()[0]
                    if admin_count < 1:
                        raise ValueError("Security Violation: Cannot drop the last remaining ADMIN user.")

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM metadata.gui_users WHERE user_id = %s", (user_id,))
            conn.commit()

        AuditLogger.log_event(
            username=actor_id,
            session_id="USER_MGMT",
            action="DROP_USER",
            resource_type="USER",
            resource_id=str(user_id),
            status="SUCCESS",
            old_value=existing,
            client_ip=client_ip or "UNKNOWN",
            user_agent=user_agent or "UNKNOWN",
        )
        return True

    # ═════════════════════════════════════════════════════════════════════════
    # 2. ROLE & GRANULAR ACCESS (PERMISSIONS) MANAGEMENT
    # ═════════════════════════════════════════════════════════════════════════

    def list_permissions(self) -> List[Dict[str, Any]]:
        """Return all supported system permissions grouped by operational domain."""
        return AVAILABLE_SYSTEM_PERMISSIONS

    def list_roles(self) -> List[Dict[str, Any]]:
        """List all roles with their assigned permission arrays and user counts."""
        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        r.role_id,
                        r.role_name,
                        r.description,
                        COALESCE(array_agg(p.permission_name) FILTER (WHERE p.permission_name IS NOT NULL), '{}') AS permissions,
                        (SELECT COUNT(*) FROM metadata.gui_users u WHERE u.role_id = r.role_id) AS user_count
                    FROM metadata.gui_roles r
                    LEFT JOIN metadata.gui_role_permissions p ON r.role_id = p.role_id
                    GROUP BY r.role_id, r.role_name, r.description
                    ORDER BY r.role_id ASC
                """)
                roles = []
                for r in cur.fetchall():
                    roles.append({
                        "role_id": r[0],
                        "role_name": r[1],
                        "description": r[2] or "",
                        "permissions": list(r[3] or []),
                        "user_count": r[4] or 0,
                        "is_system_role": r[1] in SYSTEM_ROLES,
                    })
                return roles

    def get_role_by_id(self, role_id: int) -> Optional[Dict[str, Any]]:
        """Get role by ID with permissions."""
        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        r.role_id,
                        r.role_name,
                        r.description,
                        COALESCE(array_agg(p.permission_name) FILTER (WHERE p.permission_name IS NOT NULL), '{}') AS permissions,
                        (SELECT COUNT(*) FROM metadata.gui_users u WHERE u.role_id = r.role_id) AS user_count
                    FROM metadata.gui_roles r
                    LEFT JOIN metadata.gui_role_permissions p ON r.role_id = p.role_id
                    WHERE r.role_id = %s
                    GROUP BY r.role_id, r.role_name, r.description
                """, (role_id,))
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "role_id": r[0],
                    "role_name": r[1],
                    "description": r[2] or "",
                    "permissions": list(r[3] or []),
                    "user_count": r[4] or 0,
                    "is_system_role": r[1] in SYSTEM_ROLES,
                }

    def create_role(
        self,
        role_name: str,
        description: Optional[str] = None,
        permissions: Optional[List[str]] = None,
        actor_id: str = "admin",
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new role with manual granular access permissions."""
        name_clean = role_name.strip().upper()
        if not name_clean:
            raise ValueError("Role name cannot be empty.")

        perms = list(set(permissions or []))

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # Check duplicate
                cur.execute("SELECT role_id FROM metadata.gui_roles WHERE UPPER(role_name) = %s", (name_clean,))
                if cur.fetchone():
                    raise ValueError(f"Role '{name_clean}' already exists.")

                cur.execute("""
                    INSERT INTO metadata.gui_roles (role_name, description)
                    VALUES (%s, %s)
                    RETURNING role_id
                """, (name_clean, description or ""))
                new_role_id = cur.fetchone()[0]

                # Insert permissions
                if perms:
                    for p in perms:
                        cur.execute("""
                            INSERT INTO metadata.gui_role_permissions (role_id, permission_name)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (new_role_id, p))

            conn.commit()

        AuditLogger.log_event(
            username=actor_id,
            session_id="USER_MGMT",
            action="CREATE_ROLE",
            resource_type="ROLE",
            resource_id=str(new_role_id),
            status="SUCCESS",
            new_value={"role_name": name_clean, "description": description, "permissions": perms},
            client_ip=client_ip or "UNKNOWN",
            user_agent=user_agent or "UNKNOWN",
        )

        return self.get_role_by_id(new_role_id)

    def update_role(
        self,
        role_id: int,
        role_name: Optional[str] = None,
        description: Optional[str] = None,
        permissions: Optional[List[str]] = None,
        actor_id: str = "admin",
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update role metadata and its assigned manual access permissions."""
        existing = self.get_role_by_id(role_id)
        if not existing:
            raise KeyError(f"Role ID {role_id} not found.")

        # Invariant: Cannot rename system roles (ADMIN, VIEWER)
        if existing["is_system_role"] and role_name and role_name.strip().upper() != existing["role_name"]:
            raise ValueError(f"Cannot rename immutable system role '{existing['role_name']}'.")

        # Invariant: Cannot remove MANAGE_ROLES or MANAGE_USERS from ADMIN
        if existing["role_name"] == "ADMIN" and permissions is not None:
            required_admin_perms = {"MANAGE_ROLES", "MANAGE_USERS", "VIEW_AUDIT_LOG"}
            missing = required_admin_perms - set(permissions)
            if missing:
                raise ValueError(f"Cannot remove core administrative permissions {missing} from ADMIN role.")

        new_name = role_name.strip().upper() if role_name else existing["role_name"]

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                if role_name is not None or description is not None:
                    cur.execute("""
                        UPDATE metadata.gui_roles
                        SET role_name = %s, description = %s
                        WHERE role_id = %s
                    """, (new_name, description if description is not None else existing["description"], role_id))

                if permissions is not None:
                    # Sync permissions: delete and re-insert
                    cur.execute("DELETE FROM metadata.gui_role_permissions WHERE role_id = %s", (role_id,))
                    for p in set(permissions):
                        cur.execute("""
                            INSERT INTO metadata.gui_role_permissions (role_id, permission_name)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (role_id, p))

            conn.commit()

        AuditLogger.log_event(
            username=actor_id,
            session_id="USER_MGMT",
            action="UPDATE_ROLE",
            resource_type="ROLE",
            resource_id=str(role_id),
            status="SUCCESS",
            old_value=existing,
            new_value={"role_name": new_name, "description": description, "permissions": permissions},
            client_ip=client_ip or "UNKNOWN",
            user_agent=user_agent or "UNKNOWN",
        )

        return self.get_role_by_id(role_id)

    def drop_role(
        self,
        role_id: int,
        actor_id: str,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> bool:
        """
        Drop custom role.
        Invariants:
          - Cannot drop system roles (ADMIN, VIEWER).
          - Cannot drop a role that has users currently assigned.
        """
        existing = self.get_role_by_id(role_id)
        if not existing:
            raise KeyError(f"Role ID {role_id} not found.")

        if existing["is_system_role"]:
            raise ValueError(f"Security Violation: Cannot drop system role '{existing['role_name']}'.")

        if existing["user_count"] > 0:
            raise ValueError(
                f"Referential Conflict: Cannot drop role '{existing['role_name']}' because {existing['user_count']} user(s) "
                "are currently assigned to it. Please reassign those users first."
            )

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM metadata.gui_role_permissions WHERE role_id = %s", (role_id,))
                cur.execute("DELETE FROM metadata.gui_roles WHERE role_id = %s", (role_id,))
            conn.commit()

        AuditLogger.log_event(
            username=actor_id,
            session_id="USER_MGMT",
            action="DROP_ROLE",
            resource_type="ROLE",
            resource_id=str(role_id),
            status="SUCCESS",
            old_value=existing,
            client_ip=client_ip or "UNKNOWN",
            user_agent=user_agent or "UNKNOWN",
        )
        return True

    # ═════════════════════════════════════════════════════════════════════════
    # 3. USER ACTIVITY & AUDIT TRAIL OBSERVER
    # ═════════════════════════════════════════════════════════════════════════

    def get_audit_logs(
        self,
        username: Optional[str] = None,
        action: Optional[str] = None,
        resource_type: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Query forensic activity log to observe what each user has operated."""
        filters = []
        params = []

        if username:
            filters.append("LOWER(username) = LOWER(%s)")
            params.append(username.strip())

        if action:
            filters.append("LOWER(action) = LOWER(%s)")
            params.append(action.strip())

        if resource_type:
            filters.append("LOWER(resource_type) = LOWER(%s)")
            params.append(resource_type.strip())

        if status:
            filters.append("LOWER(status) = LOWER(%s)")
            params.append(status.strip())

        if start_date:
            filters.append("event_ts >= %s")
            params.append(start_date)

        if end_date:
            filters.append("event_ts <= %s")
            params.append(end_date)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        count_query = f"SELECT COUNT(*) FROM metadata.gui_audit_log {where_clause}"
        data_query = f"""
            SELECT 
                audit_id,
                event_ts,
                username,
                session_id,
                action,
                resource_type,
                resource_id,
                old_value,
                new_value,
                status,
                client_ip,
                user_agent
            FROM metadata.gui_audit_log
            {where_clause}
            ORDER BY event_ts DESC, audit_id DESC
            LIMIT %s OFFSET %s
        """

        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(count_query, tuple(params))
                total_count = cur.fetchone()[0]

                data_params = list(params) + [limit, offset]
                cur.execute(data_query, tuple(data_params))
                rows = cur.fetchall()

                logs = []
                for r in rows:
                    logs.append({
                        "audit_id": r[0],
                        "event_ts": r[1].isoformat() if r[1] else None,
                        "username": r[2] or "SYSTEM",
                        "session_id": r[3] or "",
                        "action": r[4],
                        "resource_type": r[5] or "",
                        "resource_id": r[6] or "",
                        "old_value": r[7],
                        "new_value": r[8],
                        "status": r[9],
                        "client_ip": r[10] or "UNKNOWN",
                        "user_agent": r[11] or "UNKNOWN",
                    })

                return {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "items": logs,
                }

    def get_audit_summary(self) -> Dict[str, Any]:
        """Aggregate operational activity summary (actions per user, top operators, total operations)."""
        with self.cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                # Total events
                cur.execute("SELECT COUNT(*) FROM metadata.gui_audit_log")
                total_ops = cur.fetchone()[0]

                # Distinct active operators
                cur.execute("SELECT COUNT(DISTINCT username) FROM metadata.gui_audit_log WHERE username IS NOT NULL")
                distinct_users = cur.fetchone()[0]

                # Top active users
                cur.execute("""
                    SELECT username, COUNT(*) AS op_count, MAX(event_ts) AS last_active
                    FROM metadata.gui_audit_log
                    WHERE username IS NOT NULL
                    GROUP BY username
                    ORDER BY op_count DESC
                    LIMIT 8
                """)
                top_users = [
                    {"username": r[0], "count": r[1], "last_active": r[2].isoformat() if r[2] else None}
                    for r in cur.fetchall()
                ]

                # Top actions
                cur.execute("""
                    SELECT action, COUNT(*) AS act_count
                    FROM metadata.gui_audit_log
                    GROUP BY action
                    ORDER BY act_count DESC
                    LIMIT 8
                """)
                top_actions = [{"action": r[0], "count": r[1]} for r in cur.fetchall()]

                return {
                    "total_operations": total_ops,
                    "active_users_count": distinct_users,
                    "top_users": top_users,
                    "top_actions": top_actions,
                }
