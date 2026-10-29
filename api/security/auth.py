# api/security/auth.py
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

import jwt
from fastapi import Request, HTTPException, status, Depends

from api.config import (
    get_secret_key,
    ALGORITHM,
    SESSION_EXPIRE_MINUTES,
    COOKIE_NAME,
    CSRF_COOKIE_NAME,
)
from api.models.auth import UserResponse, SessionPayload
from gui.models.project_context import ProjectContext
from src.connections.connection_manager import ConnectionManager
from src.security.identity_provider import LocalDatabaseIdentityProvider

log = logging.getLogger("api.security.auth")

# Singleton connection manager for API service
_connection_manager: Optional[ConnectionManager] = None

# In-memory revocation ledger for revoked session tokens (thread-safe)
_revocation_lock = threading.Lock()
_REVOKED_TOKENS: set = set()

def revoke_session_token(token: str) -> None:
    """Adds a session token to the revocation ledger."""
    with _revocation_lock:
        _REVOKED_TOKENS.add(token)

def is_token_revoked(token: str) -> bool:
    """Checks whether a session token has been revoked via logout."""
    with _revocation_lock:
        return token in _REVOKED_TOKENS


def get_connection_manager() -> ConnectionManager:
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager("config/config.yaml")
    return _connection_manager


def create_session_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    secret = get_secret_key()
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    expire = now + (expires_delta or timedelta(minutes=SESSION_EXPIRE_MINUTES))
    to_encode.update({"exp": int(expire.timestamp()), "iat": int(now.timestamp())})
    return jwt.encode(to_encode, secret, algorithm=ALGORITHM)


def decode_session_token(token: str) -> Optional[SessionPayload]:
    if is_token_revoked(token):
        log.warning("Revoked session token presented.")
        return None
    try:
        secret = get_secret_key()
        payload = jwt.decode(token, secret, algorithms=[ALGORITHM])
        return SessionPayload(
            username=payload["username"],
            role=payload.get("role", "VIEWER"),
            permissions=payload.get("permissions", []),
            exp=payload["exp"],
        )
    except (jwt.PyJWTError, KeyError, ValueError) as e:
        log.warning(f"Session token decode failed: {e}")
        return None


def is_user_active_in_db(username: str) -> bool:
    """
    Verifies that the user account is active in metadata.gui_users.
    Fails closed: returns False on DB unavailability.
    """
    try:
        cm = get_connection_manager()
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT is_active FROM metadata.gui_users WHERE username = %s",
                    (username,)
                )
                row = cur.fetchone()
                return bool(row and row[0] == "Y")
    except Exception as exc:
        log.error(f"Database unavailable checking active user; failing closed: {exc}")
        return False


def get_all_source_tags() -> List[str]:
    """Fetch distinct source tags from metadata catalog."""
    try:
        cm = get_connection_manager()
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT LOWER(source_tag)
                    FROM metadata.db_tables_etl
                    WHERE source_tag IS NOT NULL
                    ORDER BY 1
                """)
                return [r[0] for r in cur.fetchall()]
    except Exception as exc:
        log.warning(f"Database unavailable loading source tags: {exc}")
        return []


def authenticate_credentials(username: str, password: str) -> Optional[UserResponse]:
    cm = get_connection_manager()
    idp = LocalDatabaseIdentityProvider(cm)
    auth_dict = idp.authenticate(username, password)
    if not auth_dict:
        return None

    role = str(auth_dict.get("role") or "").upper()
    perms = auth_dict.get("permissions") or []
    all_tags = get_all_source_tags()

    # Admin has access to all projects; viewers/operators get all by default unless restricted
    allowed = all_tags

    return UserResponse(
        username=auth_dict["username"],
        role=role,
        permissions=perms,
        allowed_projects=allowed,
    )


async def get_current_user(request: Request) -> UserResponse:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        # Fallback to Authorization header if provided
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:].strip()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Missing session cookie or Bearer token.",
        )

    session = decode_session_token(token)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session token. Please log in again.",
        )

    # Verify user is still active in database (fail-closed)
    if not is_user_active_in_db(session.username):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is deactivated or database verification failed.",
        )

    all_tags = get_all_source_tags()
    return UserResponse(
        username=session.username,
        role=session.role,
        permissions=session.permissions,
        allowed_projects=all_tags,
    )


def require_permission(required_perm: str):
    def _perm_guard(user: UserResponse = Depends(get_current_user)) -> UserResponse:
        if user.role == "ADMIN":
            return user
        if required_perm not in user.permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Forbidden: Missing required permission '{required_perm}'.",
            )
        return user
    return _perm_guard


def verify_csrf_token(request: Request) -> None:
    """
    Enforces double-submit CSRF token validation on mutating requests
    when authenticated via session cookies.
    """
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return
    # Only enforce if caller is using cookie authentication
    if COOKIE_NAME in request.cookies:
        csrf_cookie = request.cookies.get(CSRF_COOKIE_NAME)
        csrf_header = request.headers.get("X-CSRF-Token")
        if not csrf_cookie or not csrf_header or csrf_cookie != csrf_header:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="CSRF validation failed: missing or mismatched X-CSRF-Token header.",
            )


def resolve_scoped_project_context(
    request: Request,
    user: UserResponse = Depends(get_current_user),
) -> ProjectContext:
    """
    Extracts and validates X-Project-Context or source_tag query param against user's permitted projects.
    For non-admin users, prevents bypassing project isolation via 'All Projects' or None.
    """
    target_project = request.headers.get("X-Project-Context") or request.query_params.get("source_tag")
    
    if target_project:
        target_project_clean = target_project.strip().lower()
        if user.role != "ADMIN" and target_project_clean not in [p.lower() for p in user.allowed_projects]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"RBAC Violation: User '{user.username}' is not authorized for project scope '{target_project}'.",
            )
        return ProjectContext(project_id=target_project_clean, environment="PROD")
    
    # Non-admin user without explicit project: must NOT bypass project isolation
    if user.role != "ADMIN":
        if not user.allowed_projects:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"RBAC Violation: User '{user.username}' has no authorized projects assigned.",
            )
        # Default non-admin to their primary authorized project scope
        return ProjectContext(project_id=user.allowed_projects[0].lower(), environment="PROD")

    return ProjectContext(project_id=None, environment="PROD")

