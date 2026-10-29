# api/routers/users.py
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Query, status

from api.models.envelope import ResponseEnvelope
from api.models.auth import UserResponse
from api.security.auth import get_current_user, require_permission, verify_csrf_token
from api.models.users import (
    CreateUserRequest,
    UpdateUserRequest,
    ResetPasswordRequest,
    UserItemResponse,
    CreateRoleRequest,
    UpdateRoleRequest,
    RoleItemResponse,
    PermissionItemResponse,
    AuditLogsResponse,
    AuditSummaryResponse,
)
from src.security.user_service import UserService

log = logging.getLogger("api.routers.users")

router = APIRouter(tags=["User & Role Management"])

_user_service: Optional[UserService] = None

def get_user_service() -> UserService:
    global _user_service
    if _user_service is None:
        _user_service = UserService()
    return _user_service


def get_request_metadata(request: Request) -> tuple:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    elif request.client:
        client_ip = request.client.host
    else:
        client_ip = "UNKNOWN"
    user_agent = request.headers.get("User-Agent") or "UNKNOWN"
    return client_ip, user_agent


# ═════════════════════════════════════════════════════════════════════════════
# 1. USER MANAGEMENT ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@router.get("/users", response_model=ResponseEnvelope[List[UserItemResponse]])
def list_users(
    current_user: UserResponse = Depends(require_permission("MANAGE_USERS")),
    service: UserService = Depends(get_user_service),
):
    """List all registered users with their roles, profile info, and status."""
    users = service.list_users()
    return ResponseEnvelope.ok(users)


@router.post("/users", response_model=ResponseEnvelope[UserItemResponse])
def create_user(
    req: CreateUserRequest,
    request: Request,
    current_user: UserResponse = Depends(require_permission("MANAGE_USERS")),
    service: UserService = Depends(get_user_service),
):
    """Create a new platform user with bcrypt password and assign role."""
    verify_csrf_token(request)
    client_ip, user_agent = get_request_metadata(request)

    try:
        user = service.create_user(
            username=req.username,
            password=req.password,
            role_id=req.role_id,
            full_name=req.full_name,
            email=req.email,
            allowed_projects=req.allowed_projects,
            actor_id=current_user.username,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return ResponseEnvelope.ok(user)
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        log.error(f"Error creating user {req.username}: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user.")


@router.post("/users/{user_id}/update", response_model=ResponseEnvelope[UserItemResponse])
def update_user(
    user_id: int,
    req: UpdateUserRequest,
    request: Request,
    current_user: UserResponse = Depends(require_permission("MANAGE_USERS")),
    service: UserService = Depends(get_user_service),
):
    """Update user properties (role, active state, allowed projects)."""
    verify_csrf_token(request)
    client_ip, user_agent = get_request_metadata(request)

    try:
        user = service.update_user(
            user_id=user_id,
            role_id=req.role_id,
            is_active=req.is_active,
            full_name=req.full_name,
            email=req.email,
            allowed_projects=req.allowed_projects,
            actor_id=current_user.username,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return ResponseEnvelope.ok(user)
    except KeyError as k_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(k_err))
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        log.error(f"Error updating user {user_id}: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update user.")


@router.post("/users/{user_id}/reset-password", response_model=ResponseEnvelope[dict])
def reset_password(
    user_id: int,
    req: ResetPasswordRequest,
    request: Request,
    current_user: UserResponse = Depends(require_permission("MANAGE_USERS")),
    service: UserService = Depends(get_user_service),
):
    """Reset a user's password."""
    verify_csrf_token(request)
    client_ip, user_agent = get_request_metadata(request)

    try:
        service.reset_password(
            user_id=user_id,
            new_password=req.new_password,
            actor_id=current_user.username,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return ResponseEnvelope.ok({"success": True, "message": "Password reset successfully."})
    except KeyError as k_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(k_err))
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        log.error(f"Error resetting password for user {user_id}: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to reset password.")


@router.post("/users/{user_id}/drop", response_model=ResponseEnvelope[dict])
def drop_user(
    user_id: int,
    request: Request,
    current_user: UserResponse = Depends(require_permission("MANAGE_USERS")),
    service: UserService = Depends(get_user_service),
):
    """Permanently drop a user account (enforces self-drop and last-admin protections)."""
    verify_csrf_token(request)
    client_ip, user_agent = get_request_metadata(request)

    try:
        service.drop_user(
            user_id=user_id,
            actor_id=current_user.username,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return ResponseEnvelope.ok({"dropped": True, "user_id": user_id})
    except KeyError as k_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(k_err))
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        log.error(f"Error dropping user {user_id}: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to drop user.")


# ═════════════════════════════════════════════════════════════════════════════
# 2. ROLE & GRANULAR ACCESS (PERMISSIONS) ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@router.get("/roles/permissions", response_model=ResponseEnvelope[List[PermissionItemResponse]])
def list_permissions(
    current_user: UserResponse = Depends(require_permission("MANAGE_ROLES")),
    service: UserService = Depends(get_user_service),
):
    """List all registered system permissions grouped by category."""
    perms = service.list_permissions()
    return ResponseEnvelope.ok(perms)


@router.get("/roles", response_model=ResponseEnvelope[List[RoleItemResponse]])
def list_roles(
    current_user: UserResponse = Depends(require_permission("MANAGE_ROLES")),
    service: UserService = Depends(get_user_service),
):
    """List all roles with their assigned permissions array and user counts."""
    roles = service.list_roles()
    return ResponseEnvelope.ok(roles)


@router.post("/roles", response_model=ResponseEnvelope[RoleItemResponse])
def create_role(
    req: CreateRoleRequest,
    request: Request,
    current_user: UserResponse = Depends(require_permission("MANAGE_ROLES")),
    service: UserService = Depends(get_user_service),
):
    """Create a new custom role with manual granular access permissions."""
    verify_csrf_token(request)
    client_ip, user_agent = get_request_metadata(request)

    try:
        role = service.create_role(
            role_name=req.role_name,
            description=req.description,
            permissions=req.permissions,
            actor_id=current_user.username,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return ResponseEnvelope.ok(role)
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        log.error(f"Error creating role {req.role_name}: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create role.")


@router.post("/roles/{role_id}/update", response_model=ResponseEnvelope[RoleItemResponse])
def update_role(
    role_id: int,
    req: UpdateRoleRequest,
    request: Request,
    current_user: UserResponse = Depends(require_permission("MANAGE_ROLES")),
    service: UserService = Depends(get_user_service),
):
    """Update role description or manual access permission assignments."""
    verify_csrf_token(request)
    client_ip, user_agent = get_request_metadata(request)

    try:
        role = service.update_role(
            role_id=role_id,
            role_name=req.role_name,
            description=req.description,
            permissions=req.permissions,
            actor_id=current_user.username,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return ResponseEnvelope.ok(role)
    except KeyError as k_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(k_err))
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        log.error(f"Error updating role {role_id}: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update role.")


@router.post("/roles/{role_id}/drop", response_model=ResponseEnvelope[dict])
def drop_role(
    role_id: int,
    request: Request,
    current_user: UserResponse = Depends(require_permission("MANAGE_ROLES")),
    service: UserService = Depends(get_user_service),
):
    """Drop a custom role (enforces system role and assigned user checks)."""
    verify_csrf_token(request)
    client_ip, user_agent = get_request_metadata(request)

    try:
        service.drop_role(
            role_id=role_id,
            actor_id=current_user.username,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        return ResponseEnvelope.ok({"dropped": True, "role_id": role_id})
    except KeyError as k_err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(k_err))
    except ValueError as val_err:
        # Check if it's referential conflict
        if "Referential Conflict" in str(val_err):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(val_err))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err))
    except Exception as exc:
        log.error(f"Error dropping role {role_id}: {exc}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to drop role.")


# ═════════════════════════════════════════════════════════════════════════════
# 3. USER ACTIVITY & AUDIT OBSERVER ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@router.get("/audit/logs", response_model=ResponseEnvelope[AuditLogsResponse])
def get_audit_logs(
    username: Optional[str] = Query(None, description="Filter what a specific user has operated"),
    action: Optional[str] = Query(None, description="Filter by operational action"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    status: Optional[str] = Query(None, description="Filter by status (SUCCESS, DENIED, ERROR)"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: UserResponse = Depends(require_permission("VIEW_AUDIT_LOG")),
    service: UserService = Depends(get_user_service),
):
    """Query the forensic activity ledger to observe what each user has operated."""
    result = service.get_audit_logs(
        username=username,
        action=action,
        resource_type=resource_type,
        status=status,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    return ResponseEnvelope.ok(result)


@router.get("/audit/summary", response_model=ResponseEnvelope[AuditSummaryResponse])
def get_audit_summary(
    current_user: UserResponse = Depends(require_permission("VIEW_AUDIT_LOG")),
    service: UserService = Depends(get_user_service),
):
    """Get operational summary metrics (top operators, total actions, frequency)."""
    summary = service.get_audit_summary()
    return ResponseEnvelope.ok(summary)
