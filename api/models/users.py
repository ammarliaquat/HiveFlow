# api/models/users.py
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

# ── User Request / Response Models ───────────────────────────────────────────

class CreateUserRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=6, max_length=100)
    role_id: int
    full_name: Optional[str] = Field(None, max_length=100)
    email: Optional[str] = Field(None, max_length=100)
    allowed_projects: List[str] = Field(default_factory=list)

class UpdateUserRequest(BaseModel):
    role_id: Optional[int] = None
    is_active: Optional[bool] = None
    full_name: Optional[str] = Field(None, max_length=100)
    email: Optional[str] = Field(None, max_length=100)
    allowed_projects: Optional[List[str]] = None

class ResetPasswordRequest(BaseModel):
    new_password: str = Field(..., min_length=6, max_length=100)

class UserItemResponse(BaseModel):
    user_id: int
    username: str
    role_id: Optional[int] = None
    role: str
    is_active: bool
    full_name: str = ""
    email: str = ""
    allowed_projects: List[str] = Field(default_factory=list)
    created_at: Optional[str] = None
    created_by: str = "SYSTEM"
    last_login: Optional[str] = None

# ── Role Request / Response Models ───────────────────────────────────────────

class CreateRoleRequest(BaseModel):
    role_name: str = Field(..., min_length=2, max_length=50)
    description: Optional[str] = Field(None, max_length=255)
    permissions: List[str] = Field(default_factory=list)

class UpdateRoleRequest(BaseModel):
    role_name: Optional[str] = Field(None, min_length=2, max_length=50)
    description: Optional[str] = Field(None, max_length=255)
    permissions: Optional[List[str]] = None

class RoleItemResponse(BaseModel):
    role_id: int
    role_name: str
    description: str = ""
    permissions: List[str] = Field(default_factory=list)
    user_count: int = 0
    is_system_role: bool = False

class PermissionItemResponse(BaseModel):
    id: str
    category: str
    description: str

# ── Audit Log Models ─────────────────────────────────────────────────────────

class AuditLogItemResponse(BaseModel):
    audit_id: int
    event_ts: Optional[str] = None
    username: str
    session_id: str = ""
    action: str
    resource_type: str = ""
    resource_id: str = ""
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    status: str
    client_ip: str = "UNKNOWN"
    user_agent: str = "UNKNOWN"

class AuditLogsResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[AuditLogItemResponse]

class AuditSummaryItem(BaseModel):
    username: Optional[str] = None
    action: Optional[str] = None
    count: int
    last_active: Optional[str] = None

class AuditSummaryResponse(BaseModel):
    total_operations: int
    active_users_count: int
    top_users: List[Dict[str, Any]]
    top_actions: List[Dict[str, Any]]
