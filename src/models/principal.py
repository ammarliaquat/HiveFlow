# src/models/principal.py
"""
Server-owned Authenticated Principal for RBAC and Execution Authorization.
"""
from dataclasses import dataclass, field
from typing import Set, Optional, Any
from gui.models.project_context import ProjectContext


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    """
    Server-owned identity representing an authenticated actor (USER or SYSTEM).
    Cannot be forged by external client requests.
    """
    actor_id: str = "anonymous"
    actor_type: str = "USER"
    roles: Any = field(default_factory=set)
    project_context: Optional[ProjectContext] = None
    user_id: Optional[str] = None
    username: Optional[str] = None
    client_ip: Optional[str] = None

    def __post_init__(self):
        if self.user_id and (not self.actor_id or self.actor_id == "anonymous"):
            object.__setattr__(self, "actor_id", self.user_id)
        if isinstance(self.roles, (list, tuple)):
            object.__setattr__(self, "roles", set(self.roles))

    def has_role(self, role: str) -> bool:
        """Checks if the principal has a specific role."""
        return role in self.roles

