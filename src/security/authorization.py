# src/security/authorization.py
import functools
import logging
from typing import Callable, Any, Optional

logger = logging.getLogger("Authorization")

class AuthorizationException(Exception):
    pass

class AuthorizationService:
    @staticmethod
    def has_permission(user_session: Optional[dict], permission: str, scope: Optional[str] = None) -> bool:
        """
        Checks if the provided user session has the required permission.
        scope is included for future domain-level segregation.
        """
        if not user_session:
            return False
            
        perms = user_session.get("permissions", [])
        # For Phase 2, if they have the permission name, they have it globally.
        # Future: parse "MANAGE_METADATA:erp_*" etc.
        return permission in perms

def require_permission(permission: str, resource_type: str = "UNKNOWN"):
    """
    Decorator to enforce service-layer security.
    Expects the wrapped method to receive `user_session` as a keyword argument 
    or inside a kwargs dictionary.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            user_session = kwargs.get("user_session")
            
            # Try to extract from first argument if it's a dict and has 'username'
            if not user_session and args and isinstance(args[0], dict) and "username" in args[0]:
                user_session = args[0]
                
            # Try to find user_session in args if passed explicitly
            if not user_session:
                for arg in args:
                    if isinstance(arg, dict) and "username" in arg and "permissions" in arg:
                        user_session = arg
                        break

            # Try to fetch from streamlit session_state if running in GUI context
            if not user_session:
                try:
                    import streamlit as st
                    user_session = st.session_state.get("user")
                except ImportError:
                    pass

            if not AuthorizationService.has_permission(user_session, permission):
                username = user_session.get("username", "ANONYMOUS") if user_session else "ANONYMOUS"
                logger.error(f"Authorization Denied: User '{username}' lacks '{permission}' for {resource_type}.")
                
                # We could log this to AuditLogger here, but we'd need session_id and target.
                # For now, raise an exception.
                raise AuthorizationException(f"Access Denied: Requires permission {permission}")
                
            return func(*args, **kwargs)
        return wrapper
    return decorator
