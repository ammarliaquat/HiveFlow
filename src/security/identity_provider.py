# src/security/identity_provider.py
from abc import ABC, abstractmethod
from typing import Optional, Dict, List
import bcrypt
import logging
from src.connections.connection_manager import ConnectionManager

logger = logging.getLogger("IdentityProvider")

class IdentityProvider(ABC):
    @abstractmethod
    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """
        Validates credentials and returns user details and permissions if successful.
        Returns None if authentication fails.
        Expected return dict:
        {
            "username": "...",
            "role": "...",
            "permissions": ["...", "..."]
        }
        """
        pass

class LocalDatabaseIdentityProvider(IdentityProvider):
    def __init__(self, conn_manager: ConnectionManager):
        self.conn_manager = conn_manager

    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    # Fetch user and role
                    cur.execute("""
                        SELECT u.password_hash, u.is_active, r.role_name, r.role_id
                        FROM metadata.gui_users u
                        JOIN metadata.gui_roles r ON u.role_id = r.role_id
                        WHERE u.username = %s
                    """, (username,))
                    
                    row = cur.fetchone()
                    if not row:
                        return None
                        
                    pwd_hash, is_active, role_name, role_id = row
                    
                    if is_active != 'Y':
                        logger.warning(f"Login attempt for inactive user {username}")
                        return None
                        
                    # Verify password
                    if not bcrypt.checkpw(password.encode('utf-8'), pwd_hash.encode('utf-8')):
                        return None
                        
                    # Update last login
                    cur.execute("UPDATE metadata.gui_users SET last_login = CURRENT_TIMESTAMP WHERE username = %s", (username,))
                    
                    # Fetch permissions
                    cur.execute("SELECT permission_name FROM metadata.gui_role_permissions WHERE role_id = %s", (role_id,))
                    permissions = [p[0] for p in cur.fetchall()]
                    
                conn.commit()
                
            return {
                "username": username,
                "role": role_name,
                "permissions": permissions
            }
        except Exception as e:
            logger.error(f"Authentication failed due to database error: {e}")
            return None
