# src/security/audit_logger.py
import json
import logging
from typing import Optional, Dict, Any
from src.connections.connection_manager import ConnectionManager

logger = logging.getLogger("AuditLogger")

class AuditLogger:
    @staticmethod
    def log_event(
        username: str,
        session_id: str,
        action: str,
        resource_type: str,
        resource_id: str,
        status: str,
        old_value: Optional[Dict[str, Any]] = None,
        new_value: Optional[Dict[str, Any]] = None,
        client_ip: str = "UNKNOWN",
        user_agent: str = "UNKNOWN"
    ):
        """
        Writes a forensic record to metadata.gui_audit_log.
        """
        cm = ConnectionManager()
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        INSERT INTO metadata.gui_audit_log (
                            username, session_id, action, resource_type, resource_id,
                            old_value, new_value, status, client_ip, user_agent
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cur.execute(query, (
                        username, session_id, action, resource_type, str(resource_id),
                        json.dumps(old_value) if old_value else None,
                        json.dumps(new_value) if new_value else None,
                        status, client_ip, user_agent
                    ))
                conn.commit()
        except Exception as e:
            # We fail silently here to not crash the app, but log it heavily
            logger.error(f"CRITICAL: Failed to write to audit log: {e}")
