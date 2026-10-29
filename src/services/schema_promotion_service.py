# src/services/schema_promotion_service.py
import logging
from datetime import datetime
from src.connections.connection_manager import ConnectionManager
from src.security.authorization import require_permission
from src.security.audit_logger import AuditLogger

logger = logging.getLogger("SchemaPromotionService")

class SchemaPromotionService:
    
    @classmethod
    @require_permission("APPROVE_SCHEMA_DRIFT", resource_type="TABLE")
    def approve_drift(cls, alert_id: int, user_session: dict) -> bool:
        """
        Approves a schema drift alert.
        Retires the old column definition, inserts the new one with version + 1,
        and suspends conflicting DQ rules.
        """
        cm = ConnectionManager()
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    # Get alert info
                    cur.execute("""
                        SELECT table_id, column_name, drift_type, new_canonical_type 
                        FROM metadata.schema_drift_alerts 
                        WHERE alert_id = %s AND status = 'PENDING'
                    """, (alert_id,))
                    alert = cur.fetchone()
                    if not alert:
                        logger.error(f"Alert {alert_id} not found or not PENDING.")
                        return False
                        
                    table_id, col_name, drift_type, new_type = alert
                    username = user_session.get("username", "UNKNOWN")
                    session_id = user_session.get("session_id", "UNKNOWN")
                    now = datetime.now()
                    
                    if drift_type in ("TYPE_CHANGED", "DROPPED_COLUMN", "RENAME_DETECTED"):
                        # Retire existing active column
                        cur.execute("""
                            UPDATE metadata.db_columns_etl 
                            SET retired_at = %s 
                            WHERE table_id = %s AND column_name ILIKE %s AND retired_at IS NULL
                            RETURNING schema_version
                        """, (now, table_id, col_name))
                        
                        retired = cur.fetchone()
                        old_version = retired[0] if retired else 0
                        
                        # Suspend DQ rules
                        cur.execute("""
                            UPDATE metadata.data_quality_rules
                            SET enabled = 'N'
                            WHERE table_id = %s AND column_name ILIKE %s AND enabled = 'Y'
                        """, (table_id, col_name))
                        
                        if drift_type in ("TYPE_CHANGED", "RENAME_DETECTED"):
                            # Insert new row with version + 1
                            new_name = col_name # If rename, we'd theoretically use the new name, but simplify for now
                            cur.execute("""
                                INSERT INTO metadata.db_columns_etl (
                                    table_id, column_name, data_type, is_active, 
                                    schema_version, effective_from
                                ) VALUES (%s, %s, %s, 'Y', %s, %s)
                            """, (table_id, new_name, new_type, old_version + 1, now))
                            
                    elif drift_type == "NEW_COLUMN":
                        cur.execute("""
                            INSERT INTO metadata.db_columns_etl (
                                table_id, column_name, data_type, is_active, 
                                schema_version, effective_from
                            ) VALUES (%s, %s, %s, 'Y', 1, %s)
                        """, (table_id, col_name, new_type, now))
                        
                    # Mark alert as APPROVED
                    cur.execute("""
                        UPDATE metadata.schema_drift_alerts 
                        SET status = 'APPROVED', resolved_at = %s, resolved_by = %s
                        WHERE alert_id = %s
                    """, (now, username, alert_id))
                    
                conn.commit()
                
            AuditLogger.log_event(
                username=username,
                session_id=session_id,
                action="APPROVE_SCHEMA_DRIFT",
                resource_type="TABLE",
                resource_id=str(table_id),
                status="SUCCESS",
                old_value={"alert_id": alert_id, "column": col_name, "drift": drift_type},
                new_value={"status": "APPROVED", "new_type": new_type}
            )
            return True
        except Exception as e:
            logger.error(f"Failed to approve drift alert {alert_id}: {e}")
            return False

    @classmethod
    @require_permission("REJECT_SCHEMA_DRIFT", resource_type="TABLE")
    def reject_drift(cls, alert_id: int, user_session: dict) -> bool:
        cm = ConnectionManager()
        username = user_session.get("username", "UNKNOWN")
        session_id = user_session.get("session_id", "UNKNOWN")
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE metadata.schema_drift_alerts 
                        SET status = 'REJECTED', resolved_at = %s, resolved_by = %s
                        WHERE alert_id = %s
                    """, (datetime.now(), username, alert_id))
                conn.commit()
                
            AuditLogger.log_event(
                username=username,
                session_id=session_id,
                action="REJECT_SCHEMA_DRIFT",
                resource_type="ALERT",
                resource_id=str(alert_id),
                status="SUCCESS"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to reject drift alert {alert_id}: {e}")
            return False
