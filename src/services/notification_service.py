# src/services/notification_service.py
"""
NotificationService — Enterprise Notification Center & Actionable Alerting Layer
================================================================================
Handles CRUD operations, cross-run incident correlation (`correlation_key`), 
deduplication windows, automatic resolution, configurable retention cleanup, 
and background email alerting (Level 2) for metadata.notifications.
"""
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from src.connections.connection_manager import ConnectionManager

log = logging.getLogger("NotificationService")

# Try importing Streamlit for caching decorator; fallback gracefully when running in terminal
try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False


class NotificationService:
    """
    Service layer providing static methods to manage operational notifications.
    Uses ConnectionManager singleton to connect to RepositoryDB.
    """
    _conn_manager: Optional[ConnectionManager] = None
    _email_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="notif_email")
    _memory_cache: Dict[str, Any] = {"summary": None, "ts": datetime.min}

    @classmethod
    def _get_cm(cls) -> ConnectionManager:
        if cls._conn_manager is None:
            cls._conn_manager = ConnectionManager("config/config.yaml")
        return cls._conn_manager

    @classmethod
    def create_or_update_notification(
        cls,
        notification_type: str,
        event_code: str,
        created_by_component: str,
        severity: str,
        title: str,
        message: str,
        correlation_key: Optional[str] = None,
        run_id: Optional[str] = None,
        batch_log_id: Optional[int] = None,
        summary_id: Optional[int] = None,
        source_tag: Optional[str] = None,
        source_table: Optional[str] = None,
        layer: Optional[str] = None,
    ) -> Optional[int]:
        """
        Create a new notification or correlate with an existing open incident.
        Successes (INFO) are ignored and never written to metadata.notifications.
        """
        sev = severity.upper()
        if sev == "INFO":
            # Rule 1: Don't create INFO notifications for successful pipeline runs
            return None

        cm = cls._get_cm()
        notification_id = None
        is_new_incident = False

        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    # Check if an open/unresolved incident exists with this correlation_key
                    if correlation_key:
                        cur.execute(
                            """
                            SELECT notification_id, affected_count 
                            FROM metadata.notifications
                            WHERE correlation_key = %s AND status != 'RESOLVED'
                            ORDER BY created_at DESC LIMIT 1
                            """,
                            (correlation_key,)
                        )
                        existing = cur.fetchone()
                        if existing:
                            notification_id, count = existing
                            # Rule 2 & 7: Update existing notification inside deduplication/correlation window
                            cur.execute(
                                """
                                UPDATE metadata.notifications
                                SET affected_count = %s,
                                    last_seen_at = CURRENT_TIMESTAMP,
                                    updated_at = CURRENT_TIMESTAMP,
                                    message = %s,
                                    run_id = COALESCE(%s, run_id),
                                    batch_log_id = COALESCE(%s, batch_log_id),
                                    summary_id = COALESCE(%s, summary_id)
                                WHERE notification_id = %s
                                """,
                                (count + 1, message, run_id, batch_log_id, summary_id, notification_id)
                            )
                            conn.commit()
                            cls._invalidate_cache()
                            return notification_id

                    # Insert new incident row
                    cur.execute(
                        """
                        INSERT INTO metadata.notifications (
                            notification_type, event_code, created_by_component,
                            severity, title, message, correlation_key, affected_count,
                            run_id, batch_log_id, summary_id, source_tag, source_table, layer, status
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, 1,
                            %s, %s, %s, %s, %s, %s, 'NEW'
                        ) RETURNING notification_id
                        """,
                        (
                            notification_type, event_code, created_by_component,
                            sev, title, message, correlation_key,
                            run_id, batch_log_id, summary_id, source_tag, source_table, layer
                        )
                    )
                    row = cur.fetchone()
                    if row:
                        notification_id = row[0]
                        is_new_incident = True
                conn.commit()
                cls._invalidate_cache()

            # Rule 3 (Level 2): Send email alert only for new ERROR/CRITICAL incidents
            if is_new_incident and sev in ("ERROR", "CRITICAL"):
                cls._email_pool.submit(
                    cls._send_email_async,
                    {
                        "notification_id": notification_id,
                        "notification_type": notification_type,
                        "event_code": event_code,
                        "created_by_component": created_by_component,
                        "severity": sev,
                        "title": title,
                        "message": message,
                        "run_id": str(run_id) if run_id else "N/A",
                        "source_tag": source_tag or "N/A",
                        "source_table": source_table or "N/A",
                        "layer": layer or "N/A",
                    }
                )

        except Exception as e:
            log.error(f"Failed to create/update notification: {e}")

        return notification_id

    @classmethod
    def list_notifications(
        cls,
        status_filter: Optional[str] = None,
        severity_filter: Optional[str] = None,
        type_filter: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List notifications with optional filters."""
        cm = cls._get_cm()
        results = []
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT notification_id, created_at, updated_at, last_seen_at,
                               notification_type, event_code, created_by_component,
                               severity, title, message, correlation_key, affected_count,
                               run_id, batch_log_id, summary_id, source_tag, source_table, layer,
                               status, acknowledged, acknowledged_by, acknowledged_at,
                               resolved, resolved_by, resolved_at
                        FROM metadata.notifications
                        WHERE 1=1
                    """
                    params = []
                    if status_filter:
                        if status_filter.upper() == "ACTIVE":
                            query += " AND status IN ('NEW', 'ACKNOWLEDGED')"
                        else:
                            query += " AND status = %s"
                            params.append(status_filter.upper())
                    if severity_filter:
                        query += " AND severity = %s"
                        params.append(severity_filter.upper())
                    if type_filter:
                        query += " AND notification_type = %s"
                        params.append(type_filter.upper())

                    query += " ORDER BY (status = 'NEW') DESC, (severity = 'CRITICAL') DESC, last_seen_at DESC LIMIT %s"
                    params.append(limit)

                    cur.execute(query, tuple(params))
                    columns = [desc[0] for desc in cur.description]
                    for row in cur.fetchall():
                        results.append(dict(zip(columns, row)))
        except Exception as e:
            log.error(f"Failed to list notifications: {e}")
        return results

    @classmethod
    def get_unread_summary_cached(cls) -> Dict[str, Any]:
        """
        Returns cached summary of unread/active notifications.
        Prevents noisy database queries on Streamlit refreshes (10s TTL).
        """
        # Check in-memory 10s TTL cache first
        now = datetime.now()
        if cls._memory_cache["summary"] is not None and (now - cls._memory_cache["ts"]).total_seconds() < 10:
            return cls._memory_cache["summary"]

        cm = cls._get_cm()
        summary = {"total_new": 0, "errors": 0, "warnings": 0, "criticals": 0}
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT severity, COUNT(*)
                        FROM metadata.notifications
                        WHERE status = 'NEW'
                        GROUP BY severity
                        """
                    )
                    for sev, cnt in cur.fetchall():
                        summary["total_new"] += cnt
                        if sev == "CRITICAL":
                            summary["criticals"] += cnt
                            summary["errors"] += cnt
                        elif sev == "ERROR":
                            summary["errors"] += cnt
                        elif sev == "WARNING":
                            summary["warnings"] += cnt

            cls._memory_cache["summary"] = summary
            cls._memory_cache["ts"] = now
        except Exception as e:
            log.error(f"Failed to fetch unread notification summary: {e}")
        return summary

    @classmethod
    def _invalidate_cache(cls):
        cls._memory_cache["summary"] = None
        cls._memory_cache["ts"] = datetime.min

    @classmethod
    def acknowledge_notification(cls, notification_id: int, username: str = "operator") -> bool:
        """Mark notification as ACKNOWLEDGED."""
        cm = cls._get_cm()
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE metadata.notifications
                        SET status = 'ACKNOWLEDGED',
                            acknowledged = true,
                            acknowledged_by = %s,
                            acknowledged_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE notification_id = %s
                        """,
                        (username, notification_id)
                    )
                conn.commit()
                cls._invalidate_cache()
            return True
        except Exception as e:
            log.error(f"Failed to acknowledge notification {notification_id}: {e}")
            return False

    @classmethod
    def resolve_notification(cls, notification_id: int, username: str = "operator", auto: bool = False) -> bool:
        """Mark notification as RESOLVED."""
        cm = cls._get_cm()
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE metadata.notifications
                        SET status = 'RESOLVED',
                            resolved = true,
                            resolved_by = %s,
                            resolved_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE notification_id = %s
                        """,
                        (username, notification_id)
                    )
                conn.commit()
                cls._invalidate_cache()
            return True
        except Exception as e:
            log.error(f"Failed to resolve notification {notification_id}: {e}")
            return False

    @classmethod
    def auto_resolve_by_correlation_key(cls, correlation_key: str) -> int:
        """
        Automatically resolve all open notifications matching correlation_key.
        Called when a rerun succeeds or connection test passes.
        """
        if not correlation_key:
            return 0
        cm = cls._get_cm()
        resolved_count = 0
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE metadata.notifications
                        SET status = 'RESOLVED',
                            resolved = true,
                            resolved_by = 'System (Auto-Resolve)',
                            resolved_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE correlation_key = %s AND status != 'RESOLVED'
                        RETURNING notification_id
                        """,
                        (correlation_key,)
                    )
                    rows = cur.fetchall()
                    resolved_count = len(rows)
                conn.commit()
                if resolved_count > 0:
                    cls._invalidate_cache()
                    log.info(f"Auto-resolved {resolved_count} notification(s) for key '{correlation_key}'")
        except Exception as e:
            log.error(f"Failed auto-resolving correlation_key {correlation_key}: {e}")
        return resolved_count

    @classmethod
    def cleanup_old_notifications(cls) -> int:
        """
        Enforce configurable retention policy (`pipeline.notification_retention` in config.yaml).
        Deletes old resolved notifications to prevent unbounded growth.
        """
        cm = cls._get_cm()
        raw_cfg = getattr(cm, "_raw_config", {}).get("pipeline", {}).get("notification_retention", {})
        info_days = raw_cfg.get("info", 30)
        warning_days = raw_cfg.get("warning", 90)
        error_days = raw_cfg.get("error", 365)

        deleted = 0
        try:
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        DELETE FROM metadata.notifications
                        WHERE status = 'RESOLVED' AND (
                            (severity = 'INFO' AND last_seen_at < NOW() - INTERVAL '%s days') OR
                            (severity = 'WARNING' AND last_seen_at < NOW() - INTERVAL '%s days') OR
                            (severity IN ('ERROR', 'CRITICAL') AND last_seen_at < NOW() - INTERVAL '%s days')
                        )
                        """,
                        (info_days, warning_days, error_days)
                    )
                    deleted = cur.rowcount
                conn.commit()
                if deleted > 0:
                    log.info(f"Cleaned up {deleted} old resolved notification(s) per retention policy.")
        except Exception as e:
            log.error(f"Failed cleaning up old notifications: {e}")
        return deleted

    @classmethod
    def _send_email_async(cls, payload: Dict[str, Any]):
        """
        Dispatch Level 2 email notification in background thread.
        Never raises exceptions; logs warnings cleanly if SMTP is unconfigured.
        """
        try:
            # Check if SMTP profile is configured in connection manager
            cm = cls._get_cm()
            if not cm.has_connection("smtp_alert_gateway"):
                # Silently skip if operator hasn't configured SMTP profile yet
                return

            smtp_cfg = cm.get("smtp_alert_gateway")
            # Build actionable email body
            subject = f"[ETL {payload['severity']}] {payload['title']} (Run #{payload['run_id'][:8]})"
            body = f"""
================================================================================
                       ETL OPERATIONAL ALERT ({payload['severity']})
================================================================================
Event Code : {payload['event_code']}
Component  : {payload['created_by_component']}
Source Tag : {payload['source_tag']}
Table      : {payload['source_table']}
Layer      : {payload['layer']}
Run ID     : {payload['run_id']}

Details:
{payload['message']}

Quick Action Links:
👉 Notification Center : http://pipeline-gui/Monitoring/Notification_Center
👉 Connection Manager  : http://pipeline-gui/Admin/Connection_Manager?tag={payload['source_tag']}
👉 Run History Log     : http://pipeline-gui/Monitoring/Run_History?run_id={payload['run_id']}
================================================================================
"""
            msg = MIMEMultipart()
            msg["From"] = smtp_cfg.username or "etl-alerts@company.com"
            msg["To"] = smtp_cfg.connection_options.get("alert_recipients", "data-ops@company.com") if hasattr(smtp_cfg, "connection_options") else "data-ops@company.com"
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            # Support Local/Mock Mailbox for environments without an active SMTP server
            if smtp_cfg.host.startswith("file://") or smtp_cfg.host.lower() in ("console", "mock", "local", "localhost"):
                from pathlib import Path
                outdir = Path(smtp_cfg.host.replace("file://", "")) if smtp_cfg.host.startswith("file://") else Path("logs/email_outbox")
                outdir.mkdir(parents=True, exist_ok=True)
                fname = outdir / f"alert_{payload['notification_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.eml"
                fname.write_text(msg.as_string(), encoding="utf-8")
                log.info(f"Saved mock email alert to {fname}")
            else:
                # Short 2.0s timeout to never block background worker
                with smtplib.SMTP(smtp_cfg.host, int(smtp_cfg.port or 587), timeout=2.0) as server:
                    if smtp_cfg.username and smtp_cfg.password:
                        server.starttls()
                        server.login(smtp_cfg.username, smtp_cfg.password)
                    server.send_message(msg)

            # Mark email_sent in DB
            with cm.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE metadata.notifications SET email_sent = true, email_sent_at = CURRENT_TIMESTAMP WHERE notification_id = %s",
                        (payload["notification_id"],)
                    )
                conn.commit()
            log.info(f"Sent email alert for notification #{payload['notification_id']}")
        except Exception as e:
            log.warning(f"Could not send email alert for notification #{payload.get('notification_id')}: {e}")
