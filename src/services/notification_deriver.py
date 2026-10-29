# src/services/notification_deriver.py
"""
NotificationDeriver — Derivation & Cross-Run Correlation Engine
===============================================================
Decoupled from BatchLogger. Analyzes batch log events, summaries, and connection
test outcomes to correlate failures across runs or automatically resolve open incidents.
"""
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Optional, Dict, Any

from src.services.notification_service import NotificationService

log = logging.getLogger("NotificationDeriver")


class NotificationDeriver:
    """
    Analyzes log events and connectivity checks to generate or auto-resolve notifications.
    Uses an internal thread pool (`on_batch_log_written`) so calling components never block.
    """
    _deriver_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="notif_derive")

    @classmethod
    def on_batch_log_written(cls, log_event: Dict[str, Any]):
        """Non-blocking entry point called after BatchLogger writes a row."""
        cls._deriver_pool.submit(cls._derive_from_log_entry_sync, log_event)

    @classmethod
    def on_connection_test_finished(cls, tag: str, test_result: Dict[str, Any]):
        """Non-blocking entry point called after ConnectionService runs a test."""
        cls._deriver_pool.submit(cls._derive_from_connection_test_sync, tag, test_result)

    @classmethod
    def _derive_from_log_entry_sync(cls, event: Dict[str, Any]):
        """Synchronous derivation logic executed in background thread."""
        try:
            status = str(event.get("status", "")).upper()
            source_tag = event.get("source_tag", "unknown")
            source_table = event.get("source_table", "unknown")
            layer = event.get("load_type") or event.get("layer") or "table"
            run_id = event.get("run_id")
            error_message = event.get("error_message") or "Unknown error reported during execution."

            # 1. Automatic Resolution: if run succeeded, resolve open table/connection incidents
            if status in ("SUCCESS", "COMPLETED", "RECONCILIATION_WARNING"):
                NotificationService.auto_resolve_by_correlation_key(f"load_failed:{source_tag}.{source_table}")
                # Also check if connection was marked failed earlier and resolve if this table ran fine
                NotificationService.auto_resolve_by_correlation_key(f"connection_failure:{source_tag}")
                return

            # 2. Failure Handling & Correlation
            if status in ("FAILED", "RECONCILIATION_FAILED", "CONNECTION_FAILED"):
                # Check if failure indicates a connection/source outage vs data transformation defect
                err_lower = error_message.lower()
                is_connection_error = any(
                    k in err_lower for k in [
                        "connection", "timeout", "tns:", "ora-12541", "ora-12154",
                        "socket", "refused", "unreachable", "communication link failure"
                    ]
                )

                if is_connection_error or status == "CONNECTION_FAILED":
                    # Correlate across all tables using the same source database
                    correlation_key = f"connection_failure:{source_tag}"
                    title = f"Database Source '{source_tag}' Unavailable"
                    event_code = "CONNECTION_FAILED"
                    notification_type = "CONNECTION"
                    created_by = "PIPELINE"
                    severity = "CRITICAL"
                elif status == "RECONCILIATION_FAILED":
                    correlation_key = f"reconcile_failed:{source_tag}.{source_table}"
                    title = f"Reconciliation Failed: {source_tag}.{source_table}"
                    event_code = "RECONCILIATION_FAILED"
                    notification_type = "QUALITY"
                    created_by = "RECONCILER"
                    severity = "ERROR"
                else:
                    # Standard pipeline table extraction/load failure
                    correlation_key = f"load_failed:{source_tag}.{source_table}"
                    title = f"Pipeline Load Failed: {source_table} ({layer})"
                    event_code = "PIPELINE_LOAD_FAILED"
                    notification_type = "PIPELINE"
                    created_by = "PIPELINE"
                    severity = "ERROR"

                NotificationService.create_or_update_notification(
                    notification_type=notification_type,
                    event_code=event_code,
                    created_by_component=created_by,
                    severity=severity,
                    title=title,
                    message=f"[{event_code}] {error_message}",
                    correlation_key=correlation_key,
                    run_id=run_id,
                    source_tag=source_tag,
                    source_table=source_table,
                    layer=layer,
                )
        except Exception as e:
            log.error(f"Error deriving notification from log entry: {e}")

    @classmethod
    def _derive_from_connection_test_sync(cls, tag: str, result: Dict[str, Any]):
        """Synchronous derivation logic for connection health tests."""
        try:
            status = str(result.get("status", "")).upper()
            if status == "SUCCESS":
                NotificationService.auto_resolve_by_correlation_key(f"connection_failure:{tag}")
            elif status == "FAILED":
                error_msg = result.get("error", "Connection test failed.")
                NotificationService.create_or_update_notification(
                    notification_type="CONNECTION",
                    event_code="CONNECTION_TEST_FAILED",
                    created_by_component="CONNECTION",
                    severity="ERROR",
                    title=f"Connection Test Failed: '{tag}'",
                    message=error_msg,
                    correlation_key=f"connection_failure:{tag}",
                    source_tag=tag,
                )
        except Exception as e:
            log.error(f"Error deriving notification from connection test: {e}")
