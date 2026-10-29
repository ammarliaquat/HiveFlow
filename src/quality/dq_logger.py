# src/quality/dq_logger.py
import json
import logging
from typing import List, Dict, Any
from src.connections.connection_manager import ConnectionManager
from src.services.notification_service import NotificationService

logger = logging.getLogger("DQLogger")

class DQLogger:
    def __init__(self, conn_manager: ConnectionManager, run_id: str):
        self.conn_manager = conn_manager
        self.run_id = run_id

    def log_results(self, table_id: int, table_name: str, source_tag: str, results: List[Dict[str, Any]]):
        """
        Inserts rule results into metadata.data_quality_results and
        batches notifications.
        results element format:
        {
            "rule_id": 1,
            "status": "PASS",
            "expected_value": "0",
            "actual_value": "15",
            "failed_rows": 15,
            "failure_pct": 0.05,
            "execution_ms": 120,
            "severity": "WARNING",
            "rule_type": "NOT_NULL"
        }
        """
        if not results:
            return

        critical_failures = 0
        warning_failures = 0

        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    for r in results:
                        query = """
                            INSERT INTO metadata.data_quality_results (
                                run_id, rule_id, status, expected_value, actual_value, 
                                failed_rows, failure_pct, execution_ms
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cur.execute(query, (
                            self.run_id, r["rule_id"], r["status"], r["expected_value"],
                            r["actual_value"], r["failed_rows"], r["failure_pct"], r["execution_ms"]
                        ))

                        if r["status"] == "FAIL":
                            if r.get("severity") == "CRITICAL":
                                critical_failures += 1
                            else:
                                warning_failures += 1
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to log DQ results: {e}")

        # Batch Notification
        if critical_failures > 0 or warning_failures > 0:
            total_failed = critical_failures + warning_failures
            sev = "CRITICAL" if critical_failures > 0 else "WARNING"
            
            NotificationService.create_or_update_notification(
                notification_type="QUALITY",
                event_code="DQ_FAILURE",
                created_by_component="DQ_ENGINE",
                severity=sev,
                title=f"Data Quality Failed: {table_name}",
                message=f"Table {table_name} failed {total_failed} DQ rules ({critical_failures} Critical, {warning_failures} Warning).",
                correlation_key=f"dq_failure:{table_id}",
                run_id=self.run_id,
                source_tag=source_tag,
                source_table=table_name
            )
        else:
            # Auto-resolve if all passed
            NotificationService.auto_resolve_by_correlation_key(f"dq_failure:{table_id}")


    def quarantine_failures(self, rule_id: int, failures_df):
        """
        Extracts up to 100 failing rows using toLocalIterator and inserts into data_quality_failures.
        """
        try:
            # Limit to 100 locally and convert to dictionary
            failed_rows = [row.asDict(recursive=True) for row in failures_df.limit(100).toLocalIterator()]
            if not failed_rows:
                return

            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    for row_dict in failed_rows:
                        # Extract a pseudo PK if available, else just stringify
                        pk_val = row_dict.get("id") or row_dict.get("ID") or "UNKNOWN"
                        
                        query = """
                            INSERT INTO metadata.data_quality_failures (
                                run_id, rule_id, pk_value, reason, sample_json
                            ) VALUES (%s, %s, %s, %s, %s)
                        """
                        cur.execute(query, (
                            self.run_id, rule_id, str(pk_val), 
                            "Quarantined by Technical DQ Engine",
                            json.dumps(row_dict, default=str)
                        ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to quarantine DQ failures for rule {rule_id}: {e}")

    def log_table_score(self, table_id: int, layer: str, quality_score: float, status: str):
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        INSERT INTO metadata.table_quality_scores (
                            run_id, table_id, execution_layer, quality_score, status
                        ) VALUES (%s, %s, %s, %s, %s)
                    """
                    cur.execute(query, (self.run_id, table_id, layer, quality_score, status))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to log table quality score: {e}")
