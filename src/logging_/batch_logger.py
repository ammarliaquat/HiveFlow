# src/logging_/batch_logger.py
"""
Writes one log entry per table run to metadata.batch_log.
Every ETL run has a shared run_id UUID so all tables in one run are grouped.
"""
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List
import uuid
import psycopg2

from src.constants import MetadataConstants, RunStatus, PipelinePhase
from src.connections.connection_manager import ConnectionManager
from src.services.notification_deriver import NotificationDeriver
from src.security.data_governance import DataGovernanceService


import threading
import time
import atexit
import signal

class ExecutionLeaseLostError(RuntimeError):
    """Raised when a worker can no longer prove ownership of a RUNNING attempt."""

    error_code = "LEASE_FENCED"

def _sanitize_db_params(params: tuple) -> tuple:
    """Ensure no numpy int64/float64 or PySpark scalar types reach psycopg2 parameter binding."""
    if not params:
        return params
    clean = []
    for p in params:
        if p.__class__.__name__ in ("int64", "int32", "int16", "int8"):
            clean.append(int(p))
        elif p.__class__.__name__ in ("float64", "float32"):
            clean.append(float(p))
        else:
            clean.append(p)
    return tuple(clean)


class WorkerHeartbeatThread(threading.Thread):
    """
    Background daemon maintaining overall worker run liveness in metadata.gui_job_registry
    and metadata.job_scope_claim every interval_sec seconds while status == RUNNING.
    Prevents heartbeat_reaper from fencing active runs during heavy processing.
    """
    def __init__(
        self,
        conn_manager: ConnectionManager,
        run_id: str,
        interval_sec: float = 3.0,
    ):
        super().__init__(daemon=True, name=f"WorkerHeartbeat-{run_id}")
        self.conn_manager = conn_manager
        self.run_id = str(run_id)
        self.interval_sec = interval_sec
        self.running = True

    def stop(self):
        self.running = False

    def run(self):
        while self.running:
            time.sleep(self.interval_sec)
            if not self.running:
                break
            try:
                with self.conn_manager.get_metadata_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE metadata.gui_job_registry
                            SET last_heartbeat = CURRENT_TIMESTAMP
                            WHERE run_id::text = %s
                              AND status = 'RUNNING'
                            """,
                            (self.run_id,)
                        )
                        cur.execute(
                            """
                            UPDATE metadata.job_scope_claim
                            SET last_heartbeat = CURRENT_TIMESTAMP,
                                lease_expires_at = CURRENT_TIMESTAMP + INTERVAL '120 second'
                            WHERE run_id::text = %s
                              AND status = 'ACTIVE'
                              AND released_at IS NULL
                            """,
                            (self.run_id,)
                        )
                    conn.commit()
            except Exception:
                pass


class BatchHeartbeatThread(threading.Thread):
    """
    Background daemon thread that continuously updates last_progress_update and elapsed_seconds
    in metadata.batch_log every 2 seconds for a specific log_id while status == RUNNING.
    Also keeps gui_job_registry.last_heartbeat fresh if run_id is supplied.
    """
    def __init__(
        self,
        conn_manager: ConnectionManager,
        log_id: int,
        start_time: datetime,
        attempt_number: int,
        run_id: Optional[str] = None,
    ):
        super().__init__(daemon=True, name=f"BatchHeartbeat-{log_id}")
        self.conn_manager = conn_manager
        self.log_id = log_id
        self.start_time = start_time
        self.attempt_number = int(attempt_number)
        self.run_id = str(run_id) if run_id else None
        self.running = True

    def stop(self):
        self.running = False

    def run(self):
        while self.running:
            time.sleep(2)
            if not self.running:
                break
            try:
                now = datetime.now()
                elapsed = round((now - self.start_time).total_seconds(), 2)
                with self.conn_manager.get_metadata_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                            SET elapsed_seconds = %s,
                                last_heartbeat = CURRENT_TIMESTAMP
                            WHERE id = %s
                              AND status = 'RUNNING'
                              AND attempt_number = %s
                            """,
                            (elapsed, self.log_id, self.attempt_number)
                        )
                        if self.run_id:
                            cur.execute(
                                """
                                UPDATE metadata.gui_job_registry
                                SET last_heartbeat = CURRENT_TIMESTAMP
                                WHERE run_id::text = %s
                                  AND status = 'RUNNING'
                                """,
                                (self.run_id,)
                            )
                    conn.commit()
            except Exception:
                pass


class BatchLogger:

    def __init__(self, conn_manager: ConnectionManager, run_id: str, spark_app_id: str = ""):
        self._conn_manager = conn_manager
        self.run_id = run_id
        self.spark_app_id = spark_app_id
        self._active_heartbeats = {}
        self._attempt_numbers: Dict[int, int] = {}
        # Cancellation guard is registered once by ETL._register_cancellation_guards;
        # BatchLogger must NOT register its own atexit to avoid duplicate CANCELLED updates (M-9).

    def _register_process_guards(self):
        """Register process exit & signal handlers to mark active RUNNING rows as CANCELLED on exit."""
        def exit_handler():
            self.mark_run_cancelled("Process terminated or cancelled by operator.")
        atexit.register(exit_handler)

    def renew_active_lease_or_raise(self, log_id: int) -> None:
        """Atomically renew the owned lease before irreversible Gold DML.

        Metadata failures and lost ownership are intentionally propagated: callers
        must not begin a new irreversible target operation without this proof.
        """
        attempt_number = self._attempt_numbers.get(int(log_id))
        if attempt_number is None:
            raise ExecutionLeaseLostError(f"LEASE_FENCED: missing attempt identity for batch_log id {log_id}")

        with self._conn_manager.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                    SET last_heartbeat = CURRENT_TIMESTAMP
                    WHERE id = %s
                      AND status = 'RUNNING'
                      AND attempt_number = %s
                    RETURNING id
                    """,
                    (int(log_id), attempt_number),
                )
                renewed = cur.fetchone()
            conn.commit()

        if renewed is None:
            raise ExecutionLeaseLostError(f"LEASE_FENCED: execution lease is not active for batch_log id {log_id}")

    def log_start(
        self,
        source_tag: str,
        source_table: str,
        target_table: str,
        load_type: str,
        priority: int,
        target_tag: Optional[str] = None,
        attempt_number: Optional[int] = None,
        policy_version_id: Optional[str] = None,
        policy_fingerprint: Optional[str] = None,
    ) -> int:
        """Insert a RUNNING record and return its log_id."""
        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                priority_val = int(priority) if priority is not None else None
                start_time = datetime.now()
                with conn.cursor() as cur:
                    if attempt_number is None or attempt_number <= 1:
                        cur.execute(f"""
                            SELECT COALESCE(MAX(attempt_number), 0) + 1
                            FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                            WHERE run_id = %s AND source_table = %s AND load_type = %s
                        """, [self.run_id, source_table, load_type])
                        row = cur.fetchone()
                        next_att = row[0] if row else 1
                        attempt_number = max(int(attempt_number or 1), int(next_att or 1))
                    query = f"""
                        INSERT INTO {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE} (
                            run_id, source_tag, target_tag, source_table, target_table, load_type, priority,
                            source_count, extracted_count, processed_count, target_count, load_pct, start_time, end_time,
                            elapsed_seconds, status, current_phase, last_progress_update, last_heartbeat, attempt_number,
                            error_message, spark_app_id, notes, policy_version_id, policy_fingerprint
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            NULL, NULL, 0, NULL, NULL, CURRENT_TIMESTAMP, NULL,
                            0, 'RUNNING', '{PipelinePhase.EXTRACTING.value}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s,
                            NULL, %s, NULL, %s, %s
                        ) RETURNING id
                    """
                    cur.execute(query, (
                        self.run_id, source_tag, target_tag, source_table, target_table, load_type, priority_val,
                        int(attempt_number), self.spark_app_id,
                        policy_version_id, policy_fingerprint,
                    ))
                    log_id = cur.fetchone()[0]
                    self._attempt_numbers[log_id] = int(attempt_number)
                conn.commit()

                # Start real-time heartbeat daemon thread for this log_id
                try:
                    hb = BatchHeartbeatThread(
                        self._conn_manager,
                        log_id,
                        start_time,
                        int(attempt_number),
                        run_id=self.run_id,
                    )
                    hb.start()
                    self._active_heartbeats[log_id] = hb
                except Exception:
                    pass

            return log_id
        except Exception as e:
            from src.exceptions import ExecutionOwnershipRequiredError
            raise ExecutionOwnershipRequiredError(
                f"Could not establish batch_log ownership: {e}"
            ) from e


    def update_phase(
        self,
        log_id: int,
        phase: 'PipelinePhase',
        extracted_count: Optional[int] = None,
        processed_count: Optional[int] = None,
        source_count: Optional[int] = None,
        current_chunk: Optional[int] = None,
        total_chunks: Optional[int] = None,
        rolling_rps: Optional[float] = None,
        reconciliation_status: Optional[str] = None,
    ):
        """Update current_phase, extracted_count, processed_count, progress_pct, rows_per_second, and ETA in batch_log."""
        if not log_id:
            return
        if extracted_count is not None:
            extracted_count = int(extracted_count)
        if processed_count is not None:
            processed_count = int(processed_count)
        if source_count is not None:
            source_count = int(source_count)
        try:
            now = datetime.now()
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT start_time, source_count, processed_count, rolling_rows_per_second, attempt_number FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE} WHERE id = %s",
                        (log_id,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return
                    start_time = row[0] if row else None
                    effective_src_count = source_count or (row[1] if row else None)
                    effective_proc_count = processed_count if processed_count is not None else (row[2] if row else 0)
                    prev_rolling = float(row[3]) if (row and row[3] is not None) else None
                    attempt_number = int(row[4])

                    progress_pct = None
                    rows_per_sec = None
                    eta = None
                    calc_rolling = None

                    if effective_src_count and effective_proc_count is not None and effective_src_count > 0:
                        progress_pct = min(round((effective_proc_count / float(effective_src_count)) * 100.0, 2), 100.0)

                    if start_time and effective_proc_count and effective_proc_count > 0:
                        elapsed_sec = (now - start_time).total_seconds()
                        if elapsed_sec > 0:
                            rows_per_sec = round(effective_proc_count / elapsed_sec, 2)
                            calc_rolling = rolling_rps or (round(0.3 * rows_per_sec + 0.7 * prev_rolling, 2) if prev_rolling else rows_per_sec)
                            if effective_src_count and effective_src_count > effective_proc_count and calc_rolling > 0:
                                remaining_rows = effective_src_count - effective_proc_count
                                remaining_sec = remaining_rows / calc_rolling
                                from datetime import timedelta
                                eta = now + timedelta(seconds=remaining_sec)

                    safe_extracted = int(extracted_count) if extracted_count is not None else None
                    safe_processed = int(processed_count) if processed_count is not None else None
                    safe_source = int(source_count) if source_count is not None else None
                    safe_pct = float(progress_pct) if progress_pct is not None else None
                    safe_rps = float(rows_per_sec) if rows_per_sec is not None else None
                    safe_rolling = float(rolling_rps) if rolling_rps is not None else (float(calc_rolling) if calc_rolling is not None else (float(rows_per_sec) if rows_per_sec is not None else None))
                    safe_log_id = int(log_id)

                    cur.execute(
                        f"""
                        UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        SET current_phase = %s,
                            extracted_count = COALESCE(%s, extracted_count),
                            processed_count = COALESCE(%s, processed_count),
                            source_count = COALESCE(%s, source_count),
                            progress_pct = COALESCE(%s, progress_pct),
                            rows_per_second = COALESCE(%s, rows_per_second),
                            rolling_rows_per_second = COALESCE(%s, rolling_rows_per_second),
                            current_chunk = COALESCE(%s, current_chunk),
                            total_chunks = COALESCE(%s, total_chunks),
                            reconciliation_status = COALESCE(%s, reconciliation_status),
                            estimated_completion = COALESCE(%s, estimated_completion),
                            last_progress_update = CURRENT_TIMESTAMP
                        WHERE id = %s
                          AND status = 'RUNNING'
                          AND attempt_number = %s
                        """,
                        _sanitize_db_params((phase.value if hasattr(phase, 'value') else phase, safe_extracted, safe_processed, safe_source, safe_pct, safe_rps, safe_rolling, current_chunk, total_chunks, reconciliation_status, eta, safe_log_id, attempt_number))
                    )
                conn.commit()
        except Exception as e:
            print(f"Error updating phase in batch_log: {e}")

    def update_chunk_progress(
        self,
        log_id: int,
        actual_chunk_rows: int,
        chunk_number: int = 1,
        total_chunks: int = 1,
        stage: str = "BRONZE_WRITE",
    ) -> None:
        """
        Post-commit progress update after a chunk/partition is successfully persisted to target.
        Atomically increments processed_count, updates chunk counters, progress_pct,
        rolling EWMA throughput, ETA, and last_progress_update.
        """
        if not log_id:
            return
        try:
            now = datetime.now()
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT start_time, source_count, processed_count, rolling_rows_per_second, attempt_number
                        FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        WHERE id = %s
                        """,
                        (log_id,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return
                    start_time, source_count, prev_processed, prev_rolling, attempt_number = row
                    new_processed = (prev_processed or 0) + actual_chunk_rows
                    
                    progress_pct = None
                    if source_count and source_count > 0:
                        progress_pct = min(round((new_processed / float(source_count)) * 100.0, 2), 100.0)
                    
                    elapsed_sec = (now - start_time).total_seconds() if start_time else 0.0
                    instant_rps = round(new_processed / elapsed_sec, 2) if elapsed_sec > 0 else 0.0
                    
                    prev_r = float(prev_rolling) if prev_rolling is not None else instant_rps
                    rolling_rps = round(0.3 * instant_rps + 0.7 * prev_r, 2)
                    
                    eta = None
                    if source_count and source_count > new_processed and rolling_rps > 0:
                        remaining_rows = source_count - new_processed
                        remaining_sec = remaining_rows / rolling_rps
                        from datetime import timedelta
                        eta = now + timedelta(seconds=remaining_sec)
                    
                    cur.execute(
                        f"""
                        UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        SET current_phase = %s,
                            processed_count = %s,
                            progress_pct = %s,
                            rows_per_second = %s,
                            rolling_rows_per_second = %s,
                            current_chunk = %s,
                            total_chunks = %s,
                            chunks_completed = COALESCE(chunks_completed, 0) + 1,
                            estimated_completion = %s,
                            last_progress_update = CURRENT_TIMESTAMP
                        WHERE id = %s
                          AND status = 'RUNNING'
                          AND attempt_number = %s
                        """,
                        _sanitize_db_params((stage, new_processed, progress_pct, instant_rps, rolling_rps, chunk_number, total_chunks, eta, log_id, attempt_number))
                    )
                conn.commit()
        except Exception as e:
            print(f"Error in update_chunk_progress: {e}")

    @staticmethod
    def detect_and_mark_stalled_runs(conn_manager: ConnectionManager, heartbeat_timeout_sec: int = 60, progress_timeout_sec: int = 300, reference_time: Optional[datetime] = None) -> List[str]:
        """
        Records slow-progress observations for active jobs without revoking their
        execution lease.  Lease expiry is owned by the DB-time reaper; progress
        latency is operational telemetry only.
        """
        stalled_ids = []
        try:
            with conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        SET notes = COALESCE(notes, '') || ' | [SLOW_PROGRESS]: Progress timeout exceeded.'
                        WHERE status = 'RUNNING'
                          AND last_progress_update IS NOT NULL
                          AND last_progress_update < CURRENT_TIMESTAMP - (%s * INTERVAL '1 second')
                        RETURNING run_id::text
                        """,
                        (int(progress_timeout_sec),)
                    )
                    stalled_ids = [r[0] for r in cur.fetchall()]
                conn.commit()
        except Exception as e:
            print(f"Error detecting stalled runs: {e}")
        return stalled_ids

    def update_stage_metrics(self, log_id: Optional[int], stage_metrics: dict) -> None:
        """
        Merge/update the stage_metrics JSONB payload for a batch_log row.
        """
        if not log_id:
            return
        attempt_number = self._attempt_numbers.get(int(log_id))
        if attempt_number is None:
            raise RuntimeError(f"Missing attempt identity for batch_log id {log_id}")
        try:
            import json
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    def _json_default(o):
                        if hasattr(o, "item"):
                            return o.item()
                        if isinstance(o, (datetime, date)):
                            return o.isoformat()
                        return str(o)
                    cur.execute(
                        f"""
                        UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        SET stage_metrics = COALESCE(stage_metrics, '{{}}'::jsonb) || %s::jsonb,
                            last_progress_update = CURRENT_TIMESTAMP
                        WHERE id = %s
                          AND status = 'RUNNING'
                          AND attempt_number = %s
                        """,
                        (json.dumps(stage_metrics, default=_json_default), log_id, attempt_number)
                    )
                conn.commit()
        except Exception as e:
            print(f"Error updating stage_metrics in batch_log: {e}")

    def mark_run_cancelled(self, reason: str = "Process cancelled or interrupted.") -> None:
        """
        Signal-safe: marks every RUNNING row for this run_id as CANCELLED immediately.
        Called from atexit/signal handlers — uses a fresh, short-lived connection.
        Designed to complete in milliseconds.
        """
        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    now = datetime.now()
                    cur.execute(
                        f"""
                        UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        SET status              = 'CANCELLED',
                            current_phase       = '{PipelinePhase.CANCELLED.value}',
                            error_message       = %s,
                            end_time            = %s,
                            last_progress_update = %s
                        WHERE run_id = %s
                          AND status = 'RUNNING'
                        """,
                        (reason, now, now, self.run_id)
                    )
                conn.commit()
        except Exception as e:
            # Last-resort: print to stderr — cannot raise during signal/atexit teardown
            import sys
            print(f"[BatchLogger] CRITICAL: could not mark run cancelled: {e}", file=sys.stderr)

    def mark_run_skipped(self, reason: str = "No active tables eligible for execution.") -> None:
        """
        Marks any pre-registered QUEUED or RUNNING row for this run_id as SKIPPED.
        Prevents runs from being stuck in QUEUED when no eligible tables are found.
        """
        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    now = datetime.now()
                    cur.execute(
                        f"""
                        UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        SET status               = 'SKIPPED',
                            current_phase        = '{PipelinePhase.SKIPPED.value}',
                            notes                = COALESCE(notes, '') || %s,
                            end_time             = %s,
                            last_progress_update = %s
                        WHERE run_id = %s
                          AND status IN ('QUEUED', 'RUNNING')
                        """,
                        (f" | [SKIPPED]: {reason}", now, now, self.run_id)
                    )
                conn.commit()
        except Exception as e:
            import sys
            print(f"[BatchLogger] Could not mark run skipped: {e}", file=sys.stderr)

    def log(
        self,
        source_tag: str,
        source_table: str,
        target_table: str,
        load_type: str,
        priority: int,
        source_count: int,
        target_count: int,
        start_time: datetime,
        end_time: datetime,
        status: str,
        error_message: Optional[str] = None,
        notes: Optional[str] = None,
        target_tag: Optional[str] = None,
        log_id: Optional[int] = None,
    ):
        if not log_id:
            from src.exceptions import ExecutionOwnershipRequiredError

            raise ExecutionOwnershipRequiredError(
                "Final batch status requires an owned batch_log row."
            )
        # Stop real-time background heartbeat for this log_id
        if log_id and log_id in self._active_heartbeats:
            hb = self._active_heartbeats.pop(log_id, None)
            if hb:
                hb.stop()

        error_message = DataGovernanceService.sanitize_exception(error_message, source_table)
        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                elapsed = (end_time - start_time).total_seconds()
                if source_count is not None and target_count is not None:
                    if source_count > 0:
                        pct = min(round((target_count / source_count) * 100.0, 2), 999.99)
                    else:
                        pct = 0.0
                else:
                    pct = None
                
                # Cast counts and metrics to native Python types
                priority_val = int(priority) if priority is not None else None
                source_count_val = int(source_count) if source_count is not None else None
                target_count_val = int(target_count) if target_count is not None else None
                pct_val = float(pct) if pct is not None else None
                elapsed_val = float(elapsed) if elapsed is not None else None
                rows_per_sec = round(target_count_val / elapsed_val, 2) if (elapsed_val and elapsed_val > 0 and target_count_val) else None
                final_phase = PipelinePhase.COMPLETED.value if status == RunStatus.SUCCESS else (PipelinePhase.FAILED.value if status == RunStatus.FAILED else status)

                with conn.cursor() as cur:
                    if log_id:
                        attempt_number = self._attempt_numbers.get(int(log_id))
                        if attempt_number is None:
                            raise RuntimeError(f"Missing attempt identity for batch_log id {log_id}")
                        
                        # Check if row is already terminal SUCCESS (e.g. by AtomicCDCCommitter)
                        cur.execute(
                            f"SELECT status FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE} WHERE id = %s",
                            (log_id,)
                        )
                        st_row = cur.fetchone()
                        is_already_success = bool(st_row and st_row[0] == RunStatus.SUCCESS and status == RunStatus.SUCCESS)

                        query = f"""
                            UPDATE {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                            SET source_count = %s, target_count = %s, load_pct = %s, end_time = CURRENT_TIMESTAMP,
                                elapsed_seconds = %s, status = %s, current_phase = %s,
                                processed_count = COALESCE(processed_count, %s),
                                progress_pct = COALESCE(progress_pct, %s),
                                rows_per_second = %s, last_progress_update = CURRENT_TIMESTAMP,
                                error_message = %s, notes = %s
                            WHERE id = %s
                              AND attempt_number = %s
                        """
                        if not is_already_success:
                            query += " AND status = 'RUNNING'"
                        
                        cur.execute(query, (
                            source_count_val, target_count_val, pct_val,
                            elapsed_val, status, final_phase, target_count_val,
                            pct_val, rows_per_sec,
                            error_message, notes, log_id, attempt_number
                        ))
                        if cur.rowcount != 1 and not is_already_success:
                            raise ExecutionLeaseLostError(
                                f"LEASE_FENCED: batch_log id {log_id} is no longer owned"
                            )
                    else:
                        query = f"""
                            INSERT INTO {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE} (
                                run_id, source_tag, target_tag, source_table, target_table, load_type, priority,
                                source_count, target_count, load_pct, start_time, end_time,
                                elapsed_seconds, status, error_message, spark_app_id, notes
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s
                            )
                        """
                        cur.execute(query, (
                            self.run_id, source_tag, target_tag, source_table, target_table, load_type, priority_val,
                            source_count_val, target_count_val, pct_val, start_time, end_time,
                            elapsed_val, status, error_message, self.spark_app_id, notes
                        ))
                conn.commit()
            NotificationDeriver.on_batch_log_written({
                "run_id": self.run_id,
                "source_tag": source_tag,
                "source_table": source_table,
                "load_type": load_type,
                "status": status,
                "error_message": error_message,
            })
        except Exception as e:
            print(f"Error writing to batch_log: {e}")
            raise

    def log_batch_summary(
        self,
        source_tag: Optional[str],
        layer: str,
        start_ts: datetime,
        end_ts: datetime,
        total_tables: int,
        succeeded: int,
        failed: int,
        skipped: int,
        total_rows: int,
        status: str,
        error_summary: Optional[str] = None,
        target_tag: Optional[str] = None,
        priority: Optional[int] = None,
        action: Optional[str] = None,
        elapsed_seconds: Optional[float] = None,
        rows_per_second: Optional[float] = None,
        run_date: Optional[date] = None,
    ):
        """Write per-run, per-layer execution summary to metadata.batch_summary."""
        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                total_tables_val = int(total_tables) if total_tables is not None else 0
                succeeded_val = int(succeeded) if succeeded is not None else 0
                failed_val = int(failed) if failed is not None else 0
                skipped_val = int(skipped) if skipped is not None else 0
                total_rows_val = int(total_rows) if total_rows is not None else 0
                priority_val = int(priority) if priority is not None else None
                elapsed_val = float(elapsed_seconds) if elapsed_seconds is not None else 0.0
                rows_per_sec_val = float(rows_per_second) if rows_per_second is not None else 0.0
                run_date_val = run_date or start_ts.date()
                err_summary_val = error_summary[:255] if error_summary else None
    
                with conn.cursor() as cur:
                    query = """
                        INSERT INTO metadata.batch_summary (
                            run_id, source_tag, target_tag, layer, start_ts, end_ts,
                            total_tables, succeeded, failed, skipped, total_rows,
                            status, error_summary, priority, action, run_date,
                            elapsed_seconds, rows_per_second
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s
                        )
                    """
                    cur.execute(query, (
                        self.run_id, source_tag, target_tag, layer, start_ts, end_ts,
                        total_tables_val, succeeded_val, failed_val, skipped_val, total_rows_val,
                        status, err_summary_val, priority_val, action, run_date_val,
                        elapsed_val, rows_per_sec_val
                    ))
                conn.commit()
        except Exception as e:
            print(f"Error writing to batch_summary: {e}")

    def summarize_and_log_layer(self, source_tag: Optional[str], layer: str, target_tag: Optional[str] = None, priority: Optional[int] = None, action: Optional[str] = None):
        """Aggregate batch_log entries for self.run_id and write to batch_summary."""
        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        WITH latest_log AS (
                            SELECT DISTINCT ON (source_table) *
                            FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                            WHERE run_id = %s AND load_type = %s
                            ORDER BY source_table, (status IN ('SUCCESS', 'RECONCILIATION_WARNING')) DESC, end_time DESC
                        )
                        SELECT 
                            COUNT(*),
                            SUM(CASE WHEN status IN ('SUCCESS', 'RECONCILIATION_WARNING') THEN 1 ELSE 0 END),
                            SUM(CASE WHEN status IN ('FAILED', 'RECONCILIATION_FAILED', 'CONNECTION_FAILED') THEN 1 ELSE 0 END),
                            SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END),
                            COALESCE(SUM(target_count), 0),
                            MIN(start_time),
                            MAX(end_time),
                            STRING_AGG(CASE WHEN error_message IS NOT NULL AND error_message != '' THEN source_table || ': ' || error_message ELSE NULL END, '; '),
                            MAX(target_tag)
                        FROM latest_log
                    """
                    cur.execute(query, (self.run_id, layer))
                    row = cur.fetchone()
                    if not row or row[0] == 0:
                        now = datetime.now()
                        self.log_batch_summary(
                            source_tag, layer, now, now, 0, 0, 0, 0, 0, "SKIPPED", None,
                            target_tag=target_tag, priority=priority, action=action,
                            elapsed_seconds=0.0, rows_per_second=0.0, run_date=now.date()
                        )
                        return
    
                    total_tables, succeeded, failed, skipped, total_rows, min_start, max_end, err_summary, db_target_tag = row
                    if failed and failed > 0:
                        status = "FAILED"
                    elif (skipped and skipped > 0) or (succeeded and succeeded < total_tables):
                        status = "PARTIAL"
                    else:
                        status = "COMPLETED"
    
                    start_ts = min_start or datetime.now()
                    end_ts = max_end or datetime.now()
                    elapsed_seconds = (end_ts - start_ts).total_seconds()
                    
                    rows_per_second = 0.0
                    if elapsed_seconds > 0.0 and total_rows is not None:
                        rows_per_second = float(total_rows) / elapsed_seconds

                    self.log_batch_summary(
                        source_tag=source_tag,
                        layer=layer,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        total_tables=total_tables,
                        succeeded=succeeded,
                        failed=failed,
                        skipped=skipped,
                        total_rows=total_rows,
                        status=status,
                        error_summary=err_summary,
                        target_tag=target_tag or db_target_tag,
                        priority=priority,
                        action=action,
                        elapsed_seconds=elapsed_seconds,
                        rows_per_second=rows_per_second,
                        run_date=start_ts.date(),
                    )
        except Exception as e:
            print(f"Error summarizing batch_log for summary: {e}")

    def get_run_summary(self, run_id: Optional[str] = None) -> dict:
        """Derive full pipeline execution summary purely from batch_log on demand."""
        target_run_id = run_id or self.run_id
        summary = {
            "run_id": target_run_id,
            "source_tag": "N/A",
            "target_tag": "N/A",
            "total_tables": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "total_rows": 0,
            "elapsed_seconds": 0.0,
            "slowest_table": None,
            "errors": [],
        }

        try:
            with self._conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    query = f"""
                        WITH latest_log AS (
                            SELECT DISTINCT ON (source_table, load_type) *
                            FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                            WHERE run_id = %s
                            ORDER BY source_table, load_type, (status IN ('SUCCESS', 'RECONCILIATION_WARNING')) DESC, end_time DESC
                        )
                        SELECT 
                            COUNT(*),
                            SUM(CASE WHEN status IN ('SUCCESS', 'RECONCILIATION_WARNING') THEN 1 ELSE 0 END),
                            SUM(CASE WHEN status IN ('FAILED', 'RECONCILIATION_FAILED', 'CONNECTION_FAILED') THEN 1 ELSE 0 END),
                            SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END),
                            COALESCE(SUM(target_count), 0),
                            MIN(start_time),
                            MAX(end_time),
                            MAX(source_tag),
                            MAX(target_tag)
                        FROM latest_log
                    """
                    cur.execute(query, (target_run_id,))
                    row = cur.fetchone()
                    if row and row[0] > 0:
                        total_tables, succeeded, failed, skipped, total_rows, min_start, max_end, src_tag, tgt_tag = row
                        summary["total_tables"] = int(total_tables or 0)
                        summary["succeeded"] = int(succeeded or 0)
                        summary["failed"] = int(failed or 0)
                        summary["skipped"] = int(skipped or 0)
                        summary["total_rows"] = int(total_rows or 0)
                        summary["source_tag"] = src_tag or "N/A"
                        summary["target_tag"] = tgt_tag or "N/A"
                        if min_start and max_end:
                            summary["elapsed_seconds"] = round((max_end - min_start).total_seconds(), 2)

                    # Query slowest table using the same deduplicated latest_log CTE
                    slow_query = f"""
                        WITH latest_log AS (
                            SELECT DISTINCT ON (source_table, load_type) *
                            FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                            WHERE run_id = %s
                            ORDER BY source_table, load_type, (status IN ('SUCCESS', 'RECONCILIATION_WARNING')) DESC, end_time DESC
                        )
                        SELECT source_table, COALESCE(target_count, 0), COALESCE(elapsed_seconds, 0.0)
                        FROM latest_log
                        WHERE status NOT IN ('SKIPPED')
                        ORDER BY elapsed_seconds DESC
                        LIMIT 1
                    """
                    cur.execute(slow_query, (target_run_id,))
                    slow_row = cur.fetchone()
                    if slow_row:
                        stbl, srows, ssec = slow_row
                        ssec = float(ssec or 0.0)
                        srows = int(srows or 0)
                        rate = round(srows / ssec, 1) if ssec > 0 else srows
                        rate_str = f"{round(rate/1000, 1)}K rows/sec" if rate >= 1000 else f"{rate} rows/sec"
                        summary["slowest_table"] = {
                            "table": stbl,
                            "rows": srows,
                            "seconds": round(ssec, 2),
                            "rate_str": rate_str,
                        }

                    # Query errors
                    err_query = f"""
                        SELECT source_table, error_message
                        FROM {MetadataConstants.SCHEMA}.{MetadataConstants.BATCH_LOG_TABLE}
                        WHERE run_id = %s AND error_message IS NOT NULL AND error_message != ''
                    """
                    cur.execute(err_query, (target_run_id,))
                    errs = cur.fetchall()
                    summary["errors"] = [f"{tbl}: {msg}" for tbl, msg in errs]

        except Exception as e:
            print(f"Error deriving run summary from batch_log: {e}")

        return summary

    def print_execution_summary(self, run_id: Optional[str] = None) -> None:
        """Print a formatted ASCII execution summary block to console."""
        s = self.get_run_summary(run_id)
        sep = "=" * 60
        print()
        print(sep)
        print("                 PIPELINE EXECUTION SUMMARY")
        print(sep)
        print(f"Run ID        : {s['run_id']}")
        print(f"Source Tag    : {s['source_tag']}")
        print(f"Target Tag    : {s['target_tag']}")
        print(f"Total Tables  : {s['total_tables']}")
        print(f"Success       : {s['succeeded']}")
        print(f"Failed        : {s['failed']}")
        print(f"Skipped       : {s['skipped']}")
        print(f"Total Rows    : {s['total_rows']:,}")
        print(f"Elapsed Time  : {s['elapsed_seconds']} sec")
        if s["slowest_table"]:
            st = s["slowest_table"]
            print(
                f"Slowest Table : {st['table']} ({st['rows']:,} rows, {st['seconds']} sec, {st['rate_str']})"
            )
        print(sep)
        if s["errors"]:
            print("ERROR SUMMARY:")
            for err in s["errors"][:10]:
                print(f"  - {err}")
            if len(s["errors"]) > 10:
                print(f"  ... and {len(s['errors']) - 10} more error(s)")
            print(sep)
        print()


