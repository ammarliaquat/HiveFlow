# src/orchestrator/lifecycle.py
"""
JOB-01: Authoritative Execution Lifecycle & State Transition Engine.
Sole owner of canonical job state transitions, atomic scope claim releases,
and lease-based crash recovery.
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy import text

from src.utils.json_utils import json_safe_dumps

log = logging.getLogger(__name__)

VALID_JOB_TRANSITIONS: Dict[str, List[str]] = {
    "QUEUED": ["DISPATCHED", "RUNNING", "FAILED", "CANCELLED", "ABANDONED"],
    "DISPATCHED": ["RUNNING", "FAILED", "ABANDONED", "CANCELLED"],
    "RUNNING": ["SUCCESS", "PARTIAL_SUCCESS", "FAILED", "RECONCILIATION_FAILED", "CANCELLED"],
    "SUCCESS": [],                     # Terminal
    "PARTIAL_SUCCESS": [],             # Terminal: all selected tables attempted; some failed
    "FAILED": [],                      # Terminal
    "RECONCILIATION_FAILED": [],        # Terminal
    "CANCELLED": [],                   # Terminal
    "ABANDONED": []                    # Terminal
}

TERMINAL_STATES = frozenset([
    "SUCCESS", "PARTIAL_SUCCESS", "FAILED", "RECONCILIATION_FAILED", "CANCELLED", "ABANDONED"
])


def validate_transition_allowed(from_status: str, to_status: str) -> None:
    """Validates that a status transition follows the authoritative state machine."""
    curr = (from_status or "").upper()
    tgt = (to_status or "").upper()
    allowed = VALID_JOB_TRANSITIONS.get(curr, [])
    if tgt not in allowed:
        raise ValueError(
            f"Invalid state transition: Cannot move job from '{curr}' to '{tgt}'. "
            f"Allowed transitions from '{curr}': {allowed}"
        )


def transition_job(
    engine_or_conn,
    run_id: str,
    from_status: str,
    to_status: str,
    actor_type: str = "SYSTEM",
    actor_id: str = "system",
    details: Optional[Dict[str, Any]] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    pid: Optional[int] = None,
    dispatcher_id: Optional[str] = None,
    dispatch_lease_seconds: Optional[int] = None,
    heartbeat_expired_timeout_sec: Optional[int] = None,
) -> bool:
    """
    Executes a database-validated compare-and-swap state transition on metadata.gui_job_registry.
    Atomically records a job_event audit entry and releases scope claims on terminal states.
    Returns True if the transition was successfully claimed, False if state mismatch occurred.
    """
    validate_transition_allowed(from_status, to_status)
    from_st = from_status.upper()
    to_st = to_status.upper()
    
    details_json = json_safe_dumps(details) if details else None

    # Handle both raw Connection/Engine and context managers
    def _execute_in_tx(conn) -> bool:
        # 1. Build conditional UPDATE query based on target state
        if to_st == "DISPATCHED":
            if dispatch_lease_seconds is None:
                raise ValueError(
                    "dispatch_lease_seconds is required for QUEUED -> DISPATCHED transitions."
                )
            query = text("""
                UPDATE metadata.gui_job_registry
                SET status = 'DISPATCHED',
                    dispatched_at = CURRENT_TIMESTAMP,
                    dispatcher_id = :dispatcher_id,
                    dispatch_lease_expires_at = CURRENT_TIMESTAMP + (:dispatch_lease_sec * INTERVAL '1 second'),
                    pid = COALESCE(:pid, pid)
                WHERE run_id::text = :rid AND status = :from_st
                RETURNING run_id
            """)
            params = {
                "rid": str(run_id),
                "from_st": from_st,
                "dispatcher_id": dispatcher_id or f"dispatcher_{pid or 'main'}",
                "dispatch_lease_sec": int(dispatch_lease_seconds),
                "pid": pid
            }
        elif to_st == "RUNNING":
            query = text("""
                UPDATE metadata.gui_job_registry
                SET status = 'RUNNING',
                    started_at = CURRENT_TIMESTAMP,
                    last_heartbeat = CURRENT_TIMESTAMP,
                    dispatch_lease_expires_at = NULL,
                    pid = COALESCE(:pid, pid)
                WHERE run_id::text = :rid AND status = :from_st
                RETURNING run_id
            """)
            params = {
                "rid": str(run_id),
                "from_st": from_st,
                "pid": pid
            }
        elif to_st in TERMINAL_STATES:
            heartbeat_expiry_predicate = ""
            if heartbeat_expired_timeout_sec is not None:
                if from_st != "RUNNING":
                    raise ValueError(
                        "heartbeat_expired_timeout_sec is valid only for RUNNING transitions."
                    )
                if int(heartbeat_expired_timeout_sec) <= 0:
                    raise ValueError("heartbeat_expired_timeout_sec must be positive.")
                heartbeat_expiry_predicate = """
                  AND COALESCE(last_heartbeat, started_at, submitted_at)
                      < CURRENT_TIMESTAMP - (:heartbeat_timeout_sec * INTERVAL '1 second')
                """
            query = text("""
                UPDATE metadata.gui_job_registry
                SET status = :to_st,
                    completed_at = CURRENT_TIMESTAMP,
                    dispatch_lease_expires_at = NULL,
                    error_code = :error_code,
                    error_message = :error_message
                WHERE run_id::text = :rid AND status = :from_st
            """ + heartbeat_expiry_predicate + """
                RETURNING run_id
            """)
            params = {
                "rid": str(run_id),
                "from_st": from_st,
                "to_st": to_st,
                "error_code": error_code,
                "error_message": error_message,
            }
            if heartbeat_expired_timeout_sec is not None:
                params["heartbeat_timeout_sec"] = int(heartbeat_expired_timeout_sec)
        else:
            query = text("""
                UPDATE metadata.gui_job_registry
                SET status = :to_st
                WHERE run_id::text = :rid AND status = :from_st
                RETURNING run_id
            """)
            params = {
                "rid": str(run_id),
                "from_st": from_st,
                "to_st": to_st
            }

        res = conn.execute(query, params)
        claimed = res.scalar() is not None

        if claimed:
            # 2. Insert audit event in the same transaction
            conn.execute(text("""
                INSERT INTO metadata.job_event (
                    run_id, event_type, from_status, to_status, event_timestamp, actor_type, actor_id, details
                ) VALUES (
                    :rid, :event_type, :from_st, :to_st, CURRENT_TIMESTAMP, :actor_type, :actor_id, :details
                )
            """), {
                "rid": str(run_id),
                "event_type": f"JOB_{to_st}",
                "from_st": from_st,
                "to_st": to_st,
                "actor_type": actor_type,
                "actor_id": actor_id,
                "details": details_json
            })

            # 3. Release active scope claims on terminal transition
            if to_st in TERMINAL_STATES:
                conn.execute(text("""
                    UPDATE metadata.job_scope_claim
                    SET status = 'RELEASED',
                        released_at = CURRENT_TIMESTAMP
                    WHERE run_id::text = :rid
                      AND released_at IS NULL
                      AND status IN ('RESERVED', 'ACTIVE')
                """), {"rid": str(run_id)})

            log.info(f"Lifecycle: Job '{run_id}' transitioned {from_st} -> {to_st}")
            return True
        else:
            log.warning(f"Lifecycle: Transition {from_st} -> {to_st} for job '{run_id}' failed compare-and-swap check.")
            return False

    from sqlalchemy.engine import Engine
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            return _execute_in_tx(conn)
    else:
        return _execute_in_tx(engine_or_conn)


def reap_stale_job_leases(
    engine,
    dispatch_lease_timeout_sec: int = 60,
    worker_heartbeat_timeout_sec: int = 120
) -> Tuple[int, int]:
    """
    Reaps expired DISPATCHED and RUNNING leases in metadata.gui_job_registry.
    - DISPATCHED with expired dispatch lease -> ABANDONED
    - RUNNING with expired worker heartbeat -> FAILED (fences active batch_log attempts first)
    Returns (reaped_dispatched_count, reaped_running_count).
    """
    reaped_dispatched = 0
    reaped_running = 0

    # 1. Reap expired DISPATCHED leases -> ABANDONED
    with engine.connect() as conn:
        stale_dispatched = conn.execute(text("""
            SELECT run_id 
            FROM metadata.gui_job_registry
            WHERE status = 'DISPATCHED'
              AND dispatch_lease_expires_at < CURRENT_TIMESTAMP
        """)).mappings().fetchall()

    for row in stale_dispatched:
        rid = str(row["run_id"])
        success = transition_job(
            engine,
            run_id=rid,
            from_status="DISPATCHED",
            to_status="ABANDONED",
            actor_type="SYSTEM",
            actor_id="lease_reaper",
            error_code="DISPATCH_LEASE_EXPIRED",
            error_message="Dispatcher process died before worker check-in."
        )
        if success:
            reaped_dispatched += 1

    # 2. Reap expired RUNNING worker leases -> FAILED
    with engine.connect() as conn:
        stale_running = conn.execute(text("""
            SELECT run_id, worker_attempt_number 
            FROM metadata.gui_job_registry
            WHERE status = 'RUNNING'
              AND COALESCE(last_heartbeat, started_at, submitted_at)
                  < CURRENT_TIMESTAMP - (:timeout * INTERVAL '1 second')
        """), {"timeout": int(worker_heartbeat_timeout_sec)}).mappings().fetchall()

    for row in stale_running:
        rid = str(row["run_id"])
        attempt = row.get("worker_attempt_number", 1)
        
        # The terminal CAS repeats the expiry predicate: a heartbeat committed
        # after the candidate scan wins the race and prevents fencing.
        with engine.begin() as conn:
            success = transition_job(
                conn,
                run_id=rid,
                from_status="RUNNING",
                to_status="FAILED",
                actor_type="SYSTEM",
                actor_id="heartbeat_reaper",
                error_code="WORKER_HEARTBEAT_EXPIRED",
                error_message="Worker process heartbeat expired (unresponsive or crashed).",
                heartbeat_expired_timeout_sec=worker_heartbeat_timeout_sec,
            )
            if success:
                conn.execute(text("""
                    UPDATE metadata.batch_log
                    SET status = 'FAILED',
                        end_time = CURRENT_TIMESTAMP,
                        notes = COALESCE(notes, '') || ' | [REAPER]: Parent worker heartbeat expired'
                    WHERE run_id::text = :rid AND status = 'RUNNING'
                """), {"rid": rid})
                reaped_running += 1

    return reaped_dispatched, reaped_running
