# src/orchestrator/job_manager.py
"""
Decoupled Background Job Manager.
Executes app_etl.py sub-processes asynchronously, logging PID, UUID, and execution status.
Integrates with JOB-01 authoritative lifecycle state machine.
"""
import os
import sys
import uuid
import time
import subprocess
import logging
from typing import Dict, Optional, List
from src.contracts.runtime_settings import RuntimeSettings
from src.exceptions import ExecutionOwnershipRequiredError

log = logging.getLogger(__name__)

# In-memory registry of running background processes on this host
_JOB_REGISTRY: Dict[str, dict] = {}


class JobManager:
    """Manages background PySpark/ETL process execution and log streaming."""

    @staticmethod
    def build_governed_worker_command(
        run_id: str,
        base_dir: Optional[str] = None,
        python_executable: Optional[str] = None,
    ) -> List[str]:
        """Builds the governed worker command without touching the database or filesystem."""
        resolved_base_dir = base_dir or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        return [
            python_executable or sys.executable,
            os.path.join(resolved_base_dir, "jobs", "app_etl.py"),
            "--action", "run_governed",
            "--run_id", run_id,
        ]

    @staticmethod
    def submit_job(
        action: str,
        source_tag: str,
        priority: Optional[int] = None,
        table: Optional[str] = None,
        extra_flags: Optional[List[str]] = None,
        preallocated_run_id: Optional[str] = None,
        runtime_settings: Optional[RuntimeSettings] = None,
        target_tag: Optional[str] = None,
    ) -> str:
        """
        Transitions job QUEUED -> DISPATCHED and launches app_etl.py in background.
        If subprocess fails to spawn, transitions DISPATCHED -> FAILED.
        Returns the run_id string immediately.
        """
        run_id = preallocated_run_id or str(uuid.uuid4())
        if runtime_settings is None:
            raise ValueError("JobManager.submit_job requires explicit RuntimeSettings.")

        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        logs_dir = os.path.join(base_dir, ".brain", "logs")
        os.makedirs(logs_dir, exist_ok=True)

        log_file_path = os.path.join(logs_dir, f"job_{run_id}.log")

        # 1. Transition state QUEUED -> DISPATCHED in database
        try:
            from gui.services.database import get_engine
            from src.orchestrator.lifecycle import transition_job
            engine = get_engine()
            claimed = transition_job(
                engine,
                run_id=run_id,
                from_status="QUEUED",
                to_status="DISPATCHED",
                dispatcher_id=f"dispatcher_{os.getpid()}",
                dispatch_lease_seconds=runtime_settings.dispatch_lease_timeout_sec,
                actor_type="SYSTEM",
                actor_id="job_manager"
            )
        except Exception as trans_err:
            log.warning(f"JobManager: Could not transition {run_id} to DISPATCHED: {trans_err}")

        cmd = [
            sys.executable,
            os.path.join(base_dir, "jobs", "app_etl.py"),
            "--action", action,
            "--source_tag", source_tag,
            "--run_id", run_id,
        ]

        if priority is not None:
            cmd.extend(["--priority", str(priority)])
        if table:
            cmd.extend(["--table", table])
        has_target_tag_flag = any(
            str(flag) == "--target_tag" or str(flag).startswith("--target_tag=")
            for flag in (extra_flags or [])
        )
        if target_tag and not has_target_tag_flag:
            cmd.extend(["--target_tag", target_tag])
        if extra_flags:
            cmd.extend(extra_flags)

        log.info(f"Submitting background job run_id={run_id}: {' '.join(cmd)}")

        log_file = open(log_file_path, "w", encoding="utf-8")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=base_dir,
                text=True
            )
        except Exception as spawn_err:
            log.error(f"JobManager: Failed spawning worker process for run_id={run_id}: {spawn_err}")
            try:
                from gui.services.database import get_engine
                from src.orchestrator.lifecycle import transition_job
                engine = get_engine()
                transition_job(
                    engine,
                    run_id=run_id,
                    from_status="DISPATCHED",
                    to_status="FAILED",
                    error_code="WORKER_SPAWN_FAILED",
                    error_message=str(spawn_err),
                    actor_type="SYSTEM",
                    actor_id="job_manager"
                )
            except Exception:
                pass
            raise

        _JOB_REGISTRY[run_id] = {
            "run_id": run_id,
            "action": action,
            "source_tag": source_tag,
            "priority": priority,
            "table": table,
            "pid": proc.pid,
            "proc": proc,
            "log_file_path": log_file_path,
            "log_file_obj": log_file,
            "start_time": time.time(),
            "status": "DISPATCHED"
        }

        # Update PID in gui_job_registry
        try:
            from gui.services.database import get_engine
            from sqlalchemy import text
            engine = get_engine()
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE metadata.gui_job_registry
                    SET pid = :pid
                    WHERE run_id::text = :rid
                """), {"pid": proc.pid, "rid": run_id})
        except Exception as pid_err:
            log.warning(f"JobManager: Failed to update PID in registry: {pid_err}")

        return run_id


    @staticmethod
    def submit_governed_job(run_id: str, runtime_settings: RuntimeSettings) -> str:
        """Transitions governed job QUEUED -> DISPATCHED and launches app_etl.py."""
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        logs_dir = os.path.join(base_dir, ".brain", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_file_path = os.path.join(logs_dir, f"job_{run_id}.log")

        try:
            from gui.services.database import get_engine
            from src.orchestrator.lifecycle import transition_job
            engine = get_engine()
            claimed = transition_job(
                engine,
                run_id=run_id,
                from_status="QUEUED",
                to_status="DISPATCHED",
                dispatcher_id=f"dispatcher_{os.getpid()}",
                dispatch_lease_seconds=runtime_settings.dispatch_lease_timeout_sec,
                actor_type="SYSTEM",
                actor_id="job_manager"
            )
            if not claimed:
                raise ExecutionOwnershipRequiredError(
                    f"Cannot dispatch governed run {run_id}; QUEUED ownership was not acquired."
                )
        except Exception as trans_err:
            log.error(f"JobManager: Could not transition governed run {run_id} to DISPATCHED: {trans_err}")
            raise

        cmd = JobManager.build_governed_worker_command(run_id, base_dir=base_dir)

        log.info(f"Submitting governed background job run_id={run_id}: {' '.join(cmd)}")
        log_file = open(log_file_path, "w", encoding="utf-8")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=base_dir,
                text=True
            )
        except Exception as spawn_err:
            log.error(f"JobManager: Failed spawning worker process for run_id={run_id}: {spawn_err}")
            try:
                from gui.services.database import get_engine
                from src.orchestrator.lifecycle import transition_job
                engine = get_engine()
                transition_job(
                    engine,
                    run_id=run_id,
                    from_status="DISPATCHED",
                    to_status="FAILED",
                    error_code="WORKER_SPAWN_FAILED",
                    error_message=str(spawn_err),
                    actor_type="SYSTEM",
                    actor_id="job_manager"
                )
            except Exception:
                pass
            try:
                log_file.close()
            except Exception:
                pass
            raise

        _JOB_REGISTRY[run_id] = {
            "run_id": run_id,
            "action": "run_governed",
            "source_tag": "governed",
            "priority": None,
            "table": None,
            "pid": proc.pid,
            "proc": proc,
            "log_file_path": log_file_path,
            "log_file_obj": log_file,
            "start_time": time.time(),
            "status": "DISPATCHED"
        }

        try:
            from gui.services.database import get_engine
            from sqlalchemy import text
            engine = get_engine()
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE metadata.gui_job_registry
                    SET pid = :pid
                    WHERE run_id::text = :rid
                """), {"pid": proc.pid, "rid": run_id})
        except Exception as pid_err:
            log.warning(f"JobManager: Failed to update PID in registry: {pid_err}")

        return run_id

    @staticmethod
    def get_job_status(run_id: str) -> dict:
        """Queries status of a submitted job."""
        if run_id not in _JOB_REGISTRY:
            return {"run_id": run_id, "status": "UNKNOWN", "exists": False}

        job = _JOB_REGISTRY[run_id]
        proc = job.get("proc")

        if proc:
            poll = proc.poll()
            if poll is None:
                job["status"] = job.get("status", "RUNNING")
            else:
                action = job.get("action")
                if action == "run_governed":
                    # app_etl is the sole finalizer; only close the log file
                    if job.get("log_file_obj"):
                        job["log_file_obj"].close()
                        job["log_file_obj"] = None
                    # Do not call _sync_gui_job_status
                elif poll == 0:
                    old_st = job.get("status")
                    job["status"] = "SUCCESS"
                    if old_st not in ("SUCCESS", "FAILED"):
                        JobManager._sync_gui_job_status(run_id, "SUCCESS")
                    if job.get("log_file_obj"):
                        job["log_file_obj"].close()
                        job["log_file_obj"] = None
                else:
                    old_st = job.get("status")
                    job["status"] = f"FAILED (code {poll})"
                    if "FAILED" not in str(old_st):
                        JobManager._sync_gui_job_status(run_id, "FAILED", error_msg=f"Process exited with code {poll}")
                    if job.get("log_file_obj"):
                        job["log_file_obj"].close()
                        job["log_file_obj"] = None

        return {
            "run_id": run_id,
            "status": job["status"],
            "pid": job.get("pid"),
            "action": job.get("action"),
            "source_tag": job.get("source_tag"),
            "elapsed_seconds": round(time.time() - job["start_time"], 2),
            "log_file_path": job.get("log_file_path"),
            "exists": True
        }

    @staticmethod
    def _sync_gui_job_status(run_id: str, status: str, error_msg: Optional[str] = None):
        """Helper to sync completion status into metadata.gui_job_registry via transition_job."""
        try:
            from gui.services.database import get_engine
            from src.orchestrator.lifecycle import transition_job
            engine = get_engine()
            # Try transition from RUNNING or DISPATCHED
            transition_job(
                engine,
                run_id=run_id,
                from_status="RUNNING",
                to_status=status,
                error_message=error_msg,
                actor_type="SYSTEM",
                actor_id="process_poller"
            ) or transition_job(
                engine,
                run_id=run_id,
                from_status="DISPATCHED",
                to_status=status,
                error_message=error_msg,
                actor_type="SYSTEM",
                actor_id="process_poller"
            )
        except Exception as sync_err:
            log.warning(f"JobManager: Failed syncing status to gui_job_registry: {sync_err}")

    @staticmethod
    def get_all_active_jobs() -> List[dict]:
        """Returns list of active job dicts currently running or dispatched on this host."""
        active = []
        for rid, j in list(_JOB_REGISTRY.items()):
            st = JobManager.get_job_status(rid)
            if st.get("status") in ("RUNNING", "DISPATCHED"):
                active.append(st)
        return active

    @staticmethod
    def read_job_logs(run_id: str, max_lines: int = 200, reverse: bool = True, **kwargs) -> str:
        """
        Reads recent log lines for a running or completed job.
        Default reverse=True ensures newest log lines appear at the top (descending top-down order).
        """
        log_path = None
        if run_id in _JOB_REGISTRY:
            log_path = _JOB_REGISTRY[run_id].get("log_file_path")

        # Fallback to disk .brain/logs directory if process exited or control plane restarted
        if not log_path or not os.path.exists(log_path):
            alt_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".brain", "logs", f"job_{run_id}.log")
            if os.path.exists(alt_path):
                log_path = alt_path

        if not log_path or not os.path.exists(log_path):
            db_logs = JobManager._read_db_batch_logs(run_id, max_lines=max_lines, reverse=reverse)
            if db_logs:
                return db_logs
            return f"No log found for run_id: {run_id}"

        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                recent_lines = lines[-max_lines:]
                if reverse:
                    recent_lines = list(reversed(recent_lines))
                return "".join(recent_lines)
        except Exception as e:
            db_logs = JobManager._read_db_batch_logs(run_id, max_lines=max_lines, reverse=reverse)
            if db_logs:
                return db_logs
            return f"Error reading log file: {e}"

    @staticmethod
    def _read_db_batch_logs(run_id: str, max_lines: int = 200, reverse: bool = True) -> Optional[str]:
        """Queries metadata.batch_log to construct structured log lines if disk log file was rotated or deleted."""
        try:
            from gui.services.database import get_engine
            from sqlalchemy import text
            engine = get_engine()
            with engine.connect() as conn:
                query = text("""
                    SELECT source_tag, target_table, current_phase, status, start_time, end_time, elapsed_seconds, error_message
                    FROM metadata.batch_log
                    WHERE run_id::text = :rid
                    ORDER BY id ASC
                """)
                rows = conn.execute(query, {"rid": str(run_id).strip()}).fetchall()
                if not rows:
                    return None

                log_lines = []
                log_lines.append(f"=== Database Execution Audit Telemetry (Run ID: {run_id}) ===")
                for r in rows:
                    st_str = r.start_time.strftime("%Y-%m-%d %H:%M:%S") if r.start_time else "N/A"
                    el_str = f"{r.elapsed_seconds:.2f}s" if r.elapsed_seconds is not None else "N/A"
                    line = f"[{st_str}] Source: {r.source_tag} | Table: {r.target_table or 'N/A'} | Phase: {r.current_phase} | Status: {r.status} | Duration: {el_str}"
                    if r.error_message:
                        line += f"\n   [ERROR]: {r.error_message}"
                    log_lines.append(line)

                recent_lines = log_lines[-max_lines:]
                if reverse:
                    recent_lines = list(reversed(recent_lines))
                return "\n".join(recent_lines) + "\n"
        except Exception:
            return None

    @staticmethod
    def cancel_job(run_id: str) -> bool:
        """Terminates a running background job."""
        if run_id not in _JOB_REGISTRY:
            return False

        job = _JOB_REGISTRY[run_id]
        proc = job.get("proc")
        if proc and proc.poll() is None:
            proc.terminate()
            job["status"] = "CANCELLED"
            if job.get("log_file_obj"):
                job["log_file_obj"].close()
                job["log_file_obj"] = None
            return True
        return False
