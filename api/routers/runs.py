# api/routers/runs.py
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, status
from fastapi.responses import JSONResponse

from api.models.run import (
    RunSummaryItem,
    RunDetailResponse,
    RunStageItem,
    TriggerRunRequest,
    TriggerRunResponse,
    CancelRunResponse,
    RunLogsResponse,
)
from api.models.envelope import ResponseEnvelope
from api.models.auth import UserResponse
from api.security.auth import (
    get_current_user,
    resolve_scoped_project_context,
    verify_csrf_token,
)
from gui.models.project_context import ProjectContext
from gui.models.submission import SubmissionRequest, SubmissionResult
from gui.services.metadata_service import get_run_history
from gui.services.execution_service import ExecutionService, AuthorizationError

router = APIRouter(prefix="/runs", tags=["Runs"])

log = logging.getLogger("api.routers.runs")

def check_trigger_permission(user: UserResponse = Depends(get_current_user)) -> UserResponse:
    if user.role in ("ADMIN", "OPERATOR") or "TRIGGER_RUNS" in user.permissions:
        return user
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Forbidden: User '{user.username}' lacks execution permission 'TRIGGER_RUNS'.",
    )


@router.get("", response_model=ResponseEnvelope[List[RunSummaryItem]])
def list_runs(
    status_filter: Optional[str] = Query(None, alias="status"),
    source_tag: Optional[str] = Query(None),
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    # Enforce RBAC on explicit query param
    if current_user.role != "ADMIN" and source_tag:
        if source_tag.strip().lower() not in [p.lower() for p in current_user.allowed_projects]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"RBAC Violation: User '{current_user.username}' is not authorized for project '{source_tag}'.",
            )

    effective_source = ctx.project_id if (ctx and not ctx.is_all_projects) else source_tag
    df = get_run_history(
        status=status_filter,
        source_tag=effective_source,
        limit=limit,
        offset=offset,
        context=ctx,
    )

    items: List[RunSummaryItem] = []
    if not df.empty:
        # Defense-in-depth: non-admin can only see authorized project runs
        if current_user.role != "ADMIN":
            allowed_lower = [p.lower() for p in current_user.allowed_projects]
            df = df[df["source_tag"].astype(str).str.lower().isin(allowed_lower)]

        for _, row in df.iterrows():
            items.append(
                RunSummaryItem(
                    run_id=str(row.get("run_id") or ""),
                    action=str(row.get("layer") or "run_pipeline"),
                    source_tag=str(row.get("source_tag") or ""),
                    target_tag=str(row.get("target_tag") or "") if row.get("target_tag") else None,
                    source_table=str(row.get("source_table") or "") if row.get("source_table") else None,
                    target_table=str(row.get("target_table") or "") if row.get("target_table") else None,
                    priority=int(row["priority"]) if "priority" in row and row["priority"] is not None and not str(row["priority"]).lower().startswith("nan") else None,
                    status=str(row.get("status") or "UNKNOWN"),
                    started_at=str(row.get("start_time")) if row.get("start_time") is not None else None,
                    completed_at=str(row.get("end_time")) if row.get("end_time") is not None else None,
                    elapsed_seconds=float(row.get("elapsed_sec") or 0.0),
                    source_count=int(row.get("source_count") or 0),
                    target_count=int(row.get("target_count") or 0),
                    rows_per_minute=round(float(row.get("rows_per_second") or 0.0) * 60.0, 1) if row.get("rows_per_second") else 0.0,
                    error_message=str(row.get("error_message") or "") if row.get("error_message") else None,
                )
            )
    return ResponseEnvelope.ok(items)

@router.get("/{run_id}", response_model=ResponseEnvelope[RunDetailResponse])
def get_run_detail_endpoint(
    run_id: str,
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    try:
        run_model = ExecutionService.get_run_detail(run_id, context=ctx)
    except AuthorizationError as auth_err:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"RBAC Violation: {auth_err}",
        )
    except Exception as exc:
        log.error(f"Error querying run detail '{run_id}': {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve run details due to an internal server or database error.",
        )

    if not run_model or run_model.status == "UNKNOWN":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found.",
        )

    # Server-side verification: ensure non-admin user cannot access unauthorized project's run
    if current_user.role != "ADMIN":
        run_proj = (run_model.project_id or "").strip().lower()
        if run_proj not in [p.lower() for p in current_user.allowed_projects]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"RBAC Violation: User '{current_user.username}' is not authorized to view runs for project '{run_model.project_id}'.",
            )

    # Compute operational telemetry
    rps = float(run_model.throughput_rows_sec or 0.0)
    rolling_rps = float(run_model.rolling_rows_per_second or rps)
    rows_per_minute = round(rps * 60.0, 1)
    rolling_rows_per_minute = round(rolling_rps * 60.0, 1)

    chunk_pct = None
    if run_model.total_chunks and run_model.total_chunks > 0:
        completed = run_model.chunks_completed if run_model.chunks_completed is not None else (run_model.current_chunk or 0)
        chunk_pct = round(min(100.0, max(0.0, (completed / run_model.total_chunks) * 100.0)), 1)

    hb_age = None
    is_stalled = bool(run_model.is_stalled)
    if run_model.last_heartbeat_at:
        now_dt = datetime.now(run_model.last_heartbeat_at.tzinfo) if run_model.last_heartbeat_at.tzinfo else datetime.now()
        hb_age = round(max(0.0, (now_dt - run_model.last_heartbeat_at).total_seconds()), 1)
        if run_model.status in ("RUNNING", "DISPATCHED") and hb_age > 45.0:
            is_stalled = True

    eta_sec = None
    if run_model.status in ("RUNNING", "DISPATCHED") and rolling_rps > 0:
        total_target = run_model.extracted_count or run_model.rows_read
        if total_target > run_model.rows_written:
            eta_sec = round((total_target - run_model.rows_written) / rolling_rps, 1)

    # Convert stage models with throughput in rows/min
    stage_items = [
        RunStageItem(
            stage_name=s.stage_name,
            status=s.status,
            rows_processed=s.rows_processed or 0,
            throughput_rows_sec=float(s.throughput_rows_sec) if s.throughput_rows_sec is not None else 0.0,
            throughput_rows_min=round(float(s.throughput_rows_sec or 0.0) * 60.0, 1) if s.throughput_rows_sec is not None else 0.0,
        )
        for s in run_model.stages
    ]

    detail = RunDetailResponse(
        run_id=run_model.run_id,
        parent_run_id=run_model.parent_run_id,
        retry_attempt=run_model.retry_attempt,
        project_id=run_model.project_id,
        environment=run_model.environment,
        pipeline_id=run_model.pipeline_id,
        dataset_id=run_model.dataset_id,
        status=run_model.status,
        started_at=run_model.started_at.isoformat() if run_model.started_at else None,
        ended_at=run_model.ended_at.isoformat() if run_model.ended_at else None,
        last_heartbeat_at=run_model.last_heartbeat_at.isoformat() if run_model.last_heartbeat_at else None,
        elapsed_seconds=run_model.elapsed_seconds,
        rows_read=run_model.rows_read,
        extracted_count=run_model.extracted_count,
        rows_written=run_model.rows_written,
        throughput_rows_sec=run_model.throughput_rows_sec,
        rolling_rows_per_second=run_model.rolling_rows_per_second,
        rows_per_minute=rows_per_minute,
        rolling_rows_per_minute=rolling_rows_per_minute,
        current_chunk=run_model.current_chunk,
        total_chunks=run_model.total_chunks,
        chunks_completed=run_model.chunks_completed,
        chunk_progress_pct=chunk_pct,
        is_stalled=is_stalled,
        last_heartbeat_age_seconds=hb_age,
        estimated_remaining_seconds=eta_sec,
        reconciliation_status=run_model.reconciliation_status,
        stages=stage_items,
        error_message=run_model.error_message,
        parameters=None,
    )
    return ResponseEnvelope.ok(detail)


@router.post("/trigger", response_model=ResponseEnvelope[TriggerRunResponse], status_code=status.HTTP_201_CREATED)
def trigger_run_endpoint(
    req: TriggerRunRequest,
    current_user: UserResponse = Depends(check_trigger_permission),
    _csrf: None = Depends(verify_csrf_token),
):
    """
    JOB-01 / Milestone 2: Authoritative Execution Submission & Concurrency Governance.
    - Validates caller permissions & project RBAC scoping.
    - Submits execution request to authoritative ExecutionService.
    - Resolves scope claims, rejects concurrent overlaps with 409 Conflict.
    - Returns 201 Created with run_id on success.
    """
    # Enforce RBAC on target source project
    if current_user.role != "ADMIN":
        allowed = [p.lower() for p in current_user.allowed_projects]
        if req.source_tag.strip().lower() not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"RBAC Violation: User '{current_user.username}' is not authorized to trigger runs for project '{req.source_tag}'.",
            )

    req_id = req.request_id or str(uuid.uuid4())
    sub_req = SubmissionRequest(
        request_id=req_id,
        action="run_governed",
        source_tag=req.source_tag,
        source_table=req.source_table,
        source_table_ids=tuple(req.source_table_ids) if req.source_table_ids else (),
        target_tag=req.target_tag,
        priority=req.priority,
        actor_type="USER",
        actor_id=current_user.username,
        execution_scope=req.execution_scope or "FULL",
    )

    # Bind execution context explicitly to the target source project being executed
    # to prevent stale UI navigation filter headers (e.g. from Topbar) from causing
    # false-positive RBAC context mismatch rejections.
    exec_ctx = ProjectContext(project_id=req.source_tag.strip().lower(), environment="PROD")

    try:
        result = ExecutionService.submit_run(sub_req, context=exec_ctx)
    except AuthorizationError as auth_err:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(auth_err))
    except Exception as exc:
        log.error(f"ExecutionService failed during submit_run: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit pipeline run: {str(exc)}",
        )

    if isinstance(result, SubmissionResult):
        if result.accepted:
            return ResponseEnvelope.ok(
                TriggerRunResponse(
                    accepted=True,
                    request_id=result.request_id,
                    run_id=result.run_id,
                    status=result.status,
                    submitted_at=result.submitted_at.isoformat() if result.submitted_at else None,
                )
            )
        else:
            envelope = ResponseEnvelope.fail(
                code=result.rejection_code or "RUN_REJECTED",
                message=result.rejection_reason or "Job submission rejected by concurrency governance.",
            )
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={
                    **envelope.model_dump(),
                    "data": {
                        "accepted": False,
                        "request_id": result.request_id,
                        "rejection_code": result.rejection_code,
                        "rejection_reason": result.rejection_reason,
                        "existing_active_run_id": result.existing_active_run_id,
                    },
                },
            )

    return ResponseEnvelope.ok(
        TriggerRunResponse(
            accepted=True,
            request_id=req_id,
            run_id=str(result),
            status="QUEUED",
        )
    )


@router.post("/{run_id}/cancel", response_model=ResponseEnvelope[CancelRunResponse])
def cancel_run_endpoint(
    run_id: str,
    current_user: UserResponse = Depends(check_trigger_permission),
    _csrf: None = Depends(verify_csrf_token),
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
):
    """
    Milestone 2: Governed Run Cancellation.
    - Validates user execution permissions and project ownership.
    - Fails if run is already in terminal state.
    - Dispatches cancellation and releases scope claims.
    """
    try:
        run_model = ExecutionService.get_run_detail(run_id, context=ctx)
    except AuthorizationError as auth_err:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(auth_err))
    except Exception as exc:
        log.warning(f"Error fetching run '{run_id}' before cancel: {exc}")
        run_model = None

    if not run_model:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Run '{run_id}' not found.")

    if run_model.status in ("SUCCESS", "FAILED", "CANCELLED", "RECONCILIATION_FAILED"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Run '{run_id}' is already in terminal state '{run_model.status}'. Cannot cancel.",
        )

    try:
        success = ExecutionService.cancel_run(run_id, context=ctx)
        return ResponseEnvelope.ok(
            CancelRunResponse(
                success=bool(success),
                run_id=run_id,
                message="Run cancellation requested successfully." if success else "Failed to cancel run.",
            )
        )
    except AuthorizationError as auth_err:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(auth_err))
    except Exception as exc:
        log.error(f"Error cancelling run '{run_id}': {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel run '{run_id}': {str(exc)}",
        )


@router.get("/{run_id}/logs", response_model=ResponseEnvelope[RunLogsResponse])
def get_run_logs_endpoint(
    run_id: str,
    max_lines: int = Query(default=200, ge=1, le=1000),
    current_user: UserResponse = Depends(get_current_user),
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
):
    """
    Milestone 2: Live Run Execution Log Streaming.
    - Validates project context RBAC on target run.
    - Streams log content from authoritative JobManager output.
    """
    try:
        ExecutionService.get_run_detail(run_id, context=ctx)
        logs = ExecutionService.stream_run_logs(run_id, max_lines=max_lines, reverse=False, context=ctx)
        lines = logs.splitlines() if logs else []
        return ResponseEnvelope.ok(
            RunLogsResponse(
                run_id=run_id,
                logs=logs or "",
                lines_returned=len(lines),
            )
        )
    except AuthorizationError as auth_err:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(auth_err))
    except Exception as exc:
        log.error(f"Error reading logs for run '{run_id}': {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not load logs for run '{run_id}': {str(exc)}",
        )

