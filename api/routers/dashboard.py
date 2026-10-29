# api/routers/dashboard.py
from typing import Optional, List
from fastapi import APIRouter, Depends, Query

from api.models.dashboard import KPIData, RecentActivityItem, DashboardActivityResponse
from api.models.envelope import ResponseEnvelope
from api.models.auth import UserResponse
from api.security.auth import get_current_user, resolve_scoped_project_context
from gui.models.project_context import ProjectContext
from gui.services.metadata_service import get_dashboard_kpis, get_recent_activity

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

@router.get("/kpis", response_model=ResponseEnvelope[KPIData])
def get_kpis(
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    raw_kpis = get_dashboard_kpis(context=ctx)
    kpi_data = KPIData(
        running=int(raw_kpis.get("running") or 0),
        success_today=int(raw_kpis.get("success_today") or 0),
        failed_today=int(raw_kpis.get("failed_today") or 0),
        last_run_end=str(raw_kpis.get("last_run_end")) if raw_kpis.get("last_run_end") else None,
        stalled_count=int(raw_kpis.get("stalled_count") or 0),
        project_scope=ctx.project_id,
    )
    return ResponseEnvelope.ok(kpi_data)

@router.get("/activity", response_model=ResponseEnvelope[DashboardActivityResponse])
def get_activity(
    limit: int = Query(default=15, ge=1, le=100),
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    df = get_recent_activity(limit=limit, context=ctx)
    items: List[RecentActivityItem] = []
    if not df.empty:
        for _, row in df.iterrows():
            items.append(
                RecentActivityItem(
                    run_id=str(row["run_id"]) if row.get("run_id") is not None else None,
                    layer=str(row.get("layer") or ""),
                    source=str(row.get("source") or ""),
                    target_tag=str(row.get("target_tag") or "") if row.get("target_tag") else None,
                    table=str(row.get("table") or ""),
                    target_table=str(row.get("target_table") or "") if row.get("target_table") else None,
                    status=str(row.get("status") or ""),
                    source_count=int(row.get("source_count") or 0),
                    target_count=int(row.get("target_count") or 0),
                    elapsed_sec=float(row.get("elapsed_sec") or 0.0),
                    end_time=str(row.get("end_time")) if row.get("end_time") is not None else None,
                )
            )
    return ResponseEnvelope.ok(DashboardActivityResponse(activities=items))
