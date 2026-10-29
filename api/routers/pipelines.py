# api/routers/pipelines.py
from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, status

from api.models.pipeline import PipelineItem, ColumnMappingItem, PipelineDetailResponse
from api.models.envelope import ResponseEnvelope
from api.models.auth import UserResponse
from api.security.auth import get_current_user, resolve_scoped_project_context
from gui.models.project_context import ProjectContext
from gui.services.metadata_service import (
    get_pipeline_tables,
    get_table_columns,
    get_table_detail,
)

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])

@router.get("", response_model=ResponseEnvelope[List[PipelineItem]])
def list_pipelines(
    source_tag: Optional[str] = Query(None),
    target_tag: Optional[str] = Query(None),
    priority: Optional[int] = Query(None),
    is_active: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
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

    # Enforce ProjectContext scope
    effective_source = ctx.project_id if (ctx and not ctx.is_all_projects) else source_tag
    df = get_pipeline_tables(
        source_tag=effective_source,
        target_tag=target_tag,
        priority=priority,
        is_active=is_active,
        search=search,
        context=ctx,
    )

    items: List[PipelineItem] = []
    if not df.empty:
        # Defense-in-depth: non-admin can only see authorized project tables
        if current_user.role != "ADMIN":
            allowed_lower = [p.lower() for p in current_user.allowed_projects]
            df = df[df["source_tag"].astype(str).str.lower().isin(allowed_lower)]

        for _, row in df.iterrows():
            items.append(
                PipelineItem(
                    table_id=int(row["table_id"]),
                    source_tag=str(row["source_tag"]),
                    target_tag=str(row["target_tag"]) if row.get("target_tag") else None,
                    priority=int(row.get("priority") or 1),
                    source_table=str(row["source_table"]),
                    target_table=str(row.get("target_table")) if row.get("target_table") else None,
                    is_active=str(row.get("is_active") or "n").strip().lower() in ("y", "true", "1"),
                    is_cdc=str(row.get("is_cdc") or "n").strip().lower() in ("y", "true", "1"),
                    cdc_strategy=str(row.get("cdc_strategy")) if row.get("cdc_strategy") else None,
                    columns_unique_key=str(row.get("columns_unique_key")) if row.get("columns_unique_key") else None,
                    execution_authority_mode=str(row.get("execution_authority_mode")) if row.get("execution_authority_mode") else None,
                    last_success_date=str(row.get("last_success_date")) if row.get("last_success_date") else None,
                    last_success_timestamp=str(row.get("last_success_timestamp")) if row.get("last_success_timestamp") else None,
                    landing_mode=str(row.get("landing_mode") or "METADATA"),
                )
            )
    return ResponseEnvelope.ok(items)

@router.get("/{source_tag}/{table_name}", response_model=ResponseEnvelope[PipelineDetailResponse])
def get_pipeline(
    source_tag: str,
    table_name: str,
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    # RBAC project scope check
    if current_user.role != "ADMIN" and source_tag.lower() not in [p.lower() for p in current_user.allowed_projects]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"RBAC Violation: Not authorized for project '{source_tag}'.",
        )

    # 1. Fetch table metadata
    df_tables = get_pipeline_tables(source_tag=source_tag, search=table_name, context=ctx)
    if df_tables.empty or "source_table" not in df_tables.columns:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline table '{source_tag}.{table_name}' not found.",
        )

    matched = df_tables[df_tables["source_table"].str.lower() == table_name.lower()]
    if matched.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline table '{source_tag}.{table_name}' not found.",
        )

    row = matched.iloc[0]
    pipeline_item = PipelineItem(
        table_id=int(row["table_id"]),
        source_tag=str(row["source_tag"]),
        target_tag=str(row.get("target_tag")) if row.get("target_tag") else None,
        priority=int(row.get("priority") or 1),
        source_table=str(row["source_table"]),
        target_table=str(row.get("target_table")) if row.get("target_table") else None,
        is_active=str(row.get("is_active") or "n").strip().lower() in ("y", "true", "1"),
        is_cdc=str(row.get("is_cdc") or "n").strip().lower() in ("y", "true", "1"),
        cdc_strategy=str(row.get("cdc_strategy")) if row.get("cdc_strategy") else None,
        columns_unique_key=str(row.get("columns_unique_key")) if row.get("columns_unique_key") else None,
        execution_authority_mode=str(row.get("execution_authority_mode")) if row.get("execution_authority_mode") else None,
        last_success_date=str(row.get("last_success_date")) if row.get("last_success_date") else None,
        last_success_timestamp=str(row.get("last_success_timestamp")) if row.get("last_success_timestamp") else None,
        landing_mode=str(row.get("landing_mode") or "METADATA"),
    )

    # 2. Fetch column mappings
    df_cols = get_table_columns(source_tag=source_tag, source_table=table_name)
    column_items: List[ColumnMappingItem] = []
    if not df_cols.empty:
        for _, c_row in df_cols.iterrows():
            column_items.append(
                ColumnMappingItem(
                    column_id=int(c_row["column_id"]) if "column_id" in c_row and c_row["column_id"] is not None else None,
                    source_column=str(c_row.get("column_name") or ""),
                    target_column=str(c_row.get("column_name") or ""),
                    canonical_type=str(c_row.get("column_type") or "STRING"),
                    is_key=bool(c_row.get("is_primary_key", False)),
                    is_cdc_column=False,
                )
            )

    return ResponseEnvelope.ok(
        PipelineDetailResponse(pipeline=pipeline_item, columns=column_items)
    )
