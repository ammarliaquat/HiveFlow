# api/routers/discovery.py
import logging
import uuid
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, Query, HTTPException, status
import psycopg2.extras

from api.models.discovery import (
    CrawlRequest,
    CrawlResponse,
    CrawlStatusResponse,
    CatalogTableItem,
    CatalogSummaryResponse,
    CatalogColumnItem,
    PromoteRequest,
    PromoteResponse,
    PolicyRefreshSummary,
    DriftCheckItem,
    DriftCheckResponse,
    Tier2SubscriptionItem,
)
from api.models.envelope import ResponseEnvelope
from api.models.auth import UserResponse
from api.security.auth import (
    get_current_user,
    resolve_scoped_project_context,
    verify_csrf_token,
    get_connection_manager,
)

from gui.models.project_context import ProjectContext
from gui.models.submission import SubmissionRequest, SubmissionResult
from gui.services.metadata_service import (
    get_source_tags,
    get_target_tags,
    get_catalog_tables,
    get_catalog_summary,
    get_catalog_columns,
    get_pipeline_tables,
    clear_gui_cache,
)
from gui.services.execution_service import ExecutionService, AuthorizationError
from gui.services.promotion_service import PromotionService
from src.services.connection_service import ConnectionService
from src.connections.connection_manager import ConnectionManager
from src.models.principal import AuthenticatedPrincipal

router = APIRouter(prefix="/discovery", tags=["Discovery & Promotion"])
log = logging.getLogger("api.routers.discovery")


def require_admin_or_operator(user: UserResponse = Depends(get_current_user)) -> UserResponse:
    if user.role in ("ADMIN", "OPERATOR") or "TRIGGER_RUNS" in user.permissions:
        return user
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Forbidden: User '{user.username}' lacks crawler/discovery permission.",
    )


def require_admin_role(user: UserResponse = Depends(get_current_user)) -> UserResponse:
    if user.role.upper() == "ADMIN":
        return user
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Forbidden: Principal '{user.username}' requires ADMIN role for governed dataset promotion.",
    )


# ── 1. Crawler Execution & Log Streaming ──────────────────────────────────────

@router.post("/crawl", response_model=ResponseEnvelope[CrawlResponse])
def run_schema_crawler(
    payload: CrawlRequest,
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(require_admin_or_operator),
    _csrf: None = Depends(verify_csrf_token),
):
    source_tag = payload.source_tag.strip()
    if not source_tag:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="source_tag is required.")

    # Validate RBAC tenant boundary
    if current_user.role != "ADMIN":
        allowed = [p.lower() for p in current_user.allowed_projects]
        if source_tag.lower() not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"RBAC Violation: User '{current_user.username}' is not authorized for source '{source_tag}'.",
            )

    try:
        req = SubmissionRequest(
            request_id=str(uuid.uuid4()),
            action="sync",
            source_tag=source_tag,
            actor_type="USER",
            actor_id=current_user.username,
        )
        result = ExecutionService.submit_run(req, context=ctx)
        run_id = result.run_id if isinstance(result, SubmissionResult) else str(result)
        return ResponseEnvelope.ok(
            CrawlResponse(
                run_id=run_id,
                status="DISPATCHED",
                message=f"Dispatched discovery crawler for source system '{source_tag}'.",
            )
        )
    except AuthorizationError as auth_err:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(auth_err))
    except Exception as exc:
        log.error(f"Error submitting crawler run for '{source_tag}': {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit discovery crawler: {exc}",
        )


@router.get("/crawl/{run_id}/status", response_model=ResponseEnvelope[CrawlStatusResponse])
def get_crawl_status(
    run_id: str,
    current_user: UserResponse = Depends(get_current_user),
):
    try:
        status_info = ExecutionService.get_run_status(run_id)
        if not status_info:
            return ResponseEnvelope.ok(
                CrawlStatusResponse(
                    run_id=run_id,
                    status="UNKNOWN",
                    elapsed_seconds=0.0,
                    error_message=None,
                )
            )
        return ResponseEnvelope.ok(
            CrawlStatusResponse(
                run_id=run_id,
                status=str(status_info.get("status") or "UNKNOWN").upper(),
                elapsed_seconds=float(status_info.get("elapsed_seconds") or 0.0),
                error_message=status_info.get("error_message"),
            )
        )
    except Exception as exc:
        log.error(f"Error fetching crawler status for '{run_id}': {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check crawler status: {exc}",
        )


@router.get("/crawl/{run_id}/logs", response_model=ResponseEnvelope[Dict[str, Any]])
def get_crawl_logs(
    run_id: str,
    current_user: UserResponse = Depends(get_current_user),
):
    try:
        logs = ExecutionService.stream_run_logs(run_id)
        return ResponseEnvelope.ok({"run_id": run_id, "logs": logs or ""})
    except Exception as exc:
        log.error(f"Error reading crawler logs for '{run_id}': {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read crawler logs: {exc}",
        )


# ── 2. Tier-1 Master Catalog & Column Schema Inspector ─────────────────────────

@router.get("/catalog", response_model=ResponseEnvelope[CatalogSummaryResponse])
def get_catalog(
    source_tag: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    # RBAC filtering
    if current_user.role != "ADMIN" and source_tag:
        if source_tag.strip().lower() not in [p.lower() for p in current_user.allowed_projects]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"RBAC Violation: User '{current_user.username}' is not authorized for '{source_tag}'.",
            )

    effective_source = ctx.project_id if (ctx and not ctx.is_all_projects) else source_tag
    df = get_catalog_summary(source_tag=effective_source, context=ctx)

    if df.empty:
        return ResponseEnvelope.ok(
            CatalogSummaryResponse(total_tables=0, total_columns=0, total_rows=0, tables=[])
        )

    # Filter out unauthorized sources for non-admins
    if current_user.role != "ADMIN":
        allowed = [p.lower() for p in current_user.allowed_projects]
        df = df[df["source_tag"].astype(str).str.lower().isin(allowed)]

    total_tables = len(df)
    total_cols = int(df["column_count"].sum()) if "column_count" in df.columns else 0
    total_rows = int(df["row_count"].sum()) if "row_count" in df.columns else 0

    tables: List[CatalogTableItem] = []
    for _, row in df.iterrows():
        tables.append(
            CatalogTableItem(
                table_master_key=int(row.get("table_master_key") or 0),
                source_tag=str(row.get("source_tag") or ""),
                database_type=str(row.get("database_type") or "") if row.get("database_type") else None,
                schema_name=str(row.get("schema_name") or "") if row.get("schema_name") else None,
                source_table=str(row.get("source_table") or ""),
                table_type=str(row.get("table_type") or "BASE TABLE"),
                row_count=int(row.get("row_count") or 0),
                column_count=int(row.get("column_count") or 0),
                business_key=str(row.get("business_key") or "") if row.get("business_key") else None,
                key_source=str(row.get("key_source") or "UNVERIFIED"),
                key_confidence=float(row.get("key_confidence") or 0.0),
                cdc_columns=str(row.get("cdc_columns") or "") if row.get("cdc_columns") else None,
                cdc_strategy=str(row.get("cdc_strategy") or "FULL_SNAPSHOT"),
                cdc_confidence=float(row.get("cdc_confidence") or 0.0),
                cdc_health=str(row.get("cdc_health") or "") if row.get("cdc_health") else None,
                cdc_health_details=str(row.get("cdc_health_details") or "") if row.get("cdc_health_details") else None,
                insert_date=str(row.get("insert_date")) if row.get("insert_date") is not None else None,
            )
        )

    return ResponseEnvelope.ok(
        CatalogSummaryResponse(
            total_tables=total_tables,
            total_columns=total_cols,
            total_rows=total_rows,
            tables=tables,
        )
    )


@router.get("/catalog/columns/{table_master_key}", response_model=ResponseEnvelope[List[CatalogColumnItem]])
def get_column_schema(
    table_master_key: int,
    current_user: UserResponse = Depends(get_current_user),
):
    try:
        df_cols = get_catalog_columns(table_master_key)
        items: List[CatalogColumnItem] = []
        if not df_cols.empty:
            for _, r in df_cols.iterrows():
                items.append(
                    CatalogColumnItem(
                        column_name=str(r.get("Column Name") or ""),
                        data_type=str(r.get("Source Datatype") or ""),
                        data_length=int(r["Max Length"]) if pd_not_null(r.get("Max Length")) else None,
                        data_precision=int(r["Precision"]) if pd_not_null(r.get("Precision")) else None,
                        data_scale=int(r["Scale"]) if pd_not_null(r.get("Scale")) else None,
                        column_seq=int(r["Sequence"]) if pd_not_null(r.get("Sequence")) else None,
                        is_nullable=str(r.get("Nullable") or "YES"),
                    )
                )
        return ResponseEnvelope.ok(items)
    except Exception as exc:
        log.error(f"Error fetching columns for master key '{table_master_key}': {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch column schema: {exc}",
        )


def pd_not_null(val) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return s not in ("", "none", "nan", "null")


# ── 3. Source-to-Target Promotion & Subscriptions ──────────────────────────────

@router.get("/targets", response_model=ResponseEnvelope[List[str]])
def list_target_systems(
    current_user: UserResponse = Depends(get_current_user),
):
    try:
        targets = [t for t in get_target_tags() if t]
        return ResponseEnvelope.ok(targets)
    except Exception as exc:
        log.error(f"Error fetching target systems: {exc}", exc_info=True)
        return ResponseEnvelope.ok([])


@router.get("/unpromoted", response_model=ResponseEnvelope[Dict[str, Any]])
def get_unpromoted_tables(
    source_tag: str = Query(...),
    target_tag: str = Query(...),
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    catalog_tables = get_catalog_tables(source_tag)
    all_execution_df = get_pipeline_tables(source_tag=source_tag, context=ctx)
    execution_df = (
        all_execution_df.loc[
            all_execution_df["target_tag"].fillna("").str.lower() == str(target_tag).lower()
        ]
        if not all_execution_df.empty and "target_tag" in all_execution_df.columns
        else all_execution_df
    )
    promoted_tables = set(execution_df["source_table"].dropna().tolist()) if not execution_df.empty and "source_table" in execution_df.columns else set()
    unpromoted_tables = [t for t in catalog_tables if t not in promoted_tables]

    return ResponseEnvelope.ok({
        "source_tag": source_tag,
        "target_tag": target_tag,
        "unpromoted_tables": unpromoted_tables,
        "catalog_tables": catalog_tables,
        "promoted_tables": sorted(list(promoted_tables)),
    })


@router.get("/subscriptions", response_model=ResponseEnvelope[List[Tier2SubscriptionItem]])
def get_current_subscriptions(
    source_tag: str = Query(...),
    target_tag: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(get_current_user),
):
    all_execution_df = get_pipeline_tables(source_tag=source_tag, context=ctx)
    if all_execution_df.empty:
        return ResponseEnvelope.ok([])

    if target_tag and "target_tag" in all_execution_df.columns:
        filtered_df = all_execution_df[
            all_execution_df["target_tag"].fillna("").str.lower() == str(target_tag).lower()
        ]
    else:
        filtered_df = all_execution_df

    items: List[Tier2SubscriptionItem] = []
    for _, row in filtered_df.iterrows():
        is_gov = str(row.get("execution_authority_mode", "")).upper() == "GOVERNED"
        is_act = str(row.get("is_active", "")).lower() == "y"
        is_cdc = str(row.get("is_cdc", "")).lower() == "y"
        strategy = str(row.get("cdc_strategy") or "FULL_SNAPSHOT")

        items.append(
            Tier2SubscriptionItem(
                source_table=str(row.get("source_table") or ""),
                target_tag=str(row.get("target_tag") or ""),
                execution_authority_mode=str(row.get("execution_authority_mode") or "GOVERNED"),
                is_active=str(row.get("is_active") or "Y"),
                is_cdc=str(row.get("is_cdc") or "N"),
                cdc_strategy=strategy,
                business_key=str(row.get("columns_unique_key") or "") if row.get("columns_unique_key") else None,
                execution_status="GOVERNED • Active" if (is_gov and is_act) else "BLOCKED • Inactive",
                load_method=f"CDC • {strategy}" if is_cdc else "Full snapshot",
            )
        )

    return ResponseEnvelope.ok(items)


@router.post("/promote", response_model=ResponseEnvelope[PromoteResponse])
def promote_datasets(
    payload: PromoteRequest,
    ctx: ProjectContext = Depends(resolve_scoped_project_context),
    current_user: UserResponse = Depends(require_admin_role),
    _csrf: None = Depends(verify_csrf_token),
):
    if not payload.steward_confirmed:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Steward declaration confirmation is required to promote datasets.",
        )

    principal = AuthenticatedPrincipal(
        actor_id=current_user.username,
        actor_type="USER",
        roles={current_user.role.lower(), current_user.role.upper()},
        username=current_user.username,
        user_id=getattr(current_user, "username", "admin"),
    )


    try:
        tables_to_promote = list(payload.table_names or [])
        if payload.refresh_existing and not tables_to_promote:
            catalog_tables = get_catalog_tables(payload.source_tag)
            all_execution_df = get_pipeline_tables(source_tag=payload.source_tag, context=ctx)
            execution_df = (
                all_execution_df.loc[
                    all_execution_df["target_tag"].fillna("").str.lower() == str(payload.target_tag).lower()
                ]
                if not all_execution_df.empty and "target_tag" in all_execution_df.columns
                else all_execution_df
            )
            promoted = set(execution_df["source_table"].dropna().tolist()) if not execution_df.empty and "source_table" in execution_df.columns else set()
            tables_to_promote = [t for t in catalog_tables if t not in promoted]

        result = PromotionService.promote_catalog_datasets(
            source_tag=payload.source_tag,
            table_names=tables_to_promote,
            target_tag=payload.target_tag,
            priority=payload.priority,
            context=ctx,
            principal=principal,
            refresh_existing=payload.refresh_existing,
        )
        clear_gui_cache()
        ConnectionService.clear_cache()

        policy_refresh = None
        if "policy_refresh" in result and isinstance(result["policy_refresh"], dict):
            pr = result["policy_refresh"]
            policy_refresh = PolicyRefreshSummary(
                updated=pr.get("updated", []),
                unchanged=pr.get("unchanged", []),
                skipped=pr.get("skipped", []),
                failed=pr.get("failed", []),
            )

        success_count = len(result.get("successful", []))
        fail_count = len(result.get("failed", []))
        msg = f"Successfully promoted {success_count} dataset(s) to '{payload.target_tag}'."
        if fail_count > 0:
            msg += f" {fail_count} dataset(s) failed onboarding."

        return ResponseEnvelope.ok(
            PromoteResponse(
                successful=result.get("successful", []),
                failed=result.get("failed", []),
                total=result.get("total", success_count + fail_count),
                priority_updated=result.get("priority_updated"),
                priority=result.get("priority"),
                policy_refresh=policy_refresh,
                message=msg,
            )
        )
    except AuthorizationError as auth_err:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(auth_err))
    except Exception as exc:
        log.error(f"Dataset promotion failed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Promotion failed: {exc}",
        )


# ── 4. Schema Drift Audit ─────────────────────────────────────────────────────

@router.post("/drift-check", response_model=ResponseEnvelope[DriftCheckResponse])
def audit_schema_drift(
    source_tag: Optional[str] = Query(None),
    cm: ConnectionManager = Depends(get_connection_manager),
    current_user: UserResponse = Depends(get_current_user),
    _csrf: None = Depends(verify_csrf_token),
):
    sql = """
        SELECT
            etl.source_tag,
            etl.source_table,
            etl.columns_unique_key      AS etl_key,
            m.business_key              AS catalog_key,
            '[WARN] BUSINESS KEY MISMATCH'  AS alert
        FROM metadata.db_tables_etl etl
        JOIN metadata.db_tables m
          ON  LOWER(etl.source_tag)   = LOWER(m.database_tag)
         AND  LOWER(etl.source_table) = LOWER(m.table_name)
        WHERE etl.columns_unique_key IS DISTINCT FROM m.business_key
    """
    params = []
    if source_tag and source_tag.strip():
        sql += " AND LOWER(etl.source_tag) = LOWER(%s)"
        params.append(source_tag.strip())
    sql += " ORDER BY etl.source_tag, etl.source_table"

    mismatches: List[DriftCheckItem] = []

    try:
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                for r in rows:
                    mismatches.append(
                        DriftCheckItem(
                            source_tag=str(r["source_tag"]),
                            source_table=str(r["source_table"]),
                            etl_key=str(r["etl_key"]) if r["etl_key"] is not None else None,
                            catalog_key=str(r["catalog_key"]) if r["catalog_key"] is not None else None,
                            alert=str(r["alert"]),
                        )
                    )

        cnt = len(mismatches)
        status_str = "DRIFT_DETECTED" if cnt > 0 else "IN_SYNC"
        message = (
            f"Schema drift audit completed: {cnt} business key mismatch(es) detected between Master Catalog and Execution Control."
            if cnt > 0
            else "Schema drift audit passed: Zero mismatches between Master Catalog and Execution Control."
        )
        return ResponseEnvelope.ok(
            DriftCheckResponse(
                source_tag=source_tag,
                total_mismatches=cnt,
                mismatches=mismatches,
                status=status_str,
                message=message,
            )
        )
    except Exception as exc:
        log.error(f"Error auditing schema drift: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema drift audit query failed: {exc}",
        )
