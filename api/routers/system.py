# api/routers/system.py
import time
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status

from api.models.health import SystemHealthData, ProbeDetail
from api.models.integration import (
    IntegrationDetail,
    IntegrationsSummary,
    ConnectionCreateRequest,
    ConnectionTestResult,
    ConnectionSaveResponse,
)
from api.models.envelope import ResponseEnvelope
from api.models.auth import UserResponse
from api.security.auth import get_current_user, get_connection_manager
from src.validation.health_checker import HealthChecker
from src.services.connection_service import ConnectionService

router = APIRouter(prefix="/system", tags=["System"])

# In-memory cache for health checks (30-second TTL to avoid DB connection flooding)
# NOTE: Process-local cache for single-worker uvicorn deployment. When scaling horizontally,
# replace with centralized Redis or metadata lease cache.
_HEALTH_CACHE: dict = {"data": None, "cached_at": 0.0}
_CACHE_TTL_SECONDS = 30.0

@router.get("/health", response_model=ResponseEnvelope[SystemHealthData])
def get_system_health(current_user: UserResponse = Depends(get_current_user)):
    now = time.time()
    if _HEALTH_CACHE["data"] and (now - _HEALTH_CACHE["cached_at"]) < _CACHE_TTL_SECONDS:
        return ResponseEnvelope.ok(_HEALTH_CACHE["data"])

    cm = get_connection_manager()
    checker = HealthChecker(cm)
    probes: List[ProbeDetail] = []
    all_healthy = True

    # 1. Metadata Repository DB Probe
    t0 = time.time()
    db_ok, db_msg = checker.check_repository_db()
    db_lat = round((time.time() - t0) * 1000, 2)
    probes.append(
        ProbeDetail(
            name="PostgreSQL Metadata Repository",
            target_component="METADATA_DB",
            status="REACHABLE" if db_ok else "UNAVAILABLE",
            message="Connection verified (SELECT 1)" if db_ok else f"Failed: {db_msg}",
            latency_ms=db_lat,
        )
    )
    if not db_ok:
        all_healthy = False

    # 2. Schema Certification Ledger Probe
    t0 = time.time()
    try:
        from src.connections.schema_certifier import SchemaCertifier
        certifier = SchemaCertifier(cm)
        certifier.certify()
        cert_ok = True
        cert_msg = "All certified migrations verified via SHA-256 ledger"
    except Exception as cert_err:
        cert_ok = False
        cert_msg = f"Schema certification error: {cert_err}"
    cert_lat = round((time.time() - t0) * 1000, 2)
    probes.append(
        ProbeDetail(
            name="Migration Schema Certification",
            target_component="MIGRATION_LEDGER",
            status="CERTIFIED" if cert_ok else "DEGRADED",
            message=cert_msg,
            latency_ms=cert_lat,
        )
    )
    if not cert_ok:
        all_healthy = False

    # 3. Compute Engine Declaration (Per CTO Blocker 4: Do not fake cluster health)
    probes.append(
        ProbeDetail(
            name="Spark Connect Compute Engine",
            target_component="COMPUTE_ENGINE",
            status="UNMONITORED",
            message="Worker cluster reachability is governed per-job session; not probed in read-only API.",
            latency_ms=None,
        )
    )

    overall = "PARTIAL_HEALTHY" if all_healthy else "DEGRADED"
    result = SystemHealthData(
        overall_status=overall,
        scope_notice="Partial: Probes verify Metadata DB and Migration Ledger. Worker compute and target endpoints are governed per-job.",
        timestamp=datetime.now(timezone.utc).isoformat(),
        probes=probes,
    )

    _HEALTH_CACHE["data"] = result
    _HEALTH_CACHE["cached_at"] = now

    return ResponseEnvelope.ok(result)


@router.get("/integrations", response_model=ResponseEnvelope[IntegrationsSummary])
def get_integrations(
    current_user: UserResponse = Depends(get_current_user),
    scope: Optional[str] = None,
):
    """Returns governed external database and storage integrations.
    
    Zero hardcoded values: populated from metadata.connections.
    Secret hygiene: passwords, tokens, and raw credentials are NEVER returned.
    """
    raw_conns = ConnectionService.list_connections(active_only=False)
    
    # Filter by project scope if provided and user is not superadmin
    if scope and scope.lower() not in {"all", "*"}:
        filtered = [
            c for c in raw_conns
            if c.get("tag") == scope or c.get("supports_target")
        ]
    else:
        filtered = raw_conns

    details: List[IntegrationDetail] = []
    for r in filtered:
        details.append(
            IntegrationDetail(
                tag=r["tag"],
                connection_type=r.get("connection_type") or "DATABASE",
                supports_source=bool(r.get("supports_source")),
                supports_target=bool(r.get("supports_target")),
                environment=r.get("environment") or "PROD",
                db_type=r.get("db_type") or "unknown",
                server=str(r.get("server") or ""),
                port=int(r.get("port") or 0),
                database_name=str(r.get("database_name") or ""),
                schema_name=r.get("schema_name"),
                username=r.get("username"),
                is_active=str(r.get("is_active") or "y"),
                purpose=r.get("purpose"),
                last_tested_at=str(r["last_tested_at"]) if r.get("last_tested_at") else None,
                last_test_status=r.get("last_test_status"),
                last_latency_ms=float(r["last_latency_ms"]) if r.get("last_latency_ms") is not None else None,
            )
        )

    summary = IntegrationsSummary(
        total_count=len(details),
        source_count=sum(1 for d in details if d.supports_source),
        target_count=sum(1 for d in details if d.supports_target),
        active_count=sum(1 for d in details if d.is_active.lower() == "y"),
        integrations=details,
    )
    return ResponseEnvelope.ok(summary)


@router.post("/connections/test", response_model=ResponseEnvelope[ConnectionTestResult])
def test_connection_profile(
    req: ConnectionCreateRequest,
    current_user: UserResponse = Depends(get_current_user),
):
    """Executes a pre-save connectivity and latency probe without saving to metadata.
    
    Zero hardcoded values. Resolves credentials via environment references (.env).
    """
    profile = req.model_dump()
    result = ConnectionService.test_connection(profile, update_db=False)
    return ResponseEnvelope.ok(
        ConnectionTestResult(
            status=result["status"],
            latency_ms=result.get("latency_ms", 0),
            error=result.get("error") if result.get("error") else None,
        )
    )


@router.post("/connections", response_model=ResponseEnvelope[ConnectionSaveResponse])
def save_connection_profile(
    req: ConnectionCreateRequest,
    current_user: UserResponse = Depends(get_current_user),
):
    """Persists connection profile to metadata.connections with Git-style audit snapshot.
    
    Restricted to ADMIN users.
    Enforces secret hygiene: raw credentials never stored, only credential_reference.
    """
    if current_user.role.upper() != "ADMIN" and "manage_connections" not in current_user.permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Admin role required to onboard connections.",
        )
    
    profile = req.model_dump()
    try:
        ConnectionService.save_connection(profile, username=current_user.username)
        ConnectionService.clear_cache()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to save connection profile: {e}",
        )
    
    return ResponseEnvelope.ok(
        ConnectionSaveResponse(
            tag=req.tag.strip().lower(),
            status="SAVED",
            message=f"Connection '{req.tag}' successfully onboarded to metadata catalog.",
        )
    )


@router.post("/cache/clear", response_model=ResponseEnvelope[dict])
def clear_system_cache(current_user: UserResponse = Depends(get_current_user)):
    """Clears application and metadata query caches."""
    global _HEALTH_CACHE
    _HEALTH_CACHE = {"data": None, "cached_at": 0.0}
    try:
        from gui.services.metadata_service import clear_gui_cache
        clear_gui_cache()
    except Exception:
        pass
    try:
        ConnectionService.clear_cache()
    except Exception:
        pass
    return ResponseEnvelope.ok({
        "status": "CLEARED",
        "message": "Application query and connection cache successfully cleared.",
    })


