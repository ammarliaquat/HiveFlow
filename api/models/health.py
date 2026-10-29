# api/models/health.py
from typing import List, Optional
from pydantic import BaseModel, Field

class ProbeDetail(BaseModel):
    name: str
    target_component: str = Field(..., description="Target architectural component (e.g. METADATA_DB, MIGRATION_LEDGER)")
    status: str = Field(..., description="REACHABLE, UNAVAILABLE, CERTIFIED, DEGRADED, UNMONITORED")
    message: Optional[str] = None
    latency_ms: Optional[float] = None

class SystemHealthData(BaseModel):
    overall_status: str = Field(..., description="PARTIAL_HEALTHY, DEGRADED, or UNAVAILABLE")
    scope_notice: str = Field(
        default="Partial: Probes verify Metadata DB and Migration Ledger. Worker compute and target endpoints are governed per-job.",
        description="Explicit scope declaration per CTO Blocker 4"
    )
    timestamp: str
    probes: List[ProbeDetail] = Field(default_factory=list)

