# api/models/dashboard.py
from typing import Optional, List
from pydantic import BaseModel, Field

class KPIData(BaseModel):
    running: int = 0
    success_today: int = 0
    failed_today: int = 0
    last_run_end: Optional[str] = None
    stalled_count: int = 0
    project_scope: Optional[str] = None

class RecentActivityItem(BaseModel):
    run_id: Optional[str] = None
    layer: Optional[str] = None
    source: Optional[str] = None
    target_tag: Optional[str] = None
    table: Optional[str] = None
    target_table: Optional[str] = None
    status: Optional[str] = None
    source_count: int = 0
    target_count: int = 0
    elapsed_sec: float = 0.0
    end_time: Optional[str] = None

class DashboardActivityResponse(BaseModel):
    activities: List[RecentActivityItem] = Field(default_factory=list)
