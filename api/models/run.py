# api/models/run.py
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class RunStageItem(BaseModel):
    stage_name: str
    status: str
    rows_processed: int = 0
    throughput_rows_sec: Optional[float] = 0.0
    throughput_rows_min: Optional[float] = 0.0

class RunSummaryItem(BaseModel):
    run_id: str
    action: Optional[str] = None
    source_tag: Optional[str] = None
    target_tag: Optional[str] = None
    source_table: Optional[str] = None
    target_table: Optional[str] = None
    priority: Optional[int] = None
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    elapsed_seconds: float = 0.0
    source_count: int = 0
    target_count: int = 0
    rows_per_minute: Optional[float] = 0.0
    error_message: Optional[str] = None

class RunDetailResponse(BaseModel):
    run_id: str
    parent_run_id: Optional[str] = None
    retry_attempt: int = 0
    project_id: str
    environment: str = "PROD"
    pipeline_id: Optional[str] = None
    dataset_id: Optional[str] = None
    status: str
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    last_heartbeat_at: Optional[str] = None
    elapsed_seconds: float = 0.0
    rows_read: int = 0
    extracted_count: int = 0
    rows_written: int = 0
    throughput_rows_sec: float = 0.0
    rolling_rows_per_second: float = 0.0
    rows_per_minute: float = 0.0
    rolling_rows_per_minute: float = 0.0
    current_chunk: Optional[int] = None
    total_chunks: Optional[int] = None
    chunks_completed: Optional[int] = None
    chunk_progress_pct: Optional[float] = None
    is_stalled: bool = False
    last_heartbeat_age_seconds: Optional[float] = None
    estimated_remaining_seconds: Optional[float] = None
    reconciliation_status: str = "PENDING"
    stages: List[RunStageItem] = Field(default_factory=list)
    error_message: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None

class TriggerRunRequest(BaseModel):
    source_tag: str
    target_tag: Optional[str] = None
    source_table_ids: Optional[List[int]] = None
    source_table: Optional[str] = None
    execution_scope: Optional[str] = "FULL"  # "FULL", "BRONZE", "SILVER", "GOLD"
    priority: Optional[int] = None
    request_id: Optional[str] = None

class TriggerRunResponse(BaseModel):
    accepted: bool
    request_id: str
    run_id: Optional[str] = None
    status: str
    submitted_at: Optional[str] = None
    rejection_code: Optional[str] = None
    rejection_reason: Optional[str] = None
    existing_active_run_id: Optional[str] = None

class CancelRunResponse(BaseModel):
    success: bool
    run_id: str
    message: str

class RunLogsResponse(BaseModel):
    run_id: str
    logs: str
    lines_returned: int

