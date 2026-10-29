# src/models/submission.py
"""
Domain models for execution request submission and authoritative results.
Guarantees caller-generated request idempotency and structured database provenance.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple


@dataclass(frozen=True)
class SubmissionRequest:
    """
    Caller-generated submission request model.
    Immutable value object passed from UI/API into ExecutionService.
    """
    request_id: str
    action: str
    source_tag: str
    source_table_ids: Tuple[int, ...] = field(default_factory=tuple)
    source_table: Optional[str] = None
    target_tag: Optional[str] = None
    priority: Optional[int] = None
    actor_type: str = "USER"
    actor_id: str = "anonymous"
    extra_flags: Optional[List[str]] = None
    parameters: Optional[Dict[str, Any]] = None
    execution_scope: Optional[str] = None  # FULL | BRONZE | SILVER | GOLD; None means FULL


@dataclass(frozen=True)
class SubmissionResult:
    """
    Authoritative result of an execution submission attempt.
    For rejections: accepted=False, run_id=None, submitted_at=None.
    For acceptances: accepted=True, run_id=UUID, submitted_at=DB_TIMESTAMP.
    """
    accepted: bool
    request_id: str
    run_id: Optional[str] = None
    status: str = "REJECTED"
    submitted_at: Optional[datetime] = None
    rejection_code: Optional[str] = None
    rejection_reason: Optional[str] = None
    existing_active_run_id: Optional[str] = None
    existing_submitted_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "accepted": self.accepted,
            "request_id": self.request_id,
            "run_id": self.run_id,
            "status": self.status,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "rejection_code": self.rejection_code,
            "rejection_reason": self.rejection_reason,
            "existing_active_run_id": self.existing_active_run_id,
            "existing_submitted_at": self.existing_submitted_at.isoformat() if self.existing_submitted_at else None,
        }
