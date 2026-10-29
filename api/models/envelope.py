# api/models/envelope.py
from datetime import datetime, timezone
from typing import Generic, TypeVar, Optional, Any
from pydantic import BaseModel, Field

T = TypeVar("T")

class ErrorDetail(BaseModel):
    code: str = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Sanitized human-readable message")
    failing_stage: Optional[str] = Field(None, description="Stage where failure occurred")
    correlation_id: Optional[str] = Field(None, description="Correlation identifier for tracing")
    retryable: bool = Field(False, description="Whether the operation can be safely retried")
    remediation: Optional[str] = Field(None, description="Actionable operator remediation guidance")

class ResponseMeta(BaseModel):
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    request_id: Optional[str] = None

class ResponseEnvelope(BaseModel, Generic[T]):
    success: bool = True
    data: Optional[T] = None
    error: Optional[ErrorDetail] = None
    meta: ResponseMeta = Field(default_factory=ResponseMeta)

    @classmethod
    def ok(cls, data: T, request_id: Optional[str] = None) -> "ResponseEnvelope[T]":
        return cls(
            success=True,
            data=data,
            error=None,
            meta=ResponseMeta(request_id=request_id),
        )

    @classmethod
    def fail(
        cls,
        code: str,
        message: str,
        failing_stage: Optional[str] = None,
        correlation_id: Optional[str] = None,
        retryable: bool = False,
        remediation: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> "ResponseEnvelope[Any]":
        return cls(
            success=False,
            data=None,
            error=ErrorDetail(
                code=code,
                message=message,
                failing_stage=failing_stage,
                correlation_id=correlation_id,
                retryable=retryable,
                remediation=remediation,
            ),
            meta=ResponseMeta(request_id=request_id or correlation_id),
        )
