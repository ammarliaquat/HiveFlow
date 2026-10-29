# api/models/integration.py
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class IntegrationDetail(BaseModel):
    tag: str = Field(..., description="Unique connection tag / system identifier")
    connection_type: str = Field(default="DATABASE", description="DATABASE, STORAGE, or SERVICE")
    supports_source: bool = Field(default=False)
    supports_target: bool = Field(default=False)
    environment: str = Field(default="PROD")
    db_type: str = Field(..., description="mysql, postgres, oracle, sqlserver, etc.")
    server: str = Field(..., description="Resolved host or hostname")
    port: int = Field(default=0)
    database_name: str = Field(...)
    schema_name: Optional[str] = None
    username: Optional[str] = None
    is_active: str = Field(default="y")
    purpose: Optional[str] = None
    last_tested_at: Optional[str] = None
    last_test_status: Optional[str] = None
    last_latency_ms: Optional[float] = None

class IntegrationsSummary(BaseModel):
    total_count: int
    source_count: int
    target_count: int
    active_count: int
    integrations: List[IntegrationDetail]


class ConnectionCreateRequest(BaseModel):
    tag: str = Field(..., description="Unique lowercase connection tag, e.g. 'crm_mysql'")
    connection_type: str = Field(default="DATABASE", description="DATABASE, STORAGE, or SERVICE")
    supports_source: bool = Field(default=True)
    supports_target: bool = Field(default=False)
    environment: str = Field(default="PROD")
    db_type: str = Field(..., description="mysql, postgres, oracle, sqlserver, etc.")
    server: str = Field(..., description="Host or ${ENV_VAR}")
    port: int = Field(default=0)
    database_name: str = Field(..., description="Database name or service catalog")
    schema_name: Optional[str] = None
    username: str = Field(...)
    credential_reference: str = Field(..., description="Environment variable reference for secret, e.g. ${CRM_PASSWORD}")
    service_name: Optional[str] = None
    connection_options: Optional[Dict[str, Any]] = None
    owner: Optional[str] = "Admin"
    team: Optional[str] = "Data Engineering"
    purpose: Optional[str] = "Onboarded via HiveFlow GUI"


class ConnectionTestResult(BaseModel):
    status: str = Field(..., description="SUCCESS or FAILED")
    latency_ms: int = Field(default=0)
    error: Optional[str] = None


class ConnectionSaveResponse(BaseModel):
    tag: str
    status: str
    message: str
