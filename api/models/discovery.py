# api/models/discovery.py
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class CrawlRequest(BaseModel):
    source_tag: str = Field(..., description="Target source database system tag to crawl")


class CrawlResponse(BaseModel):
    run_id: str
    status: str
    message: str


class CrawlStatusResponse(BaseModel):
    run_id: str
    status: str
    elapsed_seconds: float
    error_message: Optional[str] = None


class CatalogTableItem(BaseModel):
    table_master_key: int
    source_tag: str
    database_type: Optional[str] = None
    schema_name: Optional[str] = None
    source_table: str
    table_type: str = "BASE TABLE"
    row_count: int = 0
    column_count: int = 0
    business_key: Optional[str] = None
    key_source: str = "UNVERIFIED"
    key_confidence: float = 0.0
    cdc_columns: Optional[str] = None
    cdc_strategy: str = "FULL_SNAPSHOT"
    cdc_confidence: float = 0.0
    cdc_health: Optional[str] = None
    cdc_health_details: Optional[str] = None
    insert_date: Optional[str] = None


class CatalogSummaryResponse(BaseModel):
    total_tables: int
    total_columns: int
    total_rows: int
    tables: List[CatalogTableItem]


class CatalogColumnItem(BaseModel):
    column_name: str
    data_type: str
    data_length: Optional[int] = None
    data_precision: Optional[int] = None
    data_scale: Optional[int] = None
    column_seq: Optional[int] = None
    is_nullable: Optional[str] = None


class PromoteRequest(BaseModel):
    source_tag: str = Field(..., description="Source database system tag")
    target_tag: str = Field(..., description="Target destination system tag")
    table_names: List[str] = Field(default_factory=list, description="List of dataset names to promote")
    priority: int = Field(default=1, ge=1, le=3, description="Execution priority level (1, 2, 3)")
    refresh_existing: bool = Field(default=False, description="Whether to refresh policies for existing tables")
    steward_confirmed: bool = Field(default=True, description="Operator confirmation of steward onboarding")


class PolicyRefreshSummary(BaseModel):
    updated: List[str] = Field(default_factory=list)
    unchanged: List[str] = Field(default_factory=list)
    skipped: List[str] = Field(default_factory=list)
    failed: List[Dict[str, Any]] = Field(default_factory=list)


class PromoteResponse(BaseModel):
    successful: List[str] = Field(default_factory=list)
    failed: List[Dict[str, Any]] = Field(default_factory=list)
    total: int = 0
    priority_updated: Optional[int] = None
    priority: Optional[int] = None
    policy_refresh: Optional[PolicyRefreshSummary] = None
    message: str


class DriftCheckItem(BaseModel):
    source_tag: str
    source_table: str
    etl_key: Optional[str] = None
    catalog_key: Optional[str] = None
    alert: str = "[WARN] BUSINESS KEY MISMATCH"


class DriftCheckResponse(BaseModel):
    source_tag: Optional[str] = None
    total_mismatches: int
    mismatches: List[DriftCheckItem]
    status: str
    message: str


class Tier2SubscriptionItem(BaseModel):
    source_table: str
    target_tag: str
    execution_authority_mode: str = "GOVERNED"
    is_active: str = "Y"
    is_cdc: str = "N"
    cdc_strategy: str = "FULL_SNAPSHOT"
    business_key: Optional[str] = None
    execution_status: str
    load_method: str
