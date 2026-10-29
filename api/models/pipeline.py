# api/models/pipeline.py
from typing import Optional, List
from pydantic import BaseModel, Field, computed_field

class PipelineItem(BaseModel):
    table_id: int
    source_tag: str
    target_tag: Optional[str] = None
    priority: int = 1
    source_table: str
    target_table: Optional[str] = None
    is_active: bool = True
    is_cdc: bool = False
    cdc_strategy: Optional[str] = None
    columns_unique_key: Optional[str] = None
    execution_authority_mode: Optional[str] = None
    last_success_date: Optional[str] = None
    last_success_timestamp: Optional[str] = None
    landing_mode: str = "METADATA"

    @computed_field
    @property
    def table_name(self) -> str:
        return self.source_table

    @computed_field
    @property
    def target_table_name(self) -> Optional[str]:
        return self.target_table

class ColumnMappingItem(BaseModel):
    column_id: Optional[int] = None
    source_column: str
    target_column: Optional[str] = None
    canonical_type: Optional[str] = None
    is_key: bool = False
    is_cdc_column: bool = False

class PipelineDetailResponse(BaseModel):
    pipeline: PipelineItem
    columns: List[ColumnMappingItem] = Field(default_factory=list)
