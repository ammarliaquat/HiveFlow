# src/metadata/metadata_models.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Any, Tuple
from datetime import datetime, date


@dataclass
class MetadataRow:
    """
    Represents one enriched row returned by MetadataReader.get_tables().
    Combines metadata.db_tables_etl (execution rules) with metadata.db_tables
    (master catalog, joined on source_tag + source_table).
    """
    id: int
    source_tag: str
    source_schema: Optional[str]
    source_table: str
    source_db_type: str              # From metadata.db_tables.database_type via LEFT JOIN

    landing_table: Optional[str]
    history_table: Optional[str]
    target_table: Optional[str]
    target_schema: Optional[str]

    is_active: str
    priority: int
    load_type: str

    is_cdc: str
    cdc_columns: Optional[str]          # comma-separated string
    cdc_p_days: int
    delta_days: int
    last_success_timestamp: Optional[datetime]

    is_reconcile: str
    reconcile_column: Optional[str]
    reconcile_measure: str

    chunk_size: int
    num_partitions: int
    partition_column: Optional[str]

    source_key_prefix: Optional[str]
    natural_key_column: Optional[str]
    skip_dedup: str = "n"

    notes: Optional[str] = None
    reconcile_threshold: float = 0.0
    reconcile_action: str = "FAIL"
    rerun_flag: str = "n"
    last_success_date: Optional[datetime] = None
    target_tag: Optional[str] = None      # metadata.connections tag for target DB
    target_database_type: Optional[str] = None
    # Self-healing observability: populated from batch_log LEFT JOIN
    last_load_pct: Optional[float] = None    # load_pct from the most recent batch run
    last_run_end_time: Optional[datetime] = None  # end_time from the most recent batch run
    landing_mode: str = "METADATA"
    mapping_policy: str = "EXPLICIT"
    schema_evolution_policy: str = "PERMISSIVE"
    partition_lower_bound: Optional[int] = None
    partition_upper_bound: Optional[int] = None
    target_rows_per_partition: int = 250000
    minimum_partition_threshold: int = 50000
    maximum_partitions: int = 32
    key_source: Optional[str] = None
    key_confidence: Optional[int] = None
    key_candidate_columns: Optional[Any] = None
    key_evidence_type: Optional[str] = None
    key_candidate_nullable: Optional[bool] = None
    key_candidate_compatible: Optional[bool] = None
    current_key_evidence_fingerprint: Optional[str] = None
    key_approval_state: Optional[str] = None
    approved_source_business_key: Optional[Any] = None
    approved_key_evidence: Optional[str] = None
    approved_key_evidence_fingerprint: Optional[str] = None
    key_approved_by: Optional[str] = None
    key_approved_at: Optional[datetime] = None
    key_approval_revision: Optional[int] = None
    cdc_strategy: Optional[str] = None
    cdc_source: Optional[str] = None
    cdc_confidence: Optional[int] = None
    cdc_health: str = "OK"
    
    # MAP-01: Frozen column mapping snapshot resolved once per attempt
    resolved_mappings: Optional['ResolvedTableMappings'] = None

    # KEY-02: Canonical target business key and immutable execution contract
    resolved_target_key: Optional[Any] = None
    table_contract: Optional[Any] = None

    def calculate_adaptive_partitions(self, row_count: Optional[int] = None, override: Optional[int] = None) -> int:
        """
        Industry-Standard Adaptive Partitioning Algorithm:
        Formula: numPartitions = min(ceil(source_count / target_rows_per_partition), maximum_partitions)
        If source_count < minimum_partition_threshold -> 1 partition.
        """
        if override is not None:
            return override
        count = row_count
        if count is None:
            return self.num_partitions or 1
        if count < self.minimum_partition_threshold:
            return 1
        import math
        calc = math.ceil(count / float(self.target_rows_per_partition))
        return min(calc, self.maximum_partitions)

    @property
    def cdc_column_list(self) -> List[str]:
        """Returns CDC columns as a Python list"""
        if not self.cdc_columns:
            return []
        return [c.strip() for c in self.cdc_columns.split(",")]

    @property
    def use_cdc(self) -> bool:
        return self.is_cdc == "y"

    @property
    def use_reconcile(self) -> bool:
        return self.is_reconcile == "y"

    @property
    def full_source_table(self) -> str:
        """Returns schema.table if schema is set"""
        if self.source_schema:
            return f"{self.source_schema}.{self.source_table}"
        return self.source_table

    @property
    def canonical_business_key_columns(self) -> Tuple[str, ...]:
        """
        Return normalized legacy candidate columns for compatibility displays.

        This property is not execution authority. New policy code must use
        canonical_source_business_key instead.
        """
        if not self.natural_key_column or not str(self.natural_key_column).strip():
            return tuple()
        parts = str(self.natural_key_column).replace("+", ",").split(",")
        cleaned = [p.strip() for p in parts if p.strip()]
        return tuple(cleaned)

    @property
    def canonical_source_business_key(self) -> Optional[Tuple[str, ...]]:
        """Return only explicitly approved source identity columns."""
        if str(self.key_approval_state or "").strip().upper() != "APPROVED":
            return None
        provenance = (
            self.approved_key_evidence,
            self.approved_key_evidence_fingerprint,
            self.current_key_evidence_fingerprint,
            self.key_approved_by,
            self.key_approved_at,
            self.key_approval_revision,
        )
        if any(value is None or value == "" for value in provenance):
            return None
        if int(self.key_approval_revision) <= 0:
            return None
        if self.approved_key_evidence_fingerprint != self.current_key_evidence_fingerprint:
            return None
        if self.key_candidate_nullable is not False or self.key_candidate_compatible is not True:
            return None
        raw_columns = self.approved_source_business_key
        if raw_columns is None:
            return None
        if isinstance(raw_columns, str):
            import json

            try:
                raw_columns = json.loads(raw_columns)
            except (TypeError, ValueError):
                return None
        if not isinstance(raw_columns, (list, tuple)):
            return None
        columns = tuple(str(column).strip() for column in raw_columns)
        if not columns or any(not column for column in columns):
            return None
        if len({column.lower() for column in columns}) != len(columns):
            return None
        return columns

from enum import Enum

class MappingPolicy(Enum):
    EXPLICIT = "EXPLICIT"
    IDENTITY = "IDENTITY"

@dataclass(frozen=True)
class ResolvedColumnMapping:
    """Immutable mapping snapshot for a single column."""
    column_id: int
    column_sequence: Optional[int]
    source_column: str
    target_column: str
    data_type: Optional[str]
    source_column_datatype: Optional[str]
    canonical_type: Optional[str]
    transform_expr: Optional[str] = None
    default_value: Optional[str] = None
    
    # Compatibility with legacy dict-based access for Transformer
    def __getitem__(self, item):
        return getattr(self, item)
        
    def get(self, item, default=None):
        return getattr(self, item, default)

@dataclass(frozen=True)
class ResolvedTableMappings:
    """Immutable frozen snapshot of column mappings for an execution attempt."""
    table_id: int
    mapping_policy: MappingPolicy
    mappings: Tuple[ResolvedColumnMapping, ...]
    fingerprint: str
    frozen_source_schema: Optional[Tuple[str, ...]] = None

    @classmethod
    def build(cls, table_id: int, raw_mappings: List[dict], mapping_policy: MappingPolicy = MappingPolicy.EXPLICIT, frozen_source_schema: Optional[Tuple[str, ...]] = None) -> "ResolvedTableMappings":
        import hashlib
        import json
        
        resolved_mappings = tuple(
            ResolvedColumnMapping(**m) for m in raw_mappings
        )
        
        # Build deterministic fingerprint
        hash_list = [f"policy:{mapping_policy.value}"]
        if frozen_source_schema:
            hash_list.append(f"schema:{','.join(frozen_source_schema)}")
            
        for m in resolved_mappings:
            hash_list.append(f"{m.column_sequence}:{m.column_id}:{m.source_column}:{m.target_column}:{m.data_type}:{m.canonical_type}:{m.transform_expr}:{m.default_value}")
        
        fingerprint = hashlib.sha256(json.dumps(hash_list).encode("utf-8")).hexdigest()
        
        return cls(
            table_id=table_id,
            mapping_policy=mapping_policy,
            mappings=resolved_mappings,
            fingerprint=fingerprint,
            frozen_source_schema=frozen_source_schema
        )

    def with_frozen_schema(self, schema: list) -> "ResolvedTableMappings":
        """Returns a new immutable instance with the frozen source schema applied."""
        import hashlib
        import json
        
        # Build deterministic fingerprint
        hash_list = [f"policy:{self.mapping_policy.value}"]
        if schema:
            hash_list.append(f"schema:{','.join(schema)}")
            
        for m in self.mappings:
            hash_list.append(f"{m.column_sequence}:{m.column_id}:{m.source_column}:{m.target_column}:{m.data_type}:{m.canonical_type}:{m.transform_expr}:{m.default_value}")
        
        fingerprint = hashlib.sha256(json.dumps(hash_list).encode("utf-8")).hexdigest()
        
        return ResolvedTableMappings(
            table_id=self.table_id,
            mapping_policy=self.mapping_policy,
            mappings=self.mappings,
            fingerprint=fingerprint,
            frozen_source_schema=tuple(schema) if schema else tuple()
        )
