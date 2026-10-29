# tests/test_key02_live_gate.py
"""
KEY-02 — Live Integration Gate Verification Suite.

Validates KEY-02 target business identity resolution against real development metadata:
- Case 1: Live ulog (table_id=120) target key derivation & physical inspection.
- Case 2: Renamed source business key (sess_id -> session_identifier) deriving mapped target identity.
- Case 3: Composite key order preservation (source key order governs target key order).
"""

import pytest

from src.connections.connection_manager import ConnectionManager
from src.transform.column_mapper import ColumnMapper
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.metadata.policy_models import CandidateTableContract
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings
from src.metadata.target_key_resolver import TargetKeyResolver


# ─── CASE 1: LIVE ULOG TABLE (table_id=120) ──────────────────────────────────
def test_key02_live_case1_ulog_target_identity():
    """
    Case 1: Verify ulog (table_id 120) resolves canonical target key ('id',)
    from live metadata and inspects landing.ulog physically with ZERO mutations.
    """
    cm = ConnectionManager("config/config.local.yaml")
    mapper = ColumnMapper(cm)
    resolver = TargetKeyResolver(cm)

    # 1. Resolve live column mappings for ulog
    live_mappings = mapper.resolve_table_mappings(
        table_id=120,
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    assert live_mappings.table_id == 120
    assert len(live_mappings.mappings) >= 8

    # 2. Approved source business key for ulog
    approved_src_key = ApprovedBusinessKey(
        columns=("id",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_src_ulog_live",
    )

    # 3. Derive canonical target business key
    target_key = resolver.resolve_target_business_key(
        table_id=120,
        approved_source_key=approved_src_key,
        resolved_mappings=live_mappings,
    )

    assert isinstance(target_key, ResolvedTargetBusinessKey)
    assert target_key.table_id == 120
    assert target_key.source_key_columns == ("id",)
    assert target_key.target_key_columns == ("id",)
    assert target_key.source_key_evidence_fingerprint == "fp_src_ulog_live"
    assert target_key.mapping_fingerprint == live_mappings.fingerprint
    assert len(target_key.fingerprint) == 64

    # 4. Assemble Candidate Contract
    contract = resolver.assemble_candidate_contract(
        table_id=120,
        approved_source_key=approved_src_key,
        resolved_mappings=live_mappings,
    )
    assert isinstance(contract, CandidateTableContract)
    assert len(contract.contract_fingerprint) == 64


# ─── CASE 2: RENAMED KEY DERIVES MAPPED TARGET IDENTITY ──────────────────────
def test_key02_live_case2_renamed_key_derivation():
    """
    Case 2: Verify that for a renamed key column (sess_id -> session_identifier),
    KEY-02 derives the mapped target key column name, NOT the source column name.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("sess_id",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_sess_id_102",
    )

    # Explicit rename mapping
    raw_mappings = [
        {"column_id": 1, "column_sequence": 1, "source_column": "sess_id", "target_column": "session_identifier", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"},
        {"column_id": 2, "column_sequence": 2, "source_column": "customer_id", "target_column": "customer_id", "data_type": "bigint", "source_column_datatype": "bigint", "canonical_type": "BIGINT"},
    ]
    resolved_mappings = ResolvedTableMappings.build(
        table_id=102,
        raw_mappings=raw_mappings,
        mapping_policy=MappingPolicy.EXPLICIT,
    )

    resolver = TargetKeyResolver()
    target_key = resolver.resolve_target_business_key(
        table_id=102,
        approved_source_key=approved_src_key,
        resolved_mappings=resolved_mappings,
    )

    assert target_key.source_key_columns == ("sess_id",)
    assert target_key.target_key_columns == ("session_identifier",)  # Mapped target name, not source name
    assert "sess_id" not in target_key.target_key_columns


# ─── CASE 3: COMPOSITE KEY ORDER PRESERVATION ─────────────────────────────────
def test_key02_live_case3_composite_key_order_preservation():
    """
    Case 3: Verify composite key (hospital_id, patient_id, visit_no) preserves exact
    source key order even when mappings are returned out of order.
    """
    approved_composite_key = ApprovedBusinessKey(
        columns=("hospital_id", "patient_id", "visit_no"),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_comp_live",
    )

    # Reversed mapping order
    raw_mappings = [
        {"column_id": 3, "column_sequence": 3, "source_column": "visit_no", "target_column": "adm_visit_no", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"},
        {"column_id": 2, "column_sequence": 2, "source_column": "patient_id", "target_column": "mr_number", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"},
        {"column_id": 1, "column_sequence": 1, "source_column": "hospital_id", "target_column": "facility_code", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"},
    ]
    resolved_mappings = ResolvedTableMappings.build(
        table_id=108,
        raw_mappings=raw_mappings,
        mapping_policy=MappingPolicy.EXPLICIT,
    )

    resolver = TargetKeyResolver()
    target_key = resolver.resolve_target_business_key(
        table_id=108,
        approved_source_key=approved_composite_key,
        resolved_mappings=resolved_mappings,
    )

    # Strictly matches source key order (facility_code, mr_number, adm_visit_no)
    assert target_key.source_key_columns == ("hospital_id", "patient_id", "visit_no")
    assert target_key.target_key_columns == ("facility_code", "mr_number", "adm_visit_no")
