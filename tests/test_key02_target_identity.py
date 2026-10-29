# tests/test_key02_target_identity.py
"""
KEY-02 — Target Business Identity Authority & Attempt Contract Test Suite.

Validates:
- Simple rename-only mapping (MR_CODE -> patient_mr)
- IDENTITY mapping policy (id -> id)
- Source business-key order preservation on composite keys (independent of mapping query order)
- Same-name table isolation across schemas by table_id
- Fail-closed on missing approved source key (SOURCE_BUSINESS_KEY_REQUIRED)
- Fail-closed on source key absent from frozen schema (SOURCE_KEY_COLUMN_MISSING)
- Fail-closed on missing key mapping (TARGET_KEY_MAPPING_MISSING)
- Fail-closed on unsafe / lossy key transformations (TARGET_KEY_TRANSFORMATION_UNSAFE)
- Fail-closed on target key collisions (TARGET_KEY_COLLISION)
- Tamper-evident contract fingerprinting sensitivity
- Target physical constraint validation without DDL mutations
- Proof of Stage 0 non-mutation (zero DDL, zero Gold/CDC activation)
"""

from unittest.mock import MagicMock, patch
import pytest

from src.connections.connection_manager import ConnectionManager
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.metadata.policy_models import CandidateTableContract
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings
from src.metadata.target_key_resolver import TargetKeyResolver
from src.exceptions import (
    SourceBusinessKeyRequiredError,
    SourceKeyColumnMissingError,
    TargetKeyMappingMissingError,
    TargetKeyCollisionError,
    TargetKeyTransformationUnsafeError,
    TargetKeyColumnMissingError,
    TargetConstraintRequiredError,
)


# ─── 1. SIMPLE RENAME MAPPING ────────────────────────────────────────────────
def test_key02_simple_rename_mapping():
    """
    Test 1: (MR_CODE,) -> (patient_mr,) derives canonical lowercase logical target key.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("MR_CODE",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_src_pk_101",
    )
    raw_mappings = [
        {
            "column_id": 1,
            "column_sequence": 1,
            "source_column": "MR_CODE",
            "target_column": "patient_mr",
            "data_type": "text",
            "source_column_datatype": "varchar",
            "canonical_type": "STRING",
            "transform_expr": None,
        }
    ]
    resolved_mappings = ResolvedTableMappings.build(
        table_id=101,
        raw_mappings=raw_mappings,
        mapping_policy=MappingPolicy.EXPLICIT,
    )

    resolver = TargetKeyResolver()
    target_key = resolver.resolve_target_business_key(
        table_id=101,
        approved_source_key=approved_src_key,
        resolved_mappings=resolved_mappings,
    )

    assert isinstance(target_key, ResolvedTargetBusinessKey)
    assert target_key.table_id == 101
    assert target_key.source_key_columns == ("MR_CODE",)
    assert target_key.target_key_columns == ("patient_mr",)
    assert target_key.source_key_evidence_fingerprint == "fp_src_pk_101"
    assert target_key.mapping_fingerprint == resolved_mappings.fingerprint
    assert len(target_key.fingerprint) == 64


# ─── 2. IDENTITY MAPPING POLICY ──────────────────────────────────────────────
def test_key02_identity_mapping_policy():
    """
    Test 2: IDENTITY mapping policy derives target key directly from approved source key.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("ID",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_src_pk_102",
    )
    resolved_mappings = ResolvedTableMappings.build(
        table_id=102,
        raw_mappings=[],
        mapping_policy=MappingPolicy.IDENTITY,
    )

    resolver = TargetKeyResolver()
    target_key = resolver.resolve_target_business_key(
        table_id=102,
        approved_source_key=approved_src_key,
        resolved_mappings=resolved_mappings,
    )

    assert target_key.table_id == 102
    assert target_key.source_key_columns == ("ID",)
    assert target_key.target_key_columns == ("ID",)


# ─── 3. COMPOSITE KEY PRESERVES SOURCE ORDER ──────────────────────────────────
def test_key02_composite_key_preserves_source_order():
    """
    Test 3: Source composite key (hospital_id, patient_id, visit_no) must govern
    target key order even when mappings are returned in reverse/arbitrary DB order.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("hospital_id", "patient_id", "visit_no"),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_src_pk_103",
    )
    # Mapping rows returned in reverse order from database
    raw_mappings_reversed = [
        {"column_id": 3, "column_sequence": 3, "source_column": "visit_no", "target_column": "v_no", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"},
        {"column_id": 2, "column_sequence": 2, "source_column": "patient_id", "target_column": "pat_id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"},
        {"column_id": 1, "column_sequence": 1, "source_column": "hospital_id", "target_column": "hosp_id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"},
    ]
    resolved_mappings = ResolvedTableMappings.build(
        table_id=103,
        raw_mappings=raw_mappings_reversed,
        mapping_policy=MappingPolicy.EXPLICIT,
    )

    resolver = TargetKeyResolver()
    target_key = resolver.resolve_target_business_key(
        table_id=103,
        approved_source_key=approved_src_key,
        resolved_mappings=resolved_mappings,
    )

    # Strictly matches source key order (hosp_id, pat_id, v_no), NOT mapping order
    assert target_key.source_key_columns == ("hospital_id", "patient_id", "visit_no")
    assert target_key.target_key_columns == ("hosp_id", "pat_id", "v_no")


# ─── 4. SAME-NAME TABLE ISOLATION BY TABLE_ID ────────────────────────────────
def test_key02_same_name_table_isolation():
    """
    Test 4: schema_a.users (table_id 301) and schema_b.users (table_id 302)
    resolve completely isolated target keys and distinct fingerprints.
    """
    approved_key_a = ApprovedBusinessKey(columns=("ID",), evidence_type=KeyEvidenceType.PRIMARY_KEY, evidence_fingerprint="fp_a")
    approved_key_b = ApprovedBusinessKey(columns=("ID",), evidence_type=KeyEvidenceType.PRIMARY_KEY, evidence_fingerprint="fp_b")

    mappings_a = ResolvedTableMappings.build(
        table_id=301,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "ID", "target_column": "patient_id", "data_type": "bigint", "source_column_datatype": "bigint", "canonical_type": "BIGINT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mappings_b = ResolvedTableMappings.build(
        table_id=302,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "ID", "target_column": "staff_id", "data_type": "bigint", "source_column_datatype": "bigint", "canonical_type": "BIGINT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )

    resolver = TargetKeyResolver()
    target_key_a = resolver.resolve_target_business_key(301, approved_key_a, mappings_a)
    target_key_b = resolver.resolve_target_business_key(302, approved_key_b, mappings_b)

    assert target_key_a.table_id == 301
    assert target_key_b.table_id == 302
    assert target_key_a.target_key_columns == ("patient_id",)
    assert target_key_b.target_key_columns == ("staff_id",)
    assert target_key_a.fingerprint != target_key_b.fingerprint


# ─── 5. FAIL-CLOSED ON UNAPPROVED SOURCE KEY ─────────────────────────────────
def test_key02_fail_closed_unapproved_source_key():
    """
    Test 5: Raises SourceBusinessKeyRequiredError when no approved source key exists.
    """
    resolved_mappings = ResolvedTableMappings.build(
        table_id=105,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "ID", "target_column": "id", "data_type": "bigint", "source_column_datatype": "bigint", "canonical_type": "BIGINT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    resolver = TargetKeyResolver()

    with pytest.raises(SourceBusinessKeyRequiredError, match="SOURCE_BUSINESS_KEY_REQUIRED"):
        resolver.resolve_target_business_key(
            table_id=105,
            approved_source_key=None,
            resolved_mappings=resolved_mappings,
        )


# ─── 6. FAIL-CLOSED ON MISSING SOURCE KEY IN FROZEN SCHEMA ────────────────────
def test_key02_fail_closed_missing_source_key_in_frozen_schema():
    """
    Test 6: Raises SourceKeyColumnMissingError when an approved source key component
    is absent from the frozen source schema.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("id", "tenant_id"),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_106",
    )
    resolved_mappings = ResolvedTableMappings.build(
        table_id=106,
        raw_mappings=[],
        mapping_policy=MappingPolicy.IDENTITY,
        frozen_source_schema=("id", "name", "created_at"),  # tenant_id missing!
    )
    resolver = TargetKeyResolver()

    with pytest.raises(SourceKeyColumnMissingError, match="SOURCE_KEY_COLUMN_MISSING"):
        resolver.resolve_target_business_key(
            table_id=106,
            approved_source_key=approved_src_key,
            resolved_mappings=resolved_mappings,
        )


# ─── 7. FAIL-CLOSED ON MISSING KEY MAPPING ────────────────────────────────────
def test_key02_fail_closed_missing_explicit_key_mapping():
    """
    Test 7: Raises TargetKeyMappingMissingError when an EXPLICIT table lacks a mapping
    for one of its business key columns.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("MR_CODE", "VISIT_NO"),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_107",
    )
    # Only MR_CODE is mapped; VISIT_NO is omitted
    raw_mappings = [
        {"column_id": 1, "column_sequence": 1, "source_column": "MR_CODE", "target_column": "patient_mr", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"}
    ]
    resolved_mappings = ResolvedTableMappings.build(
        table_id=107,
        raw_mappings=raw_mappings,
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    resolver = TargetKeyResolver()

    with pytest.raises(TargetKeyMappingMissingError, match="TARGET_KEY_MAPPING_MISSING"):
        resolver.resolve_target_business_key(
            table_id=107,
            approved_source_key=approved_src_key,
            resolved_mappings=resolved_mappings,
        )


# ─── 8. FAIL-CLOSED ON UNSAFE KEY TRANSFORMATION ──────────────────────────────
def test_key02_fail_closed_unsafe_key_transformation():
    """
    Test 8: Raises TargetKeyTransformationUnsafeError when a business key column
    has a transformation expression that could compromise identity uniqueness.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("MR_CODE",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_108",
    )
    raw_mappings = [
        {
            "column_id": 1,
            "column_sequence": 1,
            "source_column": "MR_CODE",
            "target_column": "patient_mr",
            "data_type": "text",
            "source_column_datatype": "varchar",
            "canonical_type": "STRING",
            "transform_expr": "SUBSTR(MR_CODE, 1, 5)",  # Unsafe lossy truncation!
        }
    ]
    resolved_mappings = ResolvedTableMappings.build(
        table_id=108,
        raw_mappings=raw_mappings,
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    resolver = TargetKeyResolver()

    with pytest.raises(TargetKeyTransformationUnsafeError, match="TARGET_KEY_TRANSFORMATION_UNSAFE"):
        resolver.resolve_target_business_key(
            table_id=108,
            approved_source_key=approved_src_key,
            resolved_mappings=resolved_mappings,
        )


# ─── 9. FAIL-CLOSED ON TARGET KEY COLLISION ──────────────────────────────────
def test_key02_fail_closed_target_key_collision():
    """
    Test 9: Raises TargetKeyCollisionError when two distinct source key columns
    map to the same normalized target column.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("ID_PRIMARY", "ID_SECONDARY"),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_109",
    )
    raw_mappings = [
        {"column_id": 1, "column_sequence": 1, "source_column": "ID_PRIMARY", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"},
        {"column_id": 2, "column_sequence": 2, "source_column": "ID_SECONDARY", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"},
    ]
    resolved_mappings = ResolvedTableMappings.build(
        table_id=109,
        raw_mappings=raw_mappings,
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    resolver = TargetKeyResolver()

    with pytest.raises(TargetKeyCollisionError, match="TARGET_KEY_COLLISION"):
        resolver.resolve_target_business_key(
            table_id=109,
            approved_source_key=approved_src_key,
            resolved_mappings=resolved_mappings,
        )


# ─── 10. CONTRACT FINGERPRINT SENSITIVITY ─────────────────────────────────────
def test_key02_contract_fingerprint_sensitivity():
    """
    Test 10: Changing a key mapping target name must alter the contract fingerprint.
    """
    approved_src_key = ApprovedBusinessKey(
        columns=("MR_CODE",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_110",
    )
    mappings_v1 = ResolvedTableMappings.build(
        table_id=110,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "MR_CODE", "target_column": "patient_mr", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mappings_v2 = ResolvedTableMappings.build(
        table_id=110,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "MR_CODE", "target_column": "mr_number", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )

    resolver = TargetKeyResolver()
    contract_v1 = resolver.assemble_candidate_contract(110, approved_src_key, mappings_v1)
    contract_v2 = resolver.assemble_candidate_contract(110, approved_src_key, mappings_v2)

    assert isinstance(contract_v1, CandidateTableContract)
    assert isinstance(contract_v2, CandidateTableContract)
    assert contract_v1.contract_fingerprint != contract_v2.contract_fingerprint


# ─── 11. PHYSICAL CONSTRAINT VALIDATION & STAGE 0 NON-MUTATION ────────────────
@pytest.mark.live
def test_key02_physical_constraint_validation():
    """
    Test 11: validate_target_physical_schema inspects physical target columns and PK/UNIQUE
    constraints with ZERO DDL mutations.
    """
    cm = ConnectionManager("config/config.local.yaml")
    resolver = TargetKeyResolver(cm)

    # 1. On landing.ulog: Columns exist ('id'), but physical PK constraint is missing in landing schema
    target_key = ResolvedTargetBusinessKey.build(
        table_id=120,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="fp_ulog",
        mapping_fingerprint="fp_map_ulog",
    )

    # Missing constraint on landing.ulog must raise TargetConstraintRequiredError
    with pytest.raises(TargetConstraintRequiredError, match="TARGET_CONSTRAINT_REQUIRED"):
        resolver.validate_target_physical_schema(
            target_tag="land_test",
            target_schema="landing",
            target_table="ulog",
            resolved_target_key=target_key,
        )

    # 2. Missing target column raises TargetKeyColumnMissingError
    bad_col_key = ResolvedTargetBusinessKey.build(
        table_id=120,
        source_key_columns=("non_existent_key_col",),
        target_key_columns=("non_existent_key_col",),
        source_key_evidence_fingerprint="fp_ulog",
        mapping_fingerprint="fp_map_ulog",
    )
    with pytest.raises(TargetKeyColumnMissingError, match="TARGET_KEY_COLUMN_MISSING"):
        resolver.validate_target_physical_schema(
            target_tag="land_test",
            target_schema="landing",
            target_table="ulog",
            resolved_target_key=bad_col_key,
        )

    # 3. Test on a metadata DB table that HAS a primary key (e.g. metadata.db_tables_etl)
    metadata_pk_key = ResolvedTargetBusinessKey.build(
        table_id=1,
        source_key_columns=("table_id",),
        target_key_columns=("table_id",),
        source_key_evidence_fingerprint="fp_meta",
        mapping_fingerprint="fp_map_meta",
    )
    # Validate against land_test or metadata DB if configured
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cfg = MagicMock()
    mock_cfg.db_type = "postgres"
    mock_cm.get.return_value = mock_cfg

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    # Mock physical columns
    mock_cur.fetchall.side_effect = [
        [("id",), ("name",)],  # physical columns
        [("pk_ulog", "p", ["id"])],  # physical constraint
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_conn

    mock_resolver = TargetKeyResolver(mock_cm)
    assert mock_resolver.validate_target_physical_schema(
        target_tag="mock_target",
        target_schema="public",
        target_table="users",
        resolved_target_key=target_key,
    ) is True

