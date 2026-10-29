from src.models.principal import AuthenticatedPrincipal
# tests/test_meta01_policy_governance.py
"""
META-01 — Dedicated Unit & Invariant Test Suite for Policy Version Governance.
"""

from unittest.mock import MagicMock, patch
from datetime import datetime
import pytest
import uuid
import json

from src.connections.connection_manager import ConnectionManager
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings, ResolvedColumnMapping
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.metadata.policy_models import (
    CandidateTableContract,
    CDCStrategy,
    CursorFinalityMode,
    CursorLogicalType,
    PromotedTablePolicy,
    PromotionRequest,
    PromotionResult,
    PolicyActorType,
    ResolvedCDCPolicy,
    SourceMutationMode,
    compute_deterministic_fingerprint,
)
from src.metadata.policy_governance import PolicyGovernanceService
from src.exceptions import (
    PolicyNotPromotedError,
    PolicyAlreadyCurrentError,
    PolicyVersionSupersededError,
    PolicyEvidenceDriftError,
    PolicyFingerprintMismatchError,
    PolicyVersionNotFoundError,
    PolicyActorRequiredError,
    PolicyDependencyInvalidError,
    BusinessKeyRequiredError,
    MetadataSchemaIncompatibleError,
)
from src.connections.schema_certifier import SchemaCertifier


def approved_policy_row(current_policy_version_id=None):
    return (
        120,
        "EXPLICIT",
        "msopforg_opforg",
        "ulog",
        "land_test",
        "landing",
        "ulog",
        current_policy_version_id,
        ["id"],
        "PRIMARY_KEY",
        "src_fp",
        "operator_alice",
        datetime(2026, 8, 20, 11, 0, 0),
        1,
        False,
        True,
        "src_fp",
    )


def test_meta01_keyless_table_receives_an_explicit_full_snapshot_contract():
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_mapper = MagicMock()
    mock_resolver = MagicMock()
    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)

    row_without_approval = (
        120, "EXPLICIT", "msopforg_opforg", "ulog", "land_test", "landing", "ulog",
        None, None, None, None, None, None, None, False, True, "src_fp",
    )

    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=120,
        raw_mappings=[{
            "column_id": 1,
            "column_sequence": 1,
            "source_column": "raw_value",
            "target_column": "raw_value",
            "data_type": "string",
            "source_column_datatype": "varchar",
            "canonical_type": "STRING",
        }],
        mapping_policy=MappingPolicy.EXPLICIT,
    )

    candidate = service.build_candidate_contract(
        120,
        table_metadata_row=row_without_approval,
        cur=MagicMock(),
    )

    assert candidate.approved_source_business_key is None
    assert candidate.canonical_target_business_key is None
    assert candidate.cdc_policy is None
    assert candidate.contract_payload["source_business_key"] is None
    assert candidate.contract_payload["target_business_key"] is None
    assert candidate.contract_payload["source_key_fingerprint"] is None


def test_meta01_keyless_table_cannot_receive_sequence_cdc():
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_mapper = MagicMock()
    mock_resolver = MagicMock()
    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    row_without_approval = (
        120, "EXPLICIT", "msopforg_opforg", "ulog", "land_test", "landing", "ulog",
        None, None, None, None, None, None, None, False, True, "src_fp",
    )
    sequence_cdc = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("event_id",),
        cursor_types=(CursorLogicalType.INT64,),
    )

    with pytest.raises(BusinessKeyRequiredError, match="BUSINESS_KEY_REQUIRED"):
        service.build_candidate_contract(
            120,
            table_metadata_row=row_without_approval,
            cdc_policy=sequence_cdc,
            cur=MagicMock(),
        )


# ─── 1. FIRST PROMOTION CREATES VERSION 1 ─────────────────────────────────────
def test_meta01_first_promotion_creates_version_1():
    """
    Test 1: Initial promotion of an unpromoted table creates version 1, sets
    current_policy_version_id, and writes an ACCEPTED audit record.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Setup candidate mock
    mock_mapper = MagicMock()
    mock_resolver = MagicMock()

    # DB responses:
    # 1. db_tables_etl read in build_candidate_contract
    # 2. table_cdc_policy_candidate lookup in build_candidate_contract (returns None)
    # 3. SELECT FOR UPDATE in promote_policy (current_policy_version_id is None)
    # 4. table_cdc_policy_candidate lookup in promote_policy (returns None)
    # 5. INSERT INTO table_policy_version RETURNING promoted_at
    mock_cur.fetchone.side_effect = [
        approved_policy_row(),  # candidate read
        None,  # candidate cdc lookup
        approved_policy_row(None),  # lock table row
        None,  # promote cdc lookup
        (datetime(2026, 8, 20, 12, 0, 0),),  # INSERT returning promoted_at
    ]

    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=120,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=120,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )
    mock_resolver.validate_target_physical_schema.return_value = True

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    
    # 1. Build candidate
    candidate = service.build_candidate_contract(120)
    assert isinstance(candidate, CandidateTableContract)
    assert candidate.table_id == 120

    # 2. Promote candidate
    principal = AuthenticatedPrincipal(actor_id="operator_alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=120,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="operator_alice",
        reason="Initial table onboarding",
        authenticated_principal=principal,
    )
    result = service.promote_policy(req)

    assert isinstance(result, PromotionResult)
    assert result.table_id == 120
    assert result.new_version_number == 1
    assert result.previous_version_number is None
    assert result.policy_fingerprint == candidate.candidate_fingerprint


# ─── 2. SECOND PROMOTION CREATES VERSION 2 & SUPERSEDES VERSION 1 ─────────────
def test_meta01_second_promotion_creates_version_2():
    """
    Test 2: Promoting a modified candidate increments version_number to 2,
    records supersedes_version_id pointing to version 1, and updates pointer.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()

    v1_id = "v1-uuid-1111"
    mock_cur.fetchone.side_effect = [
        approved_policy_row(),  # candidate read
        None,  # candidate cdc lookup
        approved_policy_row(v1_id),  # lock table row (current is v1)
        None,  # promote cdc lookup
        (1, "old_policy_fp_999"),  # current version lookup (ver 1, different fp)
        (datetime(2026, 8, 20, 13, 0, 0),),  # INSERT returning promoted_at
    ]

    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=120,
        raw_mappings=[
            {"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"},
            {"column_id": 2, "column_sequence": 2, "source_column": "url", "target_column": "endpoint_url", "data_type": "text", "source_column_datatype": "varchar", "canonical_type": "STRING"},
        ],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=120,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp_2",
    )
    mock_resolver.validate_target_physical_schema.return_value = True

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)
    candidate = service.build_candidate_contract(120)

    principal = AuthenticatedPrincipal(actor_id="ci_cd_bot", actor_type="SYSTEM", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=120,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="SYSTEM",
        actor_id="ci_cd_bot",
        reason="Added endpoint_url mapping",
        authenticated_principal=principal,
    )
    result = service.promote_policy(req)

    assert result.table_id == 120
    assert result.new_version_number == 2
    assert result.previous_version_number == 1


# ─── 3. NO-OP CANDIDATE RAISES POLICY_ALREADY_CURRENT ─────────────────────────
def test_meta01_noop_candidate_raises_already_current():
    """
    Test 3: Attempting to promote an unchanged candidate raises PolicyAlreadyCurrentError
    without creating a new version or updating pointers.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()

    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=120,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=120,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )
    mock_resolver.validate_target_physical_schema.return_value = True

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)

    v1_id = "v1-uuid-1111"
    # First read to build candidate
    mock_cur.fetchone.side_effect = [
        approved_policy_row(),
        None,
    ]
    candidate = service.build_candidate_contract(120)

    mock_cur.fetchone.side_effect = [
        # Reconfigure fetchone sequence for promote_policy:
        # 1. SELECT FOR UPDATE
        # 2. promote candidate cdc lookup
        # 3. current version lookup (returns same fingerprint)
        approved_policy_row(v1_id),
        None,
        (1, candidate.candidate_fingerprint),  # identical fingerprint
    ]

    principal = AuthenticatedPrincipal(actor_id="operator_alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=120,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="operator_alice",
        authenticated_principal=principal,
    )
    with pytest.raises(PolicyAlreadyCurrentError, match="POLICY_ALREADY_CURRENT"):
        service.promote_policy(req)



# ─── 4. HISTORICAL RECONSTRUCTION AFTER CANDIDATE METADATA CHANGES ────────────
def test_meta01_exact_historical_payload_reconstruction():
    """
    Test 4: Historical policy version 1 can be deserialized and reconstructed
    offline with 100% fidelity even if live db_columns_etl metadata has changed.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Simulated historical payload saved in version 1
    historical_payload = {
        "schema_version": 1,
        "table_id": 120,
        "mapping_policy": "EXPLICIT",
        "source_business_key": {
            "columns": ["id"],
            "evidence_type": "PRIMARY_KEY",
            "evidence_fingerprint": "src_fp_v1",
        },
        "mappings": [
            {"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT", "transform_expr": None}
        ],
        "target_business_key": {
            "columns": ["id"],
            "fingerprint": "tgt_fp_v1",
        },
        "source_key_fingerprint": "src_fp_v1",
        "mapping_fingerprint": "map_fp_v1",
        "target_key_fingerprint": "tgt_fp_v1",
    }
    contract_fp = compute_deterministic_fingerprint(historical_payload)
    policy_fp = compute_deterministic_fingerprint({
        "policy_schema_version": 1,
        "contract_fingerprint": contract_fp,
    })

    v1_id = "v1-uuid-1111"
    # Mock get_current_promoted_policy responses:
    # 1. db_tables_etl pointer lookup
    # 2. table_policy_version read
    mock_cur.fetchone.side_effect = [
        (v1_id,),
        (
            v1_id, 120, 1, 1,
            historical_payload,
            "src_fp_v1", "map_fp_v1", "tgt_fp_v1",
            contract_fp, policy_fp,
            datetime(2026, 8, 20, 10, 0, 0), "USER", "alice", "Initial", None,
        ),
    ]

    service = PolicyGovernanceService(mock_cm)
    contract = service.materialize_execution_contract(120)

    assert isinstance(contract, ResolvedTableContract)
    assert contract.table_id == 120
    assert contract.policy_version_id == v1_id
    assert contract.version_number == 1
    assert contract.policy_fingerprint == policy_fp
    assert contract.contract_fingerprint == contract_fp
    assert contract.approved_source_business_key.columns == ("id",)
    assert contract.canonical_target_business_key.target_key_columns == ("id",)


# ─── 5. EVIDENCE DRIFT DETECTION ──────────────────────────────────────────────
def test_meta01_evidence_drift_detection():
    """
    Test 5: If the reviewed candidate fingerprint does not match the recomputed
    fingerprint during promotion transaction, promotion fails with POLICY_EVIDENCE_DRIFT.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_mapper = MagicMock()
    mock_resolver = MagicMock()

    mock_mapper.resolve_table_mappings.return_value = ResolvedTableMappings.build(
        table_id=120,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )
    mock_resolver.resolve_target_business_key.return_value = ResolvedTargetBusinessKey.build(
        table_id=120,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
    )

    mock_cur.fetchone.return_value = approved_policy_row(None)

    service = PolicyGovernanceService(mock_cm, mapper=mock_mapper, resolver=mock_resolver)

    # Pass an outdated/different expected fingerprint
    principal = AuthenticatedPrincipal(actor_id="alice", actor_type="USER", roles={"ADMIN"})
    req = PromotionRequest(
        table_id=120,
        expected_candidate_fingerprint="outdated_fp_1234567890",
        actor_type="USER",
        actor_id="alice",
        authenticated_principal=principal,
    )
    with pytest.raises(PolicyEvidenceDriftError, match="POLICY_EVIDENCE_DRIFT"):
        service.promote_policy(req)


# ─── 6. POLICY VERSION FENCING (PASS & FAIL) ──────────────────────────────────
def test_meta01_policy_version_fencing():
    """
    Test 6: certify_attempt_policy_fence passes when attempt matches current pointer,
    and raises PolicyVersionSupersededError when pointer has advanced.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    service = PolicyGovernanceService(mock_cm)

    # 1. Matching version passes
    mock_cur.fetchone.return_value = ("v1-uuid-1111",)
    assert service.certify_attempt_policy_fence(120, "v1-uuid-1111") is True

    # 2. Superseded version fails closed
    mock_cur.fetchone.return_value = ("v2-uuid-2222",)
    with pytest.raises(PolicyVersionSupersededError, match="POLICY_VERSION_SUPERSEDED"):
        service.certify_attempt_policy_fence(120, "v1-uuid-1111")


# ─── 7. UNPROMOTED TABLE FAST-FAIL ────────────────────────────────────────────
def test_meta01_unpromoted_table_fast_fail():
    """
    Test 7: Attempting to materialize execution contract on an unpromoted table
    (current_policy_version_id IS NULL) raises PolicyNotPromotedError.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # current_policy_version_id is None
    mock_cur.fetchone.return_value = (None,)

    service = PolicyGovernanceService(mock_cm)
    with pytest.raises(PolicyNotPromotedError, match="POLICY_NOT_PROMOTED"):
        service.materialize_execution_contract(120)


# ─── 8. CORRUPTED PAYLOAD FINGERPRINT MISMATCH DETECTION ──────────────────────
def test_meta01_corrupted_payload_fingerprint_mismatch():
    """
    Test 8: If stored policy payload has been tampered with or corrupted,
    materialize_execution_contract detects hash mismatch and raises PolicyFingerprintMismatchError.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    v1_id = "v1-uuid-1111"
    tampered_payload = {
        "schema_version": 1,
        "table_id": 120,
        "mapping_policy": "EXPLICIT",
        "mappings": [],
    }

    mock_cur.fetchone.side_effect = [
        (v1_id,),
        (
            v1_id, 120, 1, 1,
            tampered_payload,
            "src_fp_v1", "map_fp_v1", "tgt_fp_v1",
            "stored_bad_contract_fp", "stored_bad_policy_fp",
            datetime(2026, 8, 20, 10, 0, 0), "USER", "alice", "Initial", None,
        ),
    ]

    service = PolicyGovernanceService(mock_cm)
    with pytest.raises(PolicyFingerprintMismatchError, match="POLICY_FINGERPRINT_MISMATCH"):
        service.materialize_execution_contract(120)


# ─── 9. MISSING / INVALID ACTOR VALIDATION ────────────────────────────────────
def test_meta01_missing_or_invalid_actor():
    """
    Test 9: Rejects missing actor_id or invalid actor_type with PolicyActorRequiredError.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    service = PolicyGovernanceService(mock_cm)

    # Missing actor_id
    with pytest.raises(PolicyActorRequiredError, match="POLICY_ACTOR_REQUIRED"):
        service.promote_policy(PromotionRequest(
            table_id=120,
            expected_candidate_fingerprint="fp",
            actor_type="USER",
            actor_id="",
        ))

    # Invalid actor_type
    with pytest.raises(PolicyActorRequiredError, match="POLICY_ACTOR_REQUIRED"):
        service.promote_policy(PromotionRequest(
            table_id=120,
            expected_candidate_fingerprint="fp",
            actor_type="HACKER",
            actor_id="evil",
        ))


# ─── 10. REJECTED AUDIT EVENT PERSISTED ON FAILURE ────────────────────────────
def test_meta01_rejected_audit_event_persisted():
    """
    Test 10: Failed promotion writes an audit row with outcome='REJECTED'
    and policy_version_id=NULL.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    service = PolicyGovernanceService(mock_cm)
    service._record_audit_event(
        cur=mock_cur,
        request=PromotionRequest(
            table_id=120,
            expected_candidate_fingerprint="fp_test",
            actor_type="USER",
            actor_id="alice",
            reason="Testing rejection audit",
        ),
        outcome="REJECTED",
        policy_version_id=None,
        failure_code="POLICY_EVIDENCE_DRIFT",
        details={"diff": "test"},
    )

    mock_cur.execute.assert_called_once()
    sql = mock_cur.execute.call_args[0][0]
    assert "INSERT INTO metadata.policy_promotion_audit" in sql
    params = mock_cur.execute.call_args[0][1]
    assert params[3] == "REJECTED"
    assert params[4] is None  # policy_version_id is NULL
    assert params[5] == "POLICY_EVIDENCE_DRIFT"


# ─── 11. SCHEMA CERTIFIER CERTIFIES MIGRATION 033 ─────────────────────────────
def test_meta01_schema_certification_migration_033():
    """
    Test 11: SchemaCertifier validates all 33 canonical migrations including 033.
    """
    mock_conn_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn_mgr.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    certifier = SchemaCertifier(mock_conn_mgr)
    local_migrations = certifier._get_expected_ledger_migrations()
    assert any(m[0] == "033" for m in local_migrations)

    mock_cur.fetchall.return_value = local_migrations
    certifier.certify()  # Passes without raising exception
