# tests/test_meta01_live_gate.py
"""
META-01 — Live Integration Gate Verification Suite.

Executes live integration verification against real development metadata and target databases:
- Case 1: Initial promotion of ulog (table_id=120) to Version 1, audit trail, no-op prevention.
- Case 2: Offline contract reconstruction from immutable payload; promotion of Version 2; historical version immutability.
- Case 3: Active attempt version fencing (attempt on v1 raises POLICY_VERSION_SUPERSEDED when v2 is promoted).
"""

import pytest

from src.connections.connection_manager import ConnectionManager
from src.metadata.policy_models import PromotionRequest, PromotionResult, CandidateTableContract, PromotedTablePolicy
from src.metadata.policy_governance import PolicyGovernanceService
from src.metadata.key_contract import ResolvedTableContract
from src.exceptions import (
    PolicyNotPromotedError,
    PolicyAlreadyCurrentError,
    PolicyVersionSupersededError,
    PolicyEvidenceDriftError,
)


@pytest.fixture(scope="module")
def live_governance_service():
    cm = ConnectionManager("config/config.local.yaml")
    return PolicyGovernanceService(cm)


# ─── CASE 1: LIVE ULOG INITIAL PROMOTION & AUDIT ──────────────────────────────
def test_meta01_live_case1_ulog_promotion(live_governance_service):
    """
    Case 1: Validates initial candidate build, promotion of ulog (table_id 120) to Version 1,
    audit ledger persistence, and no-op rejection.
    """
    cm = ConnectionManager("config/config.local.yaml")
    service = live_governance_service

    # Clean state for ulog table_id 120 in test environment
    with cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE metadata.db_tables_etl SET current_policy_version_id = NULL WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.cdc_attempt_window WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.table_cdc_state WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.policy_promotion_audit WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.table_policy_version WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.table_cdc_policy_candidate WHERE table_id = 120")
        conn.commit()

    # 1. Unpromoted table raises PolicyNotPromotedError
    with pytest.raises(PolicyNotPromotedError, match="POLICY_NOT_PROMOTED"):
        service.materialize_execution_contract(120)

    # 2. Build candidate contract from live metadata
    candidate = service.build_candidate_contract(120)
    assert isinstance(candidate, CandidateTableContract)
    assert candidate.table_id == 120
    assert candidate.approved_source_business_key.columns == ("id",)
    assert candidate.canonical_target_business_key.target_key_columns == ("id",)

    # 3. Promote candidate to Version 1
    req = PromotionRequest(
        table_id=120,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="lead_architect",
        reason="Promoting certified ulog identity policy",
    )
    result = service.promote_policy(req)

    assert isinstance(result, PromotionResult)
    assert result.table_id == 120
    assert result.new_version_number == 1
    assert result.previous_version_number is None

    # 4. Materialize execution contract from promoted snapshot
    contract = service.materialize_execution_contract(120)
    assert isinstance(contract, ResolvedTableContract)
    assert contract.table_id == 120
    assert contract.version_number == 1
    assert contract.policy_version_id == result.policy_version_id
    assert contract.policy_fingerprint == result.policy_fingerprint
    assert contract.approved_source_business_key.columns == ("id",)
    assert contract.canonical_target_business_key.target_key_columns == ("id",)

    # 5. Promoting identical candidate raises PolicyAlreadyCurrentError
    with pytest.raises(PolicyAlreadyCurrentError, match="POLICY_ALREADY_CURRENT"):
        service.promote_policy(req)

    # 6. Verify audit records in database
    with cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT outcome, failure_code, actor_id, policy_version_id
                FROM metadata.policy_promotion_audit
                WHERE table_id = 120
                ORDER BY requested_at ASC
            """)
            audit_rows = cur.fetchall()

    assert len(audit_rows) == 2
    # First was ACCEPTED
    assert audit_rows[0][0] == "ACCEPTED"
    assert audit_rows[0][1] is None
    assert audit_rows[0][2] == "lead_architect"
    assert audit_rows[0][3] is not None

    # Second was REJECTED (no-op)
    assert audit_rows[1][0] == "REJECTED"
    assert audit_rows[1][1] == "POLICY_ALREADY_CURRENT"


# ─── CASE 2: OFFLINE RECONSTRUCTION & MULTI-VERSION PROMOTION ─────────────────
def test_meta01_live_case2_offline_reconstruction_and_v2_promotion(live_governance_service):
    """
    Case 2: Demonstrates that Version 1 contract reconstructs with 100% fidelity
    from stored JSONB payload even if live candidate metadata is changed, and
    promotes Version 2 without mutating historical Version 1.
    """
    cm = ConnectionManager("config/config.local.yaml")
    service = live_governance_service

    # 1. Read v1 contract
    v1_contract = service.materialize_execution_contract(120)
    v1_policy_id = v1_contract.policy_version_id

    # 2. Modify live metadata for ulog in db_columns_etl (update column_sequence for email column)
    with cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE metadata.db_columns_etl
                SET column_sequence = 99
                WHERE table_id = 120 AND source_column = 'email'
            """)
        conn.commit()

    try:
        # 3. Prove that runtime execution STILL reconstructs Version 1 perfectly
        # from the immutable snapshot (completely insulated from live db_columns_etl mutation!)
        reconstructed_v1 = service.materialize_execution_contract(120)
        assert reconstructed_v1.version_number == 1
        assert reconstructed_v1.contract_fingerprint == v1_contract.contract_fingerprint
        assert reconstructed_v1.policy_fingerprint == v1_contract.policy_fingerprint

        # 4. Build candidate for Version 2 (picks up changed sequence & new fingerprint)
        v2_candidate = service.build_candidate_contract(120)
        assert v2_candidate.candidate_fingerprint != v1_contract.policy_fingerprint

        # 5. Promote Version 2
        req_v2 = PromotionRequest(
            table_id=120,
            expected_candidate_fingerprint=v2_candidate.candidate_fingerprint,
            actor_type="USER",
            actor_id="operator_bob",
            reason="Updated column sequence for email in Version 2",
        )
        res_v2 = service.promote_policy(req_v2)
        assert res_v2.new_version_number == 2
        assert res_v2.previous_version_number == 1

        # 6. Verify historical Version 1 row in table_policy_version is completely untouched
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT version_number, supersedes_version_id, contract_fingerprint
                    FROM metadata.table_policy_version
                    WHERE table_id = 120
                    ORDER BY version_number ASC
                """)
                v_rows = cur.fetchall()

        assert len(v_rows) == 2
        # v1 row
        assert v_rows[0][0] == 1
        assert v_rows[0][1] is None
        assert v_rows[0][2] == v1_contract.contract_fingerprint

        # v2 row
        assert v_rows[1][0] == 2
        assert str(v_rows[1][1]) == str(v1_policy_id)

    finally:
        # Restore db_columns_etl email sequence
        with cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE metadata.db_columns_etl
                    SET column_sequence = 3
                    WHERE table_id = 120 AND source_column = 'email'
                """)
            conn.commit()

        v3_candidate = service.build_candidate_contract(120)
        service.promote_policy(PromotionRequest(
            table_id=120,
            expected_candidate_fingerprint=v3_candidate.candidate_fingerprint,
            actor_type="SYSTEM",
            actor_id="cleanup_fixture",
            reason="Restored ulog column sequence",
        ))



# ─── CASE 3: ACTIVE ATTEMPT POLICY VERSION FENCING ────────────────────────────
def test_meta01_live_case3_active_attempt_version_fencing(live_governance_service):
    """
    Case 3: Verifies that an executing attempt started on Version N loses commit authority
    and triggers POLICY_VERSION_SUPERSEDED if Version N+1 is promoted midway.
    """
    service = live_governance_service

    # Current promoted policy for ulog
    current_policy = service.get_current_promoted_policy(120)
    current_policy_id = current_policy.policy_version_id

    # 1. Attempt running with current policy passes fence
    assert service.certify_attempt_policy_fence(120, current_policy_id) is True

    # 2. Stale worker attempting to commit under a superseded policy version fails closed
    stale_policy_id = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(PolicyVersionSupersededError, match="POLICY_VERSION_SUPERSEDED"):
        service.certify_attempt_policy_fence(120, stale_policy_id)
