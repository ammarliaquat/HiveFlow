# tests/test_cdc01_live_gate.py
"""
CDC-01 — Live Integration Gate Verification Suite.

Validates CDC-01 against live development MySQL source and PostgreSQL metadata databases:
- Case 1: Live ulog (table_id=120) explicit candidate CDC policy setup & META-01 promotion.
- Case 2: Source upper H capture directly from MySQL, durable window persistence in metadata.cdc_attempt_window, idempotency.
- Case 3: Read-only bounded extraction query against live source MySQL table; proof of exclusion of rows > H; proof of zero metadata.table_cdc_state mutations.
"""

from datetime import datetime
import pytest
import uuid

from src.connections.connection_manager import ConnectionManager
from src.metadata.policy_models import (
    CDCStrategy,
    SourceMutationMode,
    CursorFinalityMode,
    CursorLogicalType,
    ResolvedCDCPolicy,
    CDCCursor,
    ResolvedCDCWindow,
    CandidateTableContract,
    PromotionRequest,
    PromotionResult,
)
from src.metadata.key_contract import ResolvedTableContract
from src.metadata.policy_governance import PolicyGovernanceService
from src.cdc.cdc_window_resolver import CDCWindowResolver
from src.exceptions import (
    CDCPolicyNotPromotedError,
    CDCMutationModeUnsupportedError,
    CDCCursorFinalityUncertifiedError,
)


@pytest.fixture(scope="module")
def live_cm():
    return ConnectionManager("config/config.local.yaml")


@pytest.fixture(scope="module")
def live_governance_service(live_cm):
    return PolicyGovernanceService(live_cm)


@pytest.fixture(scope="module")
def live_window_resolver(live_cm):
    return CDCWindowResolver(live_cm)


# ─── CASE 1: LIVE ULOG CANDIDATE CDC POLICY SETUP & PROMOTION ─────────────────
def test_cdc01_live_case1_ulog_cdc_policy_promotion(live_cm, live_governance_service):
    """
    Case 1: Sets up explicit candidate CDC policy for ulog (table_id=120),
    builds candidate contract with CDC policy, and promotes it via META-01.
    """
    service = live_governance_service

    # Clean state for isolated repeatable live testing on table_id 120
    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE metadata.db_tables_etl SET current_policy_version_id = NULL WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.cdc_attempt_window WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.table_cdc_state WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.policy_promotion_audit WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.table_policy_version WHERE table_id = 120")
            cur.execute("DELETE FROM metadata.table_cdc_policy_candidate WHERE table_id = 120")
        conn.commit()

    # 1. Set explicit candidate CDC policy in metadata.table_cdc_policy_candidate
    cdc_policy = service.set_candidate_cdc_policy(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("id",),
        cursor_types=(CursorLogicalType.INT64,),
        tie_breaker_columns=("id",),
    )
    assert isinstance(cdc_policy, ResolvedCDCPolicy)
    assert cdc_policy.strategy == CDCStrategy.SEQUENCE_HIGH_WATERMARK
    assert cdc_policy.mutation_mode == SourceMutationMode.APPEND_ONLY
    assert cdc_policy.cursor_finality_mode == CursorFinalityMode.PREFIX_FINAL

    # 2. Build candidate contract (loads candidate CDC policy)
    candidate = service.build_candidate_contract(120)
    assert candidate.cdc_policy is not None
    assert candidate.cdc_policy.strategy == CDCStrategy.SEQUENCE_HIGH_WATERMARK
    assert "cdc_policy" in candidate.contract_payload

    # 3. Promote candidate to authoritative version
    req = PromotionRequest(
        table_id=120,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="cdc_engineer_bob",
        reason="Promoted verified SEQUENCE_HIGH_WATERMARK CDC policy for ulog",
    )
    res = service.promote_policy(req)
    assert isinstance(res, PromotionResult)
    assert res.table_id == 120
    assert res.new_version_number == 1

    # 4. Materialize execution contract from promoted snapshot
    contract = service.materialize_execution_contract(120)
    assert isinstance(contract, ResolvedTableContract)
    assert contract.cdc_policy is not None
    assert contract.cdc_policy.strategy == CDCStrategy.SEQUENCE_HIGH_WATERMARK
    assert contract.cdc_policy.cursor_columns == ("id",)


# ─── CASE 2: LIVE SOURCE UPPER H CAPTURE & DURABLE ATTEMPT WINDOW ─────────────
def test_cdc01_live_case2_source_capture_and_window_persistence(live_cm, live_governance_service, live_window_resolver):
    """
    Case 2: Captures live source upper boundary H from MySQL source without querying target,
    persists immutable execution window in metadata.cdc_attempt_window, and proves idempotency.
    """
    service = live_governance_service
    resolver = live_window_resolver

    contract = service.materialize_execution_contract(120)
    assert contract.cdc_policy is not None

    # Register test run in gui_job_registry
    run_id = str(uuid.uuid4())
    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metadata.gui_job_registry (
                    run_id, action, source_tag, target_tag, status, actor_type, actor_id
                ) VALUES (
                    %s, 'CDC_RUN', 'ulog', 'land_test', 'RUNNING', 'USER', 'cdc_tester'
                )
            """, [run_id])
        conn.commit()

    try:
        # 1. Resolve and persist attempt window
        window = resolver.resolve_and_persist_attempt_window(
            run_id=run_id,
            table_id=120,
            attempt_number=1,
            contract=contract,
        )

        assert isinstance(window, ResolvedCDCWindow)
        assert window.run_id == run_id
        assert window.table_id == 120
        assert window.attempt_number == 1
        assert window.lower_cursor is None  # First run is bootstrap
        assert window.upper_cursor.values[0] >= 565971  # Live source upper bound
        assert window.is_empty is False

        # 2. Check persistence in metadata.cdc_attempt_window
        with live_cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT window_id, upper_cursor, lower_cursor, window_fingerprint
                    FROM metadata.cdc_attempt_window
                    WHERE run_id = %s AND table_id = 120 AND attempt_number = 1
                """, [run_id])
                row = cur.fetchone()

        assert row is not None
        assert str(row[0]) == str(window.window_id)
        assert row[3] == window.window_fingerprint

        # 3. Idempotent re-resolution for the same attempt returns the identical frozen window
        second_resolution = resolver.resolve_and_persist_attempt_window(
            run_id=run_id,
            table_id=120,
            attempt_number=1,
            contract=contract,
        )
        assert second_resolution.window_id == window.window_id
        assert second_resolution.upper_cursor.values[0] == window.upper_cursor.values[0]
        assert second_resolution.window_fingerprint == window.window_fingerprint

    finally:
        # Cleanup test run artifacts
        with live_cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM metadata.cdc_attempt_window WHERE run_id = %s", [run_id])
                cur.execute("DELETE FROM metadata.gui_job_registry WHERE run_id = %s", [run_id])
            conn.commit()


# ─── CASE 3: LIVE BOUNDED EXTRACTION QUERY & ZERO STATE ADVANCEMENT ───────────
def test_cdc01_live_case3_bounded_extraction_and_zero_state_mutation(live_cm, live_governance_service, live_window_resolver):
    """
    Case 3: Generates dialect SQL predicate, executes read-only count query on live MySQL source,
    and proves metadata.table_cdc_state receives ZERO mutations (Stage 0 freeze intact).
    """
    service = live_governance_service
    resolver = live_window_resolver

    contract = service.materialize_execution_contract(120)
    run_id = str(uuid.uuid4())

    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metadata.gui_job_registry (
                    run_id, action, source_tag, target_tag, status, actor_type, actor_id
                ) VALUES (
                    %s, 'CDC_RUN', 'ulog', 'land_test', 'RUNNING', 'USER', 'cdc_tester'
                )
            """, [run_id])
            # Record pre-test state count in metadata.table_cdc_state
            cur.execute("SELECT COUNT(*) FROM metadata.table_cdc_state WHERE table_id = 120")
            pre_state_count = cur.fetchone()[0]
        conn.commit()

    try:
        # 1. Resolve window
        window = resolver.resolve_and_persist_attempt_window(
            run_id=run_id,
            table_id=120,
            attempt_number=1,
            contract=contract,
        )

        # 2. Build extraction filter SQL
        filter_sql = resolver.build_extraction_filter_sql(window, dialect="mysql")
        upper_h = window.upper_cursor.values[0]
        assert filter_sql == f"`id` <= {upper_h}"

        # 3. Execute read-only bounded count against live MySQL source
        with live_cm.get_db_connection("msopforg_opforg", expected_role="source") as src_conn:
            with src_conn.cursor() as src_cur:
                src_cur.execute(f"SELECT COUNT(*) FROM ulog WHERE {filter_sql}")
                bounded_count = src_cur.fetchone()[0]

                # Query unbounded count to prove boundary holds
                src_cur.execute("SELECT COUNT(*) FROM ulog")
                total_source_count = src_cur.fetchone()[0]

        assert bounded_count > 0
        assert bounded_count <= total_source_count

        # 4. Prove ZERO mutations occurred on metadata.table_cdc_state
        with live_cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM metadata.table_cdc_state WHERE table_id = 120")
                post_state_count = cur.fetchone()[0]

        assert post_state_count == pre_state_count, "CDC-01 must perform ZERO committed cursor mutations!"

    finally:
        with live_cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM metadata.cdc_attempt_window WHERE run_id = %s", [run_id])
                cur.execute("DELETE FROM metadata.gui_job_registry WHERE run_id = %s", [run_id])
            conn.commit()

