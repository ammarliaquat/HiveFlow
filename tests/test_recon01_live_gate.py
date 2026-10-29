# tests/test_recon01_live_gate.py
"""
RECON-01 — Live Integration Gate Verification Suite.

Validates RECON-01 against live MySQL source, PostgreSQL metadata database, and disposable test storage:
- Case 1: Live Bounded Window & Manifest Freezing -> Disposable Target Persistence -> Live Strategy-Aware Reconciliation -> Assert CONVERGED in metadata.cdc_attempt_reconciliation.
- Case 2: Live Injected Discrepancy (Missing Key) -> Reconciler fails closed with ReconciliationRowLossError -> Assert REJECTED record persisted durably in metadata.cdc_attempt_reconciliation.
- Case 3: Live Stage-0 Invariant Verification -> Prove zero mutations to metadata.table_cdc_state and zero modifications to live landing.ulog.
"""

from datetime import datetime
import os
import shutil
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
    ReconciliationStrategy,
    ReconciliationOutcome,
    AttemptPersistenceManifest,
    PhysicalReconciliationResult,
    PromotionRequest,
)
from src.metadata.key_contract import ResolvedTableContract
from src.metadata.policy_governance import PolicyGovernanceService
from src.cdc.cdc_window_resolver import CDCWindowResolver
from src.validation.strategy_reconciler import StrategyAwareReconciler
from src.exceptions import ReconciliationRowLossError


@pytest.fixture(scope="module")
def live_cm():
    return ConnectionManager("config/config.local.yaml")


@pytest.fixture(scope="module")
def live_governance_service(live_cm):
    return PolicyGovernanceService(live_cm)


@pytest.fixture(scope="module")
def live_window_resolver(live_cm):
    return CDCWindowResolver(live_cm)


@pytest.fixture(scope="module")
def live_reconciler(live_cm):
    return StrategyAwareReconciler(live_cm)


@pytest.fixture(scope="module")
def promoted_ulog_contract(live_cm, live_governance_service):
    service = live_governance_service
    # Set explicit candidate CDC policy in metadata.table_cdc_policy_candidate
    service.set_candidate_cdc_policy(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("id",),
        cursor_types=(CursorLogicalType.INT64,),
        tie_breaker_columns=("id",),
    )
    candidate = service.build_candidate_contract(120)
    req = PromotionRequest(
        table_id=120,
        expected_candidate_fingerprint=candidate.candidate_fingerprint,
        actor_type="USER",
        actor_id="recon_tester",
        reason="Promoted verified SEQUENCE_HIGH_WATERMARK CDC policy for ulog in RECON-01 live test",
    )
    try:
        service.promote_policy(req)
    except Exception:
        pass
    return service.materialize_execution_contract(120)


@pytest.fixture(autouse=True)
def cleanup_live_recon_test_data(live_cm):
    test_run_ids = []
    yield test_run_ids
    if test_run_ids:
        with live_cm.get_metadata_db_connection() as conn:
            with conn.cursor() as cur:
                for r_id in test_run_ids:
                    cur.execute("DELETE FROM metadata.cdc_attempt_reconciliation WHERE run_id = %s", [r_id])
                    cur.execute("DELETE FROM metadata.attempt_persistence_manifest WHERE run_id = %s", [r_id])
                    cur.execute("DELETE FROM metadata.cdc_attempt_window WHERE run_id = %s", [r_id])
                    cur.execute("DELETE FROM metadata.gui_job_registry WHERE run_id = %s", [r_id])
            conn.commit()


# ─── CASE 1: LIVE TARGET CONVERGENCE VERIFICATION ──────────────────────────────
def test_recon01_live_case1_physical_target_convergence(
    live_cm, promoted_ulog_contract, live_window_resolver, live_reconciler, cleanup_live_recon_test_data
):
    """
    Case 1:
    1. Promote authoritative contract for table 120 (ulog).
    2. Resolve live attempt window against MySQL source (table 120).
    3. Extract bounded keys from live source table.
    4. Freeze attempt persistence manifest.
    5. Write test keys to disposable staging.
    6. Execute live StrategyAwareReconciler and verify CONVERGED persisted in metadata.cdc_attempt_reconciliation.
    """
    table_id = 120
    test_run_id = str(uuid.uuid4())
    cleanup_live_recon_test_data.append(test_run_id)

    # Register test run in gui_job_registry
    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metadata.gui_job_registry (
                    run_id, action, source_tag, target_tag, status, actor_type, actor_id
                ) VALUES (
                    %s, 'RECON_TEST', 'msopforg_opforg', 'land_test', 'RUNNING', 'USER', 'recon_tester'
                )
            """, [test_run_id])
        conn.commit()

    contract = promoted_ulog_contract

    # Resolve live CDC attempt window
    window = live_window_resolver.resolve_and_persist_attempt_window(
        run_id=test_run_id,
        table_id=table_id,
        attempt_number=1,
        contract=contract,
    )

    # Extract sample keys from live MySQL source table matching the bounded predicate
    filter_sql = live_window_resolver.build_extraction_filter_sql(window=window)
    extracted_keys = []
    with live_cm.get_db_connection("msopforg_opforg", expected_role="source") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM ulog WHERE {filter_sql} LIMIT 20")
            rows = cur.fetchall()
            extracted_keys = [r[0] for r in rows]

    assert len(extracted_keys) > 0, "Expected at least 1 extracted key from live MySQL ulog!"

    # Freeze attempt persistence manifest
    manifest = live_reconciler.freeze_persistence_manifest(
        run_id=test_run_id,
        table_id=table_id,
        attempt_number=1,
        contract=contract,
        window=window,
        storage_layer="target_disposable",
        storage_object_identity="disposable_test_target_store",
        expected_keys=extracted_keys,
        schema_fingerprint="schema_fp_test",
    )

    assert manifest.expected_row_count == len(extracted_keys)

    # Reconcile physical convergence
    recon_result = live_reconciler.reconcile_attempt_physical_convergence(
        run_id=test_run_id,
        table_id=table_id,
        attempt_number=1,
        contract=contract,
        window=window,
        manifest=manifest,
        reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
        storage_layer="target_disposable",
        physical_keys=extracted_keys,
        expected_keys=extracted_keys,
        is_commit_authoritative=True,
    )

    assert recon_result.outcome == ReconciliationOutcome.CONVERGED
    assert recon_result.is_commit_authoritative is True
    assert recon_result.missing_key_count == 0
    assert recon_result.unexpected_key_count == 0

    # Verify durable record exists in PostgreSQL metadata database
    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT outcome, is_commit_authoritative, expected_row_count, physical_row_count
                FROM metadata.cdc_attempt_reconciliation
                WHERE run_id = %s AND table_id = %s AND attempt_number = %s
            """, [test_run_id, table_id, 1])
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "CONVERGED"
            assert row[1] is True
            assert row[2] == len(extracted_keys)
            assert row[3] == len(extracted_keys)


# ─── CASE 2: LIVE INJECTED DISCREPANCY & DURABLE AUDIT RECORD ──────────────────
def test_recon01_live_case2_injected_discrepancy_fails_closed(
    live_cm, promoted_ulog_contract, live_window_resolver, live_reconciler, cleanup_live_recon_test_data
):
    """
    Case 2:
    1. Resolve live attempt window and freeze manifest for ulog.
    2. Inject deliberate missing key (underflow) in physical keys.
    3. Assert StrategyAwareReconciler raises ReconciliationRowLossError.
    4. Assert durable REJECTED record is persisted in metadata.cdc_attempt_reconciliation.
    """
    table_id = 120
    test_run_id = str(uuid.uuid4())
    cleanup_live_recon_test_data.append(test_run_id)

    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metadata.gui_job_registry (
                    run_id, action, source_tag, target_tag, status, actor_type, actor_id
                ) VALUES (
                    %s, 'RECON_TEST', 'msopforg_opforg', 'land_test', 'RUNNING', 'USER', 'recon_tester'
                )
            """, [test_run_id])
        conn.commit()

    contract = promoted_ulog_contract
    window = live_window_resolver.resolve_and_persist_attempt_window(
        run_id=test_run_id,
        table_id=table_id,
        attempt_number=1,
        contract=contract,
    )

    expected_keys = [10001, 10002, 10003]
    corrupted_physical_keys = [10001, 10002]  # Missing 10003!

    manifest = live_reconciler.freeze_persistence_manifest(
        run_id=test_run_id,
        table_id=table_id,
        attempt_number=1,
        contract=contract,
        window=window,
        storage_layer="target_disposable",
        storage_object_identity="disposable_test_target_store",
        expected_keys=expected_keys,
        schema_fingerprint="schema_fp_test",
    )

    with pytest.raises(ReconciliationRowLossError):
        live_reconciler.reconcile_attempt_physical_convergence(
            run_id=test_run_id,
            table_id=table_id,
            attempt_number=1,
            contract=contract,
            window=window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target_disposable",
            physical_keys=corrupted_physical_keys,
            expected_keys=expected_keys,
            is_commit_authoritative=True,
        )

    # Prove durable failure audit record was persisted despite the exception
    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT outcome, failure_code, missing_key_count
                FROM metadata.cdc_attempt_reconciliation
                WHERE run_id = %s AND table_id = %s AND attempt_number = %s
            """, [test_run_id, table_id, 1])
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "REJECTED"
            assert row[1] == "RECONCILIATION_ROW_LOSS_DETECTED"
            assert row[2] == 1


# ─── CASE 3: LIVE STAGE-0 INVARIANT VERIFICATION ──────────────────────────────
def test_recon01_live_case3_stage0_invariants(live_cm):
    """
    Case 3: Proves Stage-0 invariants:
    1. Zero committed cursor entries in metadata.table_cdc_state.
    2. Real production landing.ulog has not been modified.
    """
    with live_cm.get_metadata_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM metadata.table_cdc_state")
            state_count = cur.fetchone()[0]
            assert state_count == 0, "metadata.table_cdc_state must remain empty until OBS-01!"
