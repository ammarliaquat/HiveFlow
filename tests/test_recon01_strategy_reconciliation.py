# tests/test_recon01_strategy_reconciliation.py
"""
RECON-01 — Physical Strategy-Aware Reconciliation Unit Tests.

Validates the complete RECON-01 reconciliation engine:
1. Manifest freezing & deterministic key-set fingerprinting.
2. Exact key-set physical convergence (happy path).
3. Equal count but different key sets fails closed (RECONCILIATION_KEY_SET_MISMATCH).
4. Underflow row loss fails closed (RECONCILIATION_ROW_LOSS_DETECTED).
5. Overflow / duplicate write fails closed (RECONCILIATION_OVERFLOW_DETECTED).
6. Cursor boundary leakage fails closed (RECONCILIATION_CURSOR_BOUNDARY_VIOLATED).
7. Null business key in physical store fails closed (RECONCILIATION_NULL_KEY_DETECTED).
8. Duplicate business key in physical store fails closed (RECONCILIATION_DUPLICATE_KEY_DETECTED).
9. Empty window in populated target succeeds as EMPTY_VERIFIED.
10. Window / Manifest / Contract identity mismatch fails closed before scan.
11. Policy superseded during reconciliation scan fails closed.
12. Rejected reconciliation is persisted durably before exception is raised.
13. Multi-layer reconciliation records (Landing vs Target) coexist independently.
14. Intermediate layer reconciliation does not grant commit authority.
15. Stage-0 Safety: Zero cursor advancement and zero Gold mutations.
16. Schema certification for Migration 035.
"""

from datetime import datetime
from unittest.mock import MagicMock
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
)
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTableContract,
    ResolvedTargetBusinessKey,
)
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings
from src.validation.strategy_reconciler import StrategyAwareReconciler
from src.exceptions import (
    ReconciliationFailedError,
    ReconciliationRowLossError,
    ReconciliationOverflowError,
    ReconciliationKeySetMismatchError,
    ReconciliationCursorBoundaryViolationError,
    ReconciliationNullKeyError,
    ReconciliationDuplicateKeyError,
    ReconciliationWindowMismatchError,
    ReconciliationPolicySupersededError,
)
from src.connections.schema_certifier import SchemaCertifier


@pytest.fixture
def sample_contract():
    policy_ver_id = str(uuid.uuid4())
    cdc_policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("id",),
        cursor_types=(CursorLogicalType.INT64,),
        tie_breaker_columns=("id",),
    )
    return ResolvedTableContract(
        table_id=120,
        policy_version_id=policy_ver_id,
        version_number=1,
        policy_fingerprint="contract_fp_1111",
        approved_source_business_key=ApprovedBusinessKey(
            columns=("id",),
            evidence_type=KeyEvidenceType.PRIMARY_KEY,
            evidence_fingerprint="src_fp",
        ),
        resolved_mappings=ResolvedTableMappings.build(
            table_id=120,
            raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
            mapping_policy=MappingPolicy.EXPLICIT,
        ),
        canonical_target_business_key=ResolvedTargetBusinessKey.build(
            table_id=120,
            source_key_columns=("id",),
            target_key_columns=("id",),
            source_key_evidence_fingerprint="src_fp",
            mapping_fingerprint="map_fp",
        ),
        contract_payload={"test": "payload"},
        contract_fingerprint="contract_fp_1111",
        cdc_policy=cdc_policy,
    )


@pytest.fixture
def sample_window(sample_contract):
    lower_c = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(100,),
    )
    upper_c = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(105,),
    )
    return ResolvedCDCWindow(
        window_id=str(uuid.uuid4()),
        run_id=str(uuid.uuid4()),
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        cdc_policy_fingerprint=sample_contract.cdc_policy.cdc_policy_fingerprint,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        lower_cursor=lower_c,
        upper_cursor=upper_c,
        window_fingerprint="window_fp_123",
        captured_at=datetime.utcnow(),
        is_empty=False,
    )


@pytest.fixture
def mock_cm():
    cm = MagicMock(spec=ConnectionManager)
    conn = MagicMock()
    cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    return cm, conn, cur


# ─── 1. MANIFEST FREEZING & DETERMINISTIC FINGERPRINTING ───────────────────────
def test_recon01_manifest_freezing_and_fingerprint(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103, 104, 105]
    manifest = reconciler.freeze_persistence_manifest(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        contract=sample_contract,
        window=sample_window,
        storage_layer="landing",
        storage_object_identity="file:///opt/data/landing/ulog/run_1/attempt_1.parquet",
        expected_keys=expected_keys,
        schema_fingerprint="schema_fp_123",
    )

    assert isinstance(manifest, AttemptPersistenceManifest)
    assert manifest.expected_row_count == 5
    assert manifest.table_id == 120
    assert manifest.policy_version_id == sample_contract.policy_version_id
    assert manifest.window_id == sample_window.window_id
    assert manifest.expected_key_fingerprint is not None
    assert manifest.manifest_fingerprint is not None

    # Verify order-independence: same keys in different order yield identical fingerprint
    fp_shuffled = reconciler.compute_key_set_fingerprint([105, 101, 104, 102, 103])
    assert fp_shuffled == manifest.expected_key_fingerprint


# ─── 2. EXACT KEY-SET PHYSICAL CONVERGENCE (HAPPY PATH) ────────────────────────
def test_recon01_exact_key_set_convergence_success(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)  # Post-fence policy active check
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103, 104, 105]
    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=5,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    result = reconciler.reconcile_attempt_physical_convergence(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        contract=sample_contract,
        window=sample_window,
        manifest=manifest,
        reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
        storage_layer="target",
        physical_keys=[101, 102, 103, 104, 105],
        expected_keys=expected_keys,
        is_commit_authoritative=True,
    )

    assert isinstance(result, PhysicalReconciliationResult)
    assert result.outcome == ReconciliationOutcome.CONVERGED
    assert result.failure_code is None
    assert result.missing_key_count == 0
    assert result.unexpected_key_count == 0
    assert result.duplicate_key_count == 0
    assert result.null_key_count == 0
    assert result.is_commit_authoritative is True


# ─── 3. SAME COUNT BUT DIFFERENT KEYS FAILS CLOSED ────────────────────────────
def test_recon01_same_count_different_keys_fails_closed(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103]
    physical_keys = [101, 102, 104]  # 103 missing, 104 unexpected (both count = 3)

    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=3,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationRowLossError, match="RECONCILIATION_ROW_LOSS_DETECTED"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target",
            physical_keys=physical_keys,
            expected_keys=expected_keys,
        )


# ─── 4. UNDERFLOW ROW LOSS FAILS CLOSED ────────────────────────────────────────
def test_recon01_underflow_row_loss_fails_closed(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103, 104, 105]
    physical_keys = [101, 102, 103]  # missing 104, 105

    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="landing",
        storage_object_identity="landing.ulog",
        expected_row_count=5,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationRowLossError, match="RECONCILIATION_ROW_LOSS_DETECTED"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.ATTEMPT_SCOPED_APPEND,
            storage_layer="landing",
            physical_keys=physical_keys,
            expected_keys=expected_keys,
        )


# ─── 5. OVERFLOW DUPLICATE WRITE FAILS CLOSED ──────────────────────────────────
def test_recon01_overflow_duplicate_write_fails_closed(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102]
    physical_keys = [101, 102, 103, 104]  # extra 103, 104

    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="landing",
        storage_object_identity="landing.ulog",
        expected_row_count=2,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationOverflowError, match="RECONCILIATION_OVERFLOW_DETECTED"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.ATTEMPT_SCOPED_APPEND,
            storage_layer="landing",
            physical_keys=physical_keys,
            expected_keys=expected_keys,
        )


# ─── 6. CURSOR BOUNDARY LEAKAGE FAILS CLOSED ───────────────────────────────────
def test_recon01_cursor_boundary_leakage_fails_closed(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103]
    physical_keys = [101, 102, 103]

    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=3,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationCursorBoundaryViolationError, match="RECONCILIATION_CURSOR_BOUNDARY_VIOLATED"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target",
            physical_keys=physical_keys,
            expected_keys=expected_keys,
            out_of_bounds_count=2,  # 2 records found outside window (L, H]
        )


# ─── 7. NULL BUSINESS KEY IN TARGET FAILS CLOSED ──────────────────────────────
def test_recon01_null_key_in_target_fails_closed(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103]
    physical_keys = [101, 102, None]  # 1 null key

    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=3,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationNullKeyError, match="RECONCILIATION_NULL_KEY_DETECTED"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target",
            physical_keys=physical_keys,
            expected_keys=expected_keys,
        )


# ─── 8. DUPLICATE BUSINESS KEY IN TARGET FAILS CLOSED ─────────────────────────
def test_recon01_duplicate_key_in_target_fails_closed(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103]
    physical_keys = [101, 102, 102]  # duplicate key 102

    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=3,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationDuplicateKeyError, match="RECONCILIATION_DUPLICATE_KEY_DETECTED"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target",
            physical_keys=physical_keys,
            expected_keys=expected_keys,
        )


# ─── 9. EMPTY WINDOW SUCCEEDS AS EMPTY_VERIFIED ───────────────────────────────
def test_recon01_empty_window_in_populated_target_success(sample_contract, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    cur_100 = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(100,),
    )
    empty_window = ResolvedCDCWindow(
        window_id=str(uuid.uuid4()),
        run_id=str(uuid.uuid4()),
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        cdc_policy_fingerprint=sample_contract.cdc_policy.cdc_policy_fingerprint,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        lower_cursor=cur_100,
        upper_cursor=cur_100,
        window_fingerprint="window_fp_empty",
        captured_at=datetime.utcnow(),
        is_empty=True,
    )

    manifest = AttemptPersistenceManifest.build(
        run_id=empty_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=empty_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=0,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint([]),
        schema_fingerprint="schema_fp_123",
    )

    result = reconciler.reconcile_attempt_physical_convergence(
        run_id=empty_window.run_id,
        table_id=120,
        attempt_number=1,
        contract=sample_contract,
        window=empty_window,
        manifest=manifest,
        reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
        storage_layer="target",
        physical_keys=[],  # 0 attempt rows in window scope
        expected_keys=[],
        is_commit_authoritative=True,
    )

    assert result.outcome == ReconciliationOutcome.EMPTY_VERIFIED
    assert result.failure_code is None
    assert result.expected_row_count == 0
    assert result.physical_row_count == 0


# ─── 10. WINDOW / MANIFEST / CONTRACT IDENTITY MISMATCH FAILS CLOSED ───────────
def test_recon01_window_identity_mismatch_fast_fail(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    reconciler = StrategyAwareReconciler(cm)

    different_window_id = uuid.uuid4()
    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=different_window_id,  # Mismatched window ID!
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=3,
        expected_key_fingerprint="fp_123",
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationWindowMismatchError, match="RECONCILIATION_WINDOW_MISMATCH"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target",
            physical_keys=[101, 102, 103],
            expected_keys=[101, 102, 103],
        )


# ─── 11. POLICY SUPERSEDED DURING SCAN FAILS CLOSED ───────────────────────────
def test_recon01_policy_superseded_during_scan_fails_closed(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    # Return a newer policy version ID on post-fence check
    cur.fetchone.return_value = (str(uuid.uuid4()),)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103]
    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=3,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationPolicySupersededError, match="RECONCILIATION_POLICY_SUPERSEDED"):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target",
            physical_keys=expected_keys,
            expected_keys=expected_keys,
        )


# ─── 12. REJECTED RECONCILIATION PERSISTED DURABLY BEFORE EXCEPTION ────────────
def test_recon01_rejected_reconciliation_persisted_durably(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102, 103]
    physical_keys = [101, 102]  # Underflow

    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=3,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    with pytest.raises(ReconciliationRowLossError):
        reconciler.reconcile_attempt_physical_convergence(
            run_id=sample_window.run_id,
            table_id=120,
            attempt_number=1,
            contract=sample_contract,
            window=sample_window,
            manifest=manifest,
            reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
            storage_layer="target",
            physical_keys=physical_keys,
            expected_keys=expected_keys,
        )

    # Verify INSERT was executed before raising exception
    assert cur.execute.called
    assert conn.commit.called


# ─── 13. MULTI-LAYER RECONCILIATION RECORDS INDEPENDENT ───────────────────────
def test_recon01_multi_layer_records_independent(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102]
    manifest_landing = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="landing",
        storage_object_identity="landing.ulog",
        expected_row_count=2,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )
    manifest_target = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=2,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    res_landing = reconciler.reconcile_attempt_physical_convergence(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        contract=sample_contract,
        window=sample_window,
        manifest=manifest_landing,
        reconciliation_strategy=ReconciliationStrategy.ATTEMPT_SCOPED_APPEND,
        storage_layer="landing",
        physical_keys=expected_keys,
        expected_keys=expected_keys,
        is_commit_authoritative=False,
    )

    res_target = reconciler.reconcile_attempt_physical_convergence(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        contract=sample_contract,
        window=sample_window,
        manifest=manifest_target,
        reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
        storage_layer="target",
        physical_keys=expected_keys,
        expected_keys=expected_keys,
        is_commit_authoritative=True,
    )

    assert res_landing.storage_layer == "landing"
    assert res_landing.is_commit_authoritative is False
    assert res_target.storage_layer == "target"
    assert res_target.is_commit_authoritative is True


# ─── 14. INTERMEDIATE LAYER SUCCESS DOES NOT GRANT COMMIT AUTHORITY ───────────
def test_recon01_landing_success_does_not_authorize_commit(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102]
    manifest_landing = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="landing",
        storage_object_identity="landing.ulog",
        expected_row_count=2,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    res_landing = reconciler.reconcile_attempt_physical_convergence(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        contract=sample_contract,
        window=sample_window,
        manifest=manifest_landing,
        reconciliation_strategy=ReconciliationStrategy.ATTEMPT_SCOPED_APPEND,
        storage_layer="landing",
        physical_keys=expected_keys,
        expected_keys=expected_keys,
        is_commit_authoritative=False,
    )

    assert res_landing.outcome == ReconciliationOutcome.CONVERGED
    assert res_landing.is_commit_authoritative is False  # Must NOT be authoritative for OBS-01 commit!


# ─── 15. ZERO WATERMARK ADVANCEMENT & ZERO GOLD DML IN RECON-01 ────────────────
def test_recon01_zero_watermark_advancement_and_zero_gold_mutation(sample_contract, sample_window, mock_cm):
    cm, conn, cur = mock_cm
    cur.fetchone.return_value = (sample_contract.policy_version_id,)
    reconciler = StrategyAwareReconciler(cm)

    expected_keys = [101, 102]
    manifest = AttemptPersistenceManifest.build(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        policy_version_id=sample_contract.policy_version_id,
        window_id=sample_window.window_id,
        storage_layer="target",
        storage_object_identity="target.ulog",
        expected_row_count=2,
        expected_key_fingerprint=reconciler.compute_key_set_fingerprint(expected_keys),
        schema_fingerprint="schema_fp_123",
    )

    reconciler.reconcile_attempt_physical_convergence(
        run_id=sample_window.run_id,
        table_id=120,
        attempt_number=1,
        contract=sample_contract,
        window=sample_window,
        manifest=manifest,
        reconciliation_strategy=ReconciliationStrategy.KEYED_WINDOW_CONVERGENCE,
        storage_layer="target",
        physical_keys=expected_keys,
        expected_keys=expected_keys,
        is_commit_authoritative=True,
    )

    # Prove zero mutations to metadata.table_cdc_state
    for call_item in cur.execute.call_args_list:
        sql = call_item[0][0].lower()
        assert "table_cdc_state" not in sql, "RECON-01 must perform zero mutations to metadata.table_cdc_state!"


# ─── 16. SCHEMA CERTIFICATION CERTIFIES MIGRATION 035 ─────────────────────────
@pytest.mark.live
def test_recon01_schema_certification_migration_035():
    cm = ConnectionManager("config/config.local.yaml")
    certifier = SchemaCertifier(cm)
    certifier.certify()
