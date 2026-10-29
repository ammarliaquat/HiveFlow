# tests/test_cdc01_bounded_extraction.py
"""
CDC-01 — Bounded Source-Cursor Change Extraction Test Suite.

Validates all 21 CDC-01 architectural invariants:
- Explicit candidate CDC authority in metadata.table_cdc_policy_candidate
- Rejection of legacy is_cdc/cdc_columns as authority
- APPEND_ONLY + PREFIX_FINAL certification enforcement
- Bounded (L, H] and (None, H] bootstrap window resolution
- Detection of source regressions (H < L) and empty intervals (L == H)
- Independent PK(table_id, policy_version_id) on committed state ledger
- Durable attempt window persistence in metadata.cdc_attempt_window
- Zero target-derived watermarks & zero last_success_timestamp reliance
- Zero watermark advancement in CDC-01 (Stage 0 freeze intact)
- Schema certification of migration 034
"""

from unittest.mock import MagicMock, patch
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import pytest
import json
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
    PromotedTablePolicy,
    PromotionRequest,
    compute_deterministic_fingerprint,
)
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.metadata.metadata_models import MappingPolicy, ResolvedTableMappings
from src.metadata.policy_governance import PolicyGovernanceService
from src.cdc.cdc_window_resolver import CDCWindowResolver
from src.connections.schema_certifier import SchemaCertifier
from src.exceptions import (
    CDCPolicyNotPromotedError,
    CDCStrategyNotImplementedError,
    CDCMutationModeUnsupportedError,
    CDCCursorFinalityUncertifiedError,
    CDCCursorTypeInvalidError,
    CDCSourceCursorRegressionError,
    CDCUpperBoundCaptureFailedError,
    CDCWindowInvalidError,
)


def test_etl_never_directly_advances_governed_cdc_state():
    source = Path("src/orchestrator/etl.py").read_text(encoding="utf-8")

    assert "INSERT INTO metadata.table_cdc_state" not in source
    assert "AtomicCDCCommitter" in source
    assert "commit_attempt_watermark_and_success" in source


@pytest.fixture
def base_approved_source_key():
    return ApprovedBusinessKey(
        columns=("id",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_src_ulog",
    )


@pytest.fixture
def base_resolved_mappings():
    return ResolvedTableMappings.build(
        table_id=120,
        raw_mappings=[{"column_id": 1, "column_sequence": 1, "source_column": "id", "target_column": "id", "data_type": "int", "source_column_datatype": "int", "canonical_type": "INT"}],
        mapping_policy=MappingPolicy.EXPLICIT,
    )


@pytest.fixture
def base_target_key():
    return ResolvedTargetBusinessKey.build(
        table_id=120,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="fp_src_ulog",
        mapping_fingerprint="fp_map_ulog",
    )


@pytest.fixture
def certified_cdc_policy():
    return ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("id",),
        cursor_types=(CursorLogicalType.INT64,),
        tie_breaker_columns=("id",),
    )


# ─── 1. EXPLICIT CANDIDATE POLICY REQUIRED (ZERO LEGACY INFERENCE) ───────────
def test_cdc01_explicit_candidate_policy_required(base_approved_source_key, base_resolved_mappings, base_target_key):
    """
    Test 1: Legacy is_cdc='y' does NOT authorize CDC. Without a promoted CDC policy,
    resolving a CDC window fails closed with CDCPolicyNotPromotedError.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    resolver = CDCWindowResolver(mock_cm)

    # Contract without CDC policy (unpromoted / non-CDC table)
    promoted = PromotedTablePolicy(
        policy_version_id=str(uuid.uuid4()),
        table_id=120,
        version_number=1,
        policy_schema_version=1,
        contract_payload={"table_id": 120, "cdc_policy": None},
        source_key_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
        target_key_fingerprint="tgt_fp",
        contract_fingerprint="contract_fp",
        policy_fingerprint="policy_fp",
        promoted_at=datetime.utcnow(),
        promoted_by_type="USER",
        promoted_by_id="alice",
        promotion_reason="Initial onboarding",
        supersedes_version_id=None,
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=promoted,
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=None,
    )

    with pytest.raises(CDCPolicyNotPromotedError, match="CDC_POLICY_NOT_PROMOTED"):
        resolver.resolve_and_persist_attempt_window(
            run_id=str(uuid.uuid4()),
            table_id=120,
            attempt_number=1,
            contract=contract,
        )


# ─── 2. SEQUENCE + APPEND_ONLY + PREFIX_FINAL ALLOWED ─────────────────────────
def test_cdc01_sequence_append_only_prefix_final_success(base_approved_source_key, base_resolved_mappings, base_target_key, certified_cdc_policy):
    """
    Test 2: SEQUENCE_HIGH_WATERMARK with APPEND_ONLY and certified PREFIX_FINAL
    resolves a valid bounded window (None, H] on first run.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_src_conn = MagicMock()
    mock_src_cur = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_src_conn
    mock_src_conn.cursor.return_value.__enter__.return_value = mock_src_cur

    v1_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())

    # Mock DB responses:
    # 1. _get_persisted_attempt_window -> None (first attempt)
    # 2. source metadata lookup -> ("mysql", "ulog")
    # 3. _load_committed_cursor -> None (bootstrap run)
    # 4. persist attempt window INSERT RETURNING (window_id, captured_at)
    mock_cur.fetchone.side_effect = [
        None,
        ("mysql", "ulog"),
        None,
        (str(uuid.uuid4()), datetime(2026, 8, 20, 15, 0, 0)),
    ]
    # Source capture query SELECT MAX(`id`) -> 566003
    mock_src_cur.fetchone.return_value = (566003,)

    promoted = PromotedTablePolicy(
        policy_version_id=v1_id,
        table_id=120,
        version_number=1,
        policy_schema_version=1,
        contract_payload={"table_id": 120, "cdc_policy": certified_cdc_policy.to_dict()},
        source_key_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
        target_key_fingerprint="tgt_fp",
        contract_fingerprint="contract_fp",
        policy_fingerprint="policy_fp",
        promoted_at=datetime.utcnow(),
        promoted_by_type="USER",
        promoted_by_id="alice",
        promotion_reason="Initial onboarding with certified CDC",
        supersedes_version_id=None,
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=promoted,
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=certified_cdc_policy,
    )

    resolver = CDCWindowResolver(mock_cm)
    window = resolver.resolve_and_persist_attempt_window(
        run_id=run_id,
        table_id=120,
        attempt_number=1,
        contract=contract,
    )

    assert isinstance(window, ResolvedCDCWindow)
    assert window.table_id == 120
    assert window.lower_cursor is None
    assert window.upper_cursor.values == (566003,)
    assert window.is_empty is False

    # Check generated SQL
    sql = resolver.build_extraction_filter_sql(window, dialect="mysql")
    assert sql == "`id` <= 566003"


def test_bounded_exception_values_are_reread_outside_sequence_window():
    """Frozen exception keys remain in every active sequence extraction."""
    resolver = CDCWindowResolver(MagicMock(spec=ConnectionManager))
    window = ResolvedCDCWindow(
        window_id="window-conditional",
        run_id="run-conditional",
        table_id=120,
        attempt_number=1,
        policy_version_id="policy-conditional",
        cdc_policy_fingerprint="c" * 64,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        lower_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            columns=("id",),
            types=(CursorLogicalType.INT64,),
            values=(100,),
        ),
        upper_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            columns=("id",),
            types=(CursorLogicalType.INT64,),
            values=(200,),
        ),
        window_fingerprint="w" * 64,
        captured_at=datetime.utcnow(),
    )

    assert resolver.build_extraction_filter_sql(
        window, dialect="postgres", exception_values=(0,)
    ) == '(("id" > 100 AND "id" <= 200) OR "id" IN (0))'


def test_bounded_exception_values_are_read_when_cursor_window_is_empty():
    resolver = CDCWindowResolver(MagicMock(spec=ConnectionManager))
    window = ResolvedCDCWindow(
        window_id="window-empty-conditional",
        run_id="run-empty-conditional",
        table_id=120,
        attempt_number=1,
        policy_version_id="policy-conditional",
        cdc_policy_fingerprint="c" * 64,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        lower_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            columns=("id",),
            types=(CursorLogicalType.INT64,),
            values=(200,),
        ),
        upper_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            columns=("id",),
            types=(CursorLogicalType.INT64,),
            values=(200,),
        ),
        window_fingerprint="w" * 64,
        captured_at=datetime.utcnow(),
        is_empty=True,
    )
    assert resolver.build_extraction_filter_sql(
        window, dialect="postgres", exception_values=(0,)
    ) == '"id" IN (0)'


# ─── 3. SEQUENCE + MUTABLE FAILS CLOSED ───────────────────────────────────────
def test_cdc01_sequence_mutable_mode_rejected(base_approved_source_key, base_resolved_mappings, base_target_key):
    """
    Test 3: Sequence high-watermark CDC on MUTABLE source mode fails with
    CDCMutationModeUnsupportedError because updates to lower IDs cannot be detected.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    resolver = CDCWindowResolver(mock_cm)

    mutable_policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("id",),
        cursor_types=(CursorLogicalType.INT64,),
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id="v1", version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=mutable_policy,
    )

    with pytest.raises(CDCMutationModeUnsupportedError, match="CDC_MUTATION_MODE_UNSUPPORTED"):
        resolver.resolve_and_persist_attempt_window(
            run_id=str(uuid.uuid4()),
            table_id=120,
            attempt_number=1,
            contract=contract,
        )


# ─── 4. SEQUENCE + UNCERTIFIED FINALITY FAILS CLOSED ──────────────────────────
def test_cdc01_sequence_uncertified_finality_rejected(base_approved_source_key, base_resolved_mappings, base_target_key):
    """
    Test 4: Sequence high-watermark CDC without certified PREFIX_FINAL finality fails with
    CDCCursorFinalityUncertifiedError because concurrent transactions could commit lower IDs out of order.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    resolver = CDCWindowResolver(mock_cm)

    uncertified_policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("id",),
        cursor_types=(CursorLogicalType.INT64,),
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id="v1", version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=uncertified_policy,
    )

    with pytest.raises(CDCCursorFinalityUncertifiedError, match="CDC_CURSOR_FINALITY_UNCERTIFIED"):
        resolver.resolve_and_persist_attempt_window(
            run_id=str(uuid.uuid4()),
            table_id=120,
            attempt_number=1,
            contract=contract,
        )


# ─── 5. LOG_BASED & TIMESTAMP RETURN NOT IMPLEMENTED ──────────────────────────
def test_cdc01_unimplemented_strategies_fail_closed(base_approved_source_key, base_resolved_mappings, base_target_key):
    """
    Test 5: LOG_BASED fails closed because it has no certified provider path.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    resolver = CDCWindowResolver(mock_cm)

    policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.LOG_BASED,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("id",),
        cursor_types=(CursorLogicalType.INT64,),
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id="v1", version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=policy,
    )
    with pytest.raises(CDCStrategyNotImplementedError, match="CDC_STRATEGY_NOT_IMPLEMENTED"):
        resolver.resolve_and_persist_attempt_window(
            run_id=str(uuid.uuid4()),
            table_id=120,
            attempt_number=1,
            contract=contract,
        )


def test_timestamp_cdc_uses_frozen_overlap_and_business_key_tie_breaker():
    """Timestamp CDC rereads the overlap and bounds the captured upper tuple."""
    resolver = CDCWindowResolver(MagicMock(spec=ConnectionManager))
    window = ResolvedCDCWindow(
        window_id=str(uuid.uuid4()),
        run_id=str(uuid.uuid4()),
        table_id=120,
        attempt_number=1,
        policy_version_id=str(uuid.uuid4()),
        cdc_policy_fingerprint="c" * 64,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        lower_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
            columns=("updated_at", "id"),
            types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
            values=("2026-09-01T10:00:00", 40),
        ),
        upper_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
            columns=("updated_at", "id"),
            types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
            values=("2026-09-02T10:00:00", 42),
        ),
        window_fingerprint="w" * 64,
        captured_at=datetime.utcnow(),
    )
    assert resolver.build_extraction_filter_sql(
        window, dialect="mysql", overlap_days=7
    ) == (
        "`updated_at` >= DATE_SUB('2026-09-01T10:00:00', INTERVAL 7 DAY) "
        "AND (`updated_at` < '2026-09-02T10:00:00' OR "
        "(`updated_at` = '2026-09-02T10:00:00' AND `id` <= 42))"
    )


def test_timestamp_cdc_captures_and_persists_a_composite_upper_cursor(
    base_approved_source_key, base_resolved_mappings, base_target_key
):
    """The frozen timestamp window contains the source max (timestamp, key) tuple."""
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cm.get.return_value = SimpleNamespace(db_type="mysql")
    metadata_conn = MagicMock()
    metadata_cursor = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = metadata_conn
    metadata_conn.cursor.return_value.__enter__.return_value = metadata_cursor
    source_conn = MagicMock()
    source_cursor = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = source_conn
    source_conn.cursor.return_value.__enter__.return_value = source_cursor

    lower_cursor = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        columns=("updated_at", "id"),
        types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        values=("2026-09-01T10:00:00", 40),
    )
    metadata_cursor.fetchone.side_effect = [
        None,
        (lower_cursor.to_dict(),),
        (str(uuid.uuid4()), datetime(2026, 9, 2, 10, 5, 0)),
    ]
    source_cursor.fetchone.return_value = (datetime(2026, 9, 2, 10, 0, 0), 42)

    timestamp_policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("updated_at", "id"),
        cursor_types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        tie_breaker_columns=("id",),
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id=str(uuid.uuid4()), version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=timestamp_policy,
    )

    window = CDCWindowResolver(mock_cm).resolve_and_persist_attempt_window(
        run_id=str(uuid.uuid4()),
        table_id=120,
        attempt_number=1,
        contract=contract,
        source_tag="test_source",
        source_table="events",
        overlap_days=7,
    )

    assert window.lower_cursor == lower_cursor
    assert window.upper_cursor.values == ("2026-09-02T10:00:00", 42)
    assert window.is_empty is False
    assert "ORDER BY `updated_at` DESC, `id` DESC LIMIT 1" in source_cursor.execute.call_args.args[0]


# ─── 6. NUMERIC CURSOR TYPE VALIDATION ────────────────────────────────────────
@pytest.mark.parametrize(
    ("db_type", "expected_limit"),
    [
        ("mysql", "LIMIT 1"),
        ("postgresql", "LIMIT 1"),
        ("mssql", "SELECT TOP 1"),
        ("oracle", "ROWNUM = 1"),
    ],
)
def test_timestamp_upper_cursor_capture_uses_source_dialect_row_limit(db_type, expected_limit):
    """Capture the ordered (timestamp, key) upper tuple with valid dialect syntax."""
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_cm.get.return_value = SimpleNamespace(db_type=db_type)
    source_conn = MagicMock()
    source_cursor = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = source_conn
    source_conn.cursor.return_value.__enter__.return_value = source_cursor
    source_cursor.fetchone.return_value = (datetime(2026, 9, 2, 10, 0, 0), 42)

    policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("updated_at", "id"),
        cursor_types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        tie_breaker_columns=("id",),
    )

    cursor = CDCWindowResolver(mock_cm)._capture_source_upper_boundary(
        "test_source", "events", policy
    )

    assert cursor.values == ("2026-09-02T10:00:00", 42)
    assert expected_limit in source_cursor.execute.call_args.args[0]


def test_cdc01_non_numeric_cursor_rejected_on_sequence(base_approved_source_key, base_resolved_mappings, base_target_key):
    """
    Test 6: Sequence high-watermark with non-INT64 cursor type fails with CDCCursorTypeInvalidError.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    resolver = CDCWindowResolver(mock_cm)

    invalid_type_policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("code",),
        cursor_types=(CursorLogicalType.STRING,),
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id="v1", version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=invalid_type_policy,
    )

    with pytest.raises(CDCCursorTypeInvalidError, match="CDC_CURSOR_TYPE_INVALID"):
        resolver.resolve_and_persist_attempt_window(
            run_id=str(uuid.uuid4()),
            table_id=120,
            attempt_number=1,
            contract=contract,
        )


def test_cdc01_decimal_sequence_cursor_is_persisted_exactly_and_used_as_numeric_sql():
    """Oracle NUMBER(38) boundaries must not be coerced through signed INT64."""
    mock_cm = MagicMock(spec=ConnectionManager)
    resolver = CDCWindowResolver(mock_cm)
    policy = ResolvedCDCPolicy.build(
        table_id=120,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.APPEND_ONLY,
        cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
        cursor_columns=("emigrant_id",),
        cursor_types=(CursorLogicalType.DECIMAL,),
    )
    mock_source_conn = MagicMock()
    mock_source_cur = MagicMock()
    mock_cm.get.return_value = SimpleNamespace(db_type="oracle")
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_source_conn
    mock_source_conn.cursor.return_value.__enter__.return_value = mock_source_cur
    mock_source_cur.fetchone.return_value = (Decimal("12345678901234567890123456789012345678"),)

    cursor = resolver._capture_source_upper_boundary("opf_oracle", "opfservices.events", policy)
    window = ResolvedCDCWindow(
        window_id="window",
        run_id="run",
        table_id=120,
        attempt_number=1,
        policy_version_id="policy",
        cdc_policy_fingerprint="fp",
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        lower_cursor=CDCCursor(1, CDCStrategy.SEQUENCE_HIGH_WATERMARK, ("emigrant_id",), (CursorLogicalType.DECIMAL,), ("9999999999999999999999999999999999999",)),
        upper_cursor=cursor,
        window_fingerprint="window-fp",
        captured_at=datetime.utcnow(),
    )

    assert cursor.values == ("12345678901234567890123456789012345678",)
    assert resolver.build_extraction_filter_sql(window, dialect="oracle") == (
        'emigrant_id > 9999999999999999999999999999999999999 '
        'AND emigrant_id <= 12345678901234567890123456789012345678'
    )


# ─── 7. INCREMENTAL BOUNDED WINDOW (L < id <= H) ──────────────────────────────
def test_cdc01_incremental_bounded_window_generation(base_approved_source_key, base_resolved_mappings, base_target_key, certified_cdc_policy):
    """
    Test 7: When a previous committed cursor L exists in metadata.table_cdc_state,
    the resolver generates an incremental bounded window (L < id <= H).
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_src_conn = MagicMock()
    mock_src_cur = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_src_conn
    mock_src_conn.cursor.return_value.__enter__.return_value = mock_src_cur

    v1_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())

    committed_l = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(454663,),
    )

    mock_cur.fetchone.side_effect = [
        None,  # no persisted attempt window
        ("mysql", "ulog"),  # source table info
        (committed_l.to_dict(),),  # committed cursor L = 454663
        (str(uuid.uuid4()), datetime(2026, 8, 20, 15, 30, 0)),  # persist INSERT
    ]
    # Source capture returns H = 566003
    mock_src_cur.fetchone.return_value = (566003,)

    promoted = PromotedTablePolicy(
        policy_version_id=v1_id,
        table_id=120,
        version_number=1,
        policy_schema_version=1,
        contract_payload={"table_id": 120, "cdc_policy": certified_cdc_policy.to_dict()},
        source_key_fingerprint="src_fp",
        mapping_fingerprint="map_fp",
        target_key_fingerprint="tgt_fp",
        contract_fingerprint="contract_fp",
        policy_fingerprint="policy_fp",
        promoted_at=datetime.utcnow(),
        promoted_by_type="USER",
        promoted_by_id="alice",
        promotion_reason="Incremental test",
        supersedes_version_id=None,
    )
    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=promoted,
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=certified_cdc_policy,
    )

    resolver = CDCWindowResolver(mock_cm)
    window = resolver.resolve_and_persist_attempt_window(
        run_id=run_id,
        table_id=120,
        attempt_number=1,
        contract=contract,
    )

    assert window.lower_cursor.values == (454663,)
    assert window.upper_cursor.values == (566003,)
    assert window.is_empty is False

    sql = resolver.build_extraction_filter_sql(window, dialect="mysql")
    assert sql == "`id` > 454663 AND `id` <= 566003"


# ─── 8. L == H PRODUCES EMPTY INTERVAL (1=0) ──────────────────────────────────
def test_cdc01_equal_lower_and_upper_yields_empty_interval(base_approved_source_key, base_resolved_mappings, base_target_key, certified_cdc_policy):
    """
    Test 8: When L == H (no new rows arrived), resolver produces an empty window (1=0).
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_src_conn = MagicMock()
    mock_src_cur = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_src_conn
    mock_src_conn.cursor.return_value.__enter__.return_value = mock_src_cur

    committed_l = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(566003,),
    )

    mock_cur.fetchone.side_effect = [
        None,
        ("mysql", "ulog"),
        (committed_l.to_dict(),),
        (str(uuid.uuid4()), datetime(2026, 8, 20, 16, 0, 0)),
    ]
    # Source capture returns H = 566003 (same as L)
    mock_src_cur.fetchone.return_value = (566003,)

    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id="v1", version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=certified_cdc_policy,
    )

    resolver = CDCWindowResolver(mock_cm)
    window = resolver.resolve_and_persist_attempt_window(
        run_id=str(uuid.uuid4()),
        table_id=120,
        attempt_number=1,
        contract=contract,
    )

    assert window.is_empty is True
    sql = resolver.build_extraction_filter_sql(window, dialect="mysql")
    assert sql == "1=0"


# ─── 9. H < L FAILS AS SOURCE REGRESSION ──────────────────────────────────────
def test_cdc01_source_cursor_regression_fails_closed(base_approved_source_key, base_resolved_mappings, base_target_key, certified_cdc_policy):
    """
    Test 9: When H < L (source was truncated, reseeded, or wrong endpoint),
    resolver raises CDCSourceCursorRegressionError.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_src_conn = MagicMock()
    mock_src_cur = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_src_conn
    mock_src_conn.cursor.return_value.__enter__.return_value = mock_src_cur

    committed_l = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(566003,),
    )

    mock_cur.fetchone.side_effect = [
        None,
        ("mysql", "ulog"),
        (committed_l.to_dict(),),  # L = 566003
    ]
    # Source capture returns H = 10000 (regressed!)
    mock_src_cur.fetchone.return_value = (10000,)

    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id="v1", version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=certified_cdc_policy,
    )

    resolver = CDCWindowResolver(mock_cm)
    with pytest.raises(CDCSourceCursorRegressionError, match="CDC_SOURCE_CURSOR_REGRESSION"):
        resolver.resolve_and_persist_attempt_window(
            run_id=str(uuid.uuid4()),
            table_id=120,
            attempt_number=1,
            contract=contract,
        )


# ─── 10. FROZEN ATTEMPT WINDOW IMMUTABILITY UNDER SOURCE GROWTH ───────────────
def test_cdc01_frozen_attempt_window_immutable_under_source_growth(base_approved_source_key, base_resolved_mappings, base_target_key, certified_cdc_policy):
    """
    Test 10: Once an attempt window is persisted in metadata.cdc_attempt_window,
    re-resolving the window for the same attempt returns the identical frozen window H
    even if the live source has grown.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    run_id = str(uuid.uuid4())
    v1_id = str(uuid.uuid4())

    persisted_lower = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(454663,),
    )
    persisted_upper = CDCCursor(
        schema_version=1,
        strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
        columns=("id",),
        types=(CursorLogicalType.INT64,),
        values=(566003,),
    )

    # First fetch returns existing row in cdc_attempt_window
    mock_cur.fetchone.return_value = (
        "window-uuid-1",
        run_id,
        120,
        1,
        v1_id,
        "cdc_fp",
        "win_fp",
        persisted_lower.to_dict(),
        persisted_upper.to_dict(),
        datetime(2026, 8, 20, 15, 0, 0),
    )

    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id=v1_id, version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=certified_cdc_policy,
    )

    resolver = CDCWindowResolver(mock_cm)
    window = resolver.resolve_and_persist_attempt_window(
        run_id=run_id,
        table_id=120,
        attempt_number=1,
        contract=contract,
    )

    # Upper cursor remains 566003 from persisted window
    assert window.upper_cursor.values == (566003,)
    assert window.lower_cursor.values == (454663,)
    assert window.window_id == "window-uuid-1"


# ─── 11. NO IMPLICIT CURSOR INHERITANCE ACROSS POLICY VERSIONS ────────────────
def test_cdc01_new_policy_version_does_not_inherit_previous_cursor(base_approved_source_key, base_resolved_mappings, base_target_key, certified_cdc_policy):
    """
    Test 11: When promoting to Policy Version 2, Version 2 does NOT inherit Version 1's
    committed cursor from table_cdc_state because PK is (table_id, policy_version_id).
    Version 2 starts as a bootstrap run (L = None).
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_src_conn = MagicMock()
    mock_src_cur = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_src_conn
    mock_src_conn.cursor.return_value.__enter__.return_value = mock_src_cur

    v2_id = str(uuid.uuid4())

    # Query for table_cdc_state with (table_id=120, policy_version_id=v2_id) returns None
    mock_cur.fetchone.side_effect = [
        None,  # no attempt window
        ("mysql", "ulog"),  # source table
        None,  # no committed cursor for v2!
        (str(uuid.uuid4()), datetime(2026, 8, 20, 16, 30, 0)),
    ]
    mock_src_cur.fetchone.return_value = (567000,)

    contract_v2 = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id=v2_id, version_number=2),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=certified_cdc_policy,
    )

    resolver = CDCWindowResolver(mock_cm)
    window = resolver.resolve_and_persist_attempt_window(
        run_id=str(uuid.uuid4()),
        table_id=120,
        attempt_number=1,
        contract=contract_v2,
    )

    # v2 lower cursor is None (bootstrap)
    assert window.lower_cursor is None
    assert window.upper_cursor.values == (567000,)


# ─── 12. ZERO WATERMARK ADVANCEMENT & ZERO GOLD KEYED DML IN CDC-01 ───────────
def test_cdc01_zero_watermark_advancement_and_zero_gold_mutation(base_approved_source_key, base_resolved_mappings, base_target_key, certified_cdc_policy):
    """
    Test 12: Verifies that CDC-01 performs ZERO UPDATE / INSERT on metadata.table_cdc_state.
    Committed cursor advancement remains frozen until OBS-01.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_src_conn = MagicMock()
    mock_src_cur = MagicMock()
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_src_conn
    mock_src_conn.cursor.return_value.__enter__.return_value = mock_src_cur

    mock_cur.fetchone.side_effect = [
        None,
        ("mysql", "ulog"),
        None,
        (str(uuid.uuid4()), datetime(2026, 8, 20, 16, 45, 0)),
    ]
    mock_src_cur.fetchone.return_value = (566003,)

    contract = ResolvedTableContract.from_promoted_policy(
        promoted_policy=MagicMock(policy_version_id="v1", version_number=1),
        approved_source_business_key=base_approved_source_key,
        resolved_mappings=base_resolved_mappings,
        canonical_target_business_key=base_target_key,
        cdc_policy=certified_cdc_policy,
    )

    resolver = CDCWindowResolver(mock_cm)
    resolver.resolve_and_persist_attempt_window(
        run_id=str(uuid.uuid4()),
        table_id=120,
        attempt_number=1,
        contract=contract,
    )

    # Assert that NO executed SQL contains UPDATE on table_cdc_state
    for call in mock_cur.execute.call_args_list:
        sql = call[0][0]
        assert "UPDATE metadata.table_cdc_state" not in sql
        assert "INSERT INTO metadata.table_cdc_state" not in sql


# ─── 13. SCHEMA CERTIFIER CERTIFIES MIGRATION 034 ─────────────────────────────
def test_cdc01_schema_certification_migration_034():
    """
    Test 13: SchemaCertifier validates all canonical migrations including 034.
    """
    mock_cm = MagicMock(spec=ConnectionManager)
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    certifier = SchemaCertifier(mock_cm)
    local_migrations = certifier._get_expected_ledger_migrations()
    assert any(m[0] == "034" for m in local_migrations)

    mock_cur.fetchall.return_value = local_migrations
    certifier.certify()  # Passes without raising exception
