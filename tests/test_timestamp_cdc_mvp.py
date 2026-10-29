from src.models.principal import AuthenticatedPrincipal
# tests/test_timestamp_cdc_mvp.py
"""
MVP Test Suite: Safe Timestamp CDC (Phase 1 Single-Key).

Verifies the 8 user requirements:
1. Timestamp candidate with complete evidence promotes to is_cdc = 'y'.
2. Missing key, timestamp profile, or cdc_p_days leaves is_cdc = 'n';
   target physical readiness is checked and provisioned at landing preflight.
3. A governed timestamp run persists and reuses the same (updated_at, id) window.
4. Equal timestamps with different id values are correctly bounded in extraction filter.
5. Incremental path uses UPSERT and never truncates.
6. Target/reconciliation failure does not advance the cursor.
7. stage_metrics contains frozen filter, counts, and reconciliation outcome.
8. Full offline verification passes without live dependency.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
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
    ReconciliationOutcome,
    CandidateTableContract,
    PromotionRequest,
)
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyEvidenceType,
    ResolvedTargetBusinessKey,
    ResolvedTableContract,
)
from src.metadata.metadata_models import MetadataRow, MappingPolicy, ResolvedTableMappings
from src.cdc.cdc_window_resolver import CDCWindowResolver
from src.exceptions import (
    ReconciliationDuplicateKeyError,
    ReconciliationFailedError,
    ReconciliationNullKeyError,
)
from src.orchestrator.etl import ETL
from src.load.loader import Loader
from src.utils.db_utils import _verify_staging_target_convergence
from src.metadata.policy_governance import PolicyGovernanceService
from jobs import promote_table


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures & Helpers
# ─────────────────────────────────────────────────────────────────────────────

class _MockTargetConfig:
    db_type = "postgres"
    database = "target_db"
    schema = "dbo"


class _MockConnectionManager:
    def __init__(self, cfg_path=None):
        pass

    def get(self, tag, expected_role=None):
        return _MockTargetConfig()


def _make_meta_row(**kwargs):
    defaults = {
        "id": 200,
        "source_tag": "src_pg",
        "source_schema": "public",
        "source_table": "ulog",
        "source_db_type": "postgres",
        "landing_table": "ulog",
        "history_table": "ulog",
        "target_table": "ulog",
        "target_schema": "dbo",
        "target_tag": "land_target",
        "target_database_type": "postgres",
        "is_active": "y",
        "priority": 1,
        "load_type": "full",
        "is_cdc": "y",
        "cdc_columns": "updated_at, id",
        "cdc_p_days": 7,
        "delta_days": 1,
        "last_success_timestamp": None,
        "is_reconcile": "n",
        "reconcile_column": None,
        "reconcile_measure": "COUNT",
        "chunk_size": 1000,
        "num_partitions": 1,
        "partition_column": None,
        "source_key_prefix": None,
        "natural_key_column": "id",
    }
    defaults.update(kwargs)
    return MetadataRow(**defaults)


def _make_valid_timestamp_candidate():
    return {
        "strategy": "TIMESTAMP_HIGH_WATERMARK",
        "cursor_columns": ["updated_at", "id"],
        "cursor_types": ["TIMESTAMP", "INT64"],
        "mutation_mode": "MUTABLE",
        "cursor_finality_mode": "UNSAFE_OR_UNKNOWN",
    }


def _make_valid_master_row():
    return {
        "table_master_key": 200,
        "database_type": "postgres",
        "schema_name": "public",
        "table_name": "ulog",
        "cdc_columns": "updated_at",
        "business_key": "id",
        "key_candidate_columns": ["id"],
        "key_evidence_type": "PRIMARY_KEY",
        "key_source": "PRIMARY_KEY",
        "key_confidence": 100,
        "key_candidate_nullable": False,
        "key_candidate_compatible": True,
        "table_row_count": 500,
        "cdc_strategy": "TIMESTAMP_HIGH_WATERMARK",
        "cdc_provider": "TIMESTAMP_HIGH_WATERMARK",
        "cdc_source": "PROFILED",
        "cdc_confidence": 95,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "key_evidence_fingerprint": "fp_evidence_001",
    }


def _make_approved_promotion_key(col="id"):
    return PromotionKeyContract(
        source_table_master_key=200,
        candidate_columns=(col,),
        evidence_type="PRIMARY_KEY",
        approval_state=KeyApprovalState.APPROVED,
        approved_source_business_key=(col,),
        approved_key_evidence="PRIMARY_KEY",
        approved_key_evidence_fingerprint="fp_evidence_001",
        approved_by="dba",
        approved_at=datetime.now(timezone.utc),
        approval_revision=1,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. Catalog promotion remains CDC-disabled until an immutable policy exists
# ─────────────────────────────────────────────────────────────────────────────

def test_catalog_promotion_never_activates_timestamp_cdc_without_policy(monkeypatch):
    monkeypatch.setattr(promote_table, "ConnectionManager", _MockConnectionManager)
    monkeypatch.setattr(
        promote_table.RecommendationService,
        "get_effective_recommendation",
        lambda _conn, _table_id: MagicMock(
            allowed=True,
            cdc_column="updated_at, id",
            cdc_type="TIMESTAMP_HIGH_WATERMARK",
            strategy="TIMESTAMP_HIGH_WATERMARK",
            reason="approved",
        ),
    )

    with patch("src.metadata.target_key_resolver.TargetKeyResolver.validate_target_physical_schema") as mock_val:
        mock_val.return_value = True

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchone.side_effect = [
            _make_valid_master_row(),
            None,
            (99,),  # INSERT RETURNING table_id
        ]
        mock_cur.fetchall.return_value = []

        res = promote_table._promote_one_table(
            meta_conn=mock_conn,
            source_tag="src_pg",
            source_database="src_db",
            table_name="ulog",
            target_tag="land_target",
            priority=1,
            force=True,
            approve_physical_primary_key=True,
            approved_by="steward",
            cdc_p_days=7,
        )

        assert res is True
        insert_params = [
            c[0][1] for c in mock_cur.execute.call_args_list
            if "INSERT INTO metadata.db_tables_etl" in c[0][0]
        ][0]
        # The raw catalog promotion cannot create a frozen execution policy.
        # PolicyGovernanceService activates CDC after immutable promotion.
        assert insert_params[19] == "n"


def test_immutable_timestamp_policy_is_the_only_cdc_activation_point():
    """CDC is enabled only in the same transaction that freezes policy data."""
    policy = ResolvedCDCPolicy.build(
        table_id=200,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("updated_at", "id"),
        cursor_types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        tie_breaker_columns=("id",),
    )
    key = ApprovedBusinessKey(
        columns=("id",), evidence_type=KeyEvidenceType.PRIMARY_KEY, evidence_fingerprint="src-fp"
    )
    target_key = ResolvedTargetBusinessKey.build(
        table_id=200,
        source_key_columns=("id",),
        target_key_columns=("id",),
        source_key_evidence_fingerprint="src-fp",
        mapping_fingerprint="map-fp",
    )
    mappings = ResolvedTableMappings.build(table_id=200, raw_mappings=[], mapping_policy=MappingPolicy.IDENTITY)
    candidate = CandidateTableContract.build(
        table_id=200,
        mapping_policy=MappingPolicy.IDENTITY,
        approved_source_business_key=key,
        resolved_mappings=mappings,
        canonical_target_business_key=target_key,
        cdc_policy=policy,
    )
    cm = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.side_effect = [
        (
            200, "IDENTITY", "src", "ulog", "land_target", "landing", "ulog", None,
            ["id"], "PRIMARY_KEY", "src-fp", "operator", datetime.now(timezone.utc),
            1, False, True, "src-fp", "PROFILED", 95, "OK", 0,
        ),
        (datetime.now(timezone.utc),),
    ]
    resolver = MagicMock()
    service = PolicyGovernanceService(cm, mapper=MagicMock(), resolver=resolver)

    with patch.object(service, "build_candidate_contract", return_value=candidate):
        principal = AuthenticatedPrincipal(actor_id="cdc-profiler", actor_type="SYSTEM", roles={"ADMIN"})
        service.promote_policy(PromotionRequest(
            table_id=200,
            expected_candidate_fingerprint=candidate.candidate_fingerprint,
            actor_type="SYSTEM",
            actor_id="cdc-profiler",
            reason="profiled timestamp policy",
            authenticated_principal=principal,
        ))

    # Physical target readiness is established by the landing-job preflight,
    # not by source-policy promotion. The target may not exist yet.
    resolver.validate_target_physical_schema.assert_not_called()
    update_params = [
        call.args[1]
        for call in cur.execute.call_args_list
        if "SET current_policy_version_id" in call.args[0]
    ][0]
    assert update_params[1] == "y"
    assert update_params[2] == "TIMESTAMP_HIGH_WATERMARK"
    assert update_params[4] == "updated_at, id"


def test_promoter_activates_only_clean_profiled_policy_after_pk_approval(monkeypatch):
    """The normal promotion path persists a candidate only after Tier-2 and PK approval exist."""
    service = MagicMock()
    service.build_candidate_contract.return_value = SimpleNamespace(candidate_fingerprint="candidate-fp")
    monkeypatch.setattr(promote_table, "PolicyGovernanceService", lambda _cm: service)

    activated = promote_table._promote_profiled_cdc_policy(
        conn_manager=MagicMock(),
        table_id=200,
        master={
            "table_name": "ulog",
            "cdc_strategy": "TIMESTAMP_HIGH_WATERMARK",
            "cdc_source": "PROFILED",
            "cdc_health": "OK",
            "cdc_confidence": 95,
            "cdc_null_rows": 0,
            "cdc_columns": "updated_at,id",
        },
        master_columns=[
            {"column_name": "updated_at", "data_type": "timestamp"},
            {"column_name": "id", "data_type": "bigint"},
        ],
        promotion_key=SimpleNamespace(approved_source_business_key=("id",)),
        actor_id="operator-1",
    )

    assert activated is True
    assert service.set_candidate_cdc_policy.call_args.kwargs["strategy"] == CDCStrategy.TIMESTAMP_HIGH_WATERMARK
    request = service.promote_policy.call_args.args[0]
    assert request.expected_candidate_fingerprint == "candidate-fp"
    assert request.actor_type == "SYSTEM"


def test_promoter_does_not_activate_profiled_cdc_without_approved_key(monkeypatch):
    service = MagicMock()
    monkeypatch.setattr(promote_table, "PolicyGovernanceService", lambda _cm: service)

    activated = promote_table._promote_profiled_cdc_policy(
        conn_manager=MagicMock(),
        table_id=200,
        master={"table_name": "ulog", "cdc_strategy": "SEQUENCE_HIGH_WATERMARK"},
        master_columns=[],
        promotion_key=SimpleNamespace(approved_source_business_key=None),
        actor_id="operator-1",
    )

    assert activated is False
    service.set_candidate_cdc_policy.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 2. Missing requirements leave is_cdc = 'n'
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# 3. Governed timestamp run persists and reuses frozen window
# ─────────────────────────────────────────────────────────────────────────────

def test_governed_timestamp_run_persists_and_reuses_frozen_window():
    mock_cm = MagicMock()
    mock_meta_conn = MagicMock()
    mock_meta_cur = MagicMock()
    mock_meta_conn.cursor.return_value.__enter__.return_value = mock_meta_cur
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_meta_conn

    mock_src_conn = MagicMock()
    mock_src_cur = MagicMock()
    mock_src_conn.cursor.return_value.__enter__.return_value = mock_src_cur
    mock_cm.get_db_connection.return_value.__enter__.return_value = mock_src_conn

    policy = ResolvedCDCPolicy.build(
        table_id=200,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("updated_at", "id"),
        cursor_types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        tie_breaker_columns=("id",),
    )
    app_key = ApprovedBusinessKey(
        columns=("id",),
        evidence_type=KeyEvidenceType.PRIMARY_KEY,
        evidence_fingerprint="fp_evidence_001",
    )
    contract = ResolvedTableContract(
        table_id=200,
        policy_version_id="pol-001",
        version_number=1,
        policy_fingerprint="fp-001",
        approved_source_business_key=app_key,
        resolved_mappings=ResolvedTableMappings.build(table_id=200, raw_mappings=[], mapping_policy=MappingPolicy.IDENTITY),
        canonical_target_business_key=ResolvedTargetBusinessKey.build(
            table_id=200,
            source_key_columns=("id",),
            target_key_columns=("id",),
            source_key_evidence_fingerprint="fp_evidence_001",
            mapping_fingerprint="fp_map_001",
        ),
        contract_payload={},
        contract_fingerprint="fp-001",
        cdc_policy=policy,
    )

    resolver = CDCWindowResolver(mock_cm)

    # First attempt: window is computed from source and persisted
    mock_meta_cur.fetchone.side_effect = [
        None,  # attempt window check -> None
        (json.dumps(CDCCursor(1, CDCStrategy.TIMESTAMP_HIGH_WATERMARK, ("updated_at", "id"), (CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64), ("2026-09-01 12:00:00", 10)).to_dict()),),  # committed cursor
        ("win-101", datetime(2026, 9, 2, 12, 0, 0)),  # insert attempt window RETURNING
    ]
    mock_src_cur.fetchone.return_value = (datetime(2026, 9, 2, 12, 0, 0), 42)

    window1 = resolver.resolve_and_persist_attempt_window(
        run_id="run-101",
        table_id=200,
        attempt_number=1,
        contract=contract,
        source_tag="src_pg",
        source_schema="public",
        source_table="ulog",
        overlap_days=7,
    )
    assert window1.strategy == CDCStrategy.TIMESTAMP_HIGH_WATERMARK
    assert window1.upper_cursor.values == ("2026-09-02T12:00:00", 42)

    # Retry attempt: reloads frozen window from cdc_attempt_window without re-querying source
    mock_meta_cur.fetchone.side_effect = [
        (
            "win-101", "run-101", 200, 1,
            "pol-001", policy.cdc_policy_fingerprint, "fp_win_001",
            json.dumps(CDCCursor(1, CDCStrategy.TIMESTAMP_HIGH_WATERMARK, ("updated_at", "id"), (CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64), ("2026-09-01 12:00:00", 10)).to_dict()),
            json.dumps(CDCCursor(1, CDCStrategy.TIMESTAMP_HIGH_WATERMARK, ("updated_at", "id"), (CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64), ("2026-09-02T12:00:00", 42)).to_dict()),
            datetime(2026, 9, 2, 12, 0, 0),
        ),
    ]

    window2 = resolver.resolve_and_persist_attempt_window(
        run_id="run-101",
        table_id=200,
        attempt_number=1,
        contract=contract,
        source_tag="src_pg",
        source_schema="public",
        source_table="ulog",
        overlap_days=7,
    )
    assert window2.upper_cursor.values == window1.upper_cursor.values
    assert window2.lower_cursor.values == window1.lower_cursor.values


# ─────────────────────────────────────────────────────────────────────────────
# 4. Equal timestamps with different id values correctly bounded
# ─────────────────────────────────────────────────────────────────────────────

def test_equal_timestamps_with_different_id_values_correctly_bounded():
    resolver = CDCWindowResolver(MagicMock())
    window = ResolvedCDCWindow(
        window_id="win-001",
        run_id="run-001",
        table_id=200,
        attempt_number=1,
        policy_version_id="pol-001",
        cdc_policy_fingerprint="fp_pol_001",
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        lower_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
            columns=("updated_at", "id"),
            values=("2026-09-01 00:00:00", 100),
            types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        ),
        upper_cursor=CDCCursor(
            schema_version=1,
            strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
            columns=("updated_at", "id"),
            values=("2026-09-02 00:00:00", 500),
            types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64),
        ),
        window_fingerprint="fp_win_001",
        captured_at=datetime.now(timezone.utc),
    )

    sql = resolver.build_extraction_filter_sql(window, dialect="postgres", overlap_days=7)
    # Must enforce tie breaker comparison for upper bound:
    # ("updated_at" < '2026-09-02 00:00:00' OR ("updated_at" = '2026-09-02 00:00:00' AND "id" <= 500))
    assert '("updated_at" < \'2026-09-02 00:00:00\' OR ("updated_at" = \'2026-09-02 00:00:00\' AND "id" <= 500))' in sql
    # Must enforce overlap window:
    assert '"updated_at" >= (\'2026-09-01 00:00:00\'::timestamp - INTERVAL \'7 days\')' in sql


# ─────────────────────────────────────────────────────────────────────────────
# 5. Incremental path uses UPSERT and never TRUNCATE
# ─────────────────────────────────────────────────────────────────────────────

def test_incremental_path_uses_upsert_and_never_truncates():
    mock_cm = MagicMock()
    mock_spark = MagicMock()
    mock_target_cfg = MagicMock()
    mock_cm.get.return_value = mock_target_cfg

    loader = Loader(spark=mock_spark, conn_manager=mock_cm)

    # Mock _upsert_via_staging
    with patch.object(loader, "_upsert_via_staging") as mock_upsert, \
         patch.object(loader, "write_target_overwrite") as mock_overwrite:
        mock_upsert.return_value = ("dbo.ulog", {"target_persisted_rows": 1})

        meta = _make_meta_row(is_cdc="y", natural_key_column="id")

        mock_df = MagicMock()
        res = loader.write_target(
            df=mock_df,
            meta=meta,
            merge_key="id",
            before_target_dml=lambda: None,
        )

        assert res == ("dbo.ulog", {"target_persisted_rows": 1})
        mock_upsert.assert_called_once()
        mock_overwrite.assert_not_called()


def test_governed_cdc_requires_policy_inside_the_frozen_contract():
    """A worker must not re-materialize mutable policy after submission."""
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = MagicMock()
    meta = _make_meta_row(is_cdc="y")
    meta.table_contract = MagicMock(cdc_policy=None)

    with patch("src.metadata.policy_governance.PolicyGovernanceService.materialize_execution_contract") as reload_policy:
        with pytest.raises(Exception, match="will not re-read mutable metadata"):
            pipeline._build_governed_cdc_filter(meta, "run-frozen-policy")
    reload_policy.assert_not_called()


def _frozen_cdc_contract(policy_version_id="pol-raw-key-001"):
    return SimpleNamespace(
        policy_version_id=policy_version_id,
        approved_source_business_key=SimpleNamespace(columns=("id",)),
        cdc_policy=object(),
    )


def test_raw_cdc_duplicate_keys_disable_cdc_before_target_mutation():
    """A malformed source delta must not be silently deduplicated then upserted."""
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = MagicMock()
    logger = MagicMock()
    meta = _make_meta_row(is_cdc="y")
    meta.table_contract = _frozen_cdc_contract()

    raw_df = MagicMock()
    raw_df.filter.return_value.count.return_value = 0
    raw_df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 2

    with patch("src.orchestrator.etl.F") as mock_functions:
        mock_functions.col.return_value.isNull.return_value = MagicMock()
        mock_functions.col.return_value.__gt__.return_value = MagicMock()
        with pytest.raises(ReconciliationDuplicateKeyError, match="duplicate_keys=2"):
            pipeline._validate_raw_cdc_key_window(meta, raw_df, log_id=91, logger=logger)

    raw_df.groupBy.assert_called_once_with("id")
    metrics = logger.update_stage_metrics.call_args.args[1]["cdc"]
    assert metrics["reconciliation_outcome"] == "REJECTED"
    assert metrics["next_execution_mode"] == "REPROFILE_REQUIRED"
    assert metrics["duplicate_key_count"] == 2


def test_raw_cdc_null_keys_disable_cdc_without_deduplicating():
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = MagicMock()
    meta = _make_meta_row(is_cdc="y")
    meta.table_contract = _frozen_cdc_contract()

    raw_df = MagicMock()
    raw_df.filter.return_value.count.return_value = 1

    with patch("src.orchestrator.etl.F") as mock_functions:
        mock_functions.col.return_value.isNull.return_value = MagicMock()
        with pytest.raises(ReconciliationNullKeyError, match="null_keys=1"):
            pipeline._validate_raw_cdc_key_window(meta, raw_df, log_id=None, logger=None)

    raw_df.groupBy.assert_not_called()


def test_raw_cdc_key_rejection_happens_before_any_target_operation():
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = MagicMock()
    pipeline.extractor = MagicMock()
    pipeline.extractor.read.return_value = MagicMock()
    pipeline._freeze_schema_if_identity = MagicMock()
    pipeline._build_governed_cdc_filter = MagicMock(return_value="updated_at >= '2026-09-01'")
    pipeline._validate_raw_cdc_key_window = MagicMock(
        side_effect=ReconciliationDuplicateKeyError("duplicate keys in raw CDC window")
    )
    pipeline.loader = MagicMock()
    meta = _make_meta_row(is_cdc="y")

    with patch("src.orchestrator.etl.Reconciler") as reconciler:
        reconciler.count_and_cache.return_value = 10
        with pytest.raises(ReconciliationDuplicateKeyError):
            pipeline._process_gold_table(meta, run_id="run-raw-key-fail", log_id=92, logger=MagicMock())

    pipeline.loader.provision_target_schema_preflight.assert_not_called()
    pipeline.loader.write_target.assert_not_called()


def test_target_reconciliation_reads_one_aggregate_not_source_keys():
    spark = MagicMock()
    reader = MagicMock()
    result_df = MagicMock()
    spark.read.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = result_df
    result_df.first.return_value = {"staged_rows": 10, "matched_rows": 10}

    with patch("src.utils.db_utils.apply_jdbc_options", return_value=reader):
        evidence = _verify_staging_target_convergence(
            spark=spark,
            target_cfg={"url": "jdbc:postgresql://example/db", "user": "u", "password": "p", "driver": "org.postgresql.Driver"},
            target_table="landing.ulog",
            stage_table="landing.ulog_stg",
            merge_key="id",
            db_type="postgres",
            expected_rows=10,
        )

    assert evidence["matched_rows"] == 10
    assert evidence["proof_mode"] == "STAGING_TARGET_LEFT_JOIN"
    assert "LEFT JOIN" in reader.option.call_args_list[-1].args[1]
    result_df.collect.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# 6. Target failure or reconciliation failure does not advance cursor
# ─────────────────────────────────────────────────────────────────────────────

def test_target_write_failure_does_not_advance_cursor():
    mock_cm = MagicMock()
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = mock_cm
    mock_logger = MagicMock()

    meta = _make_meta_row(is_cdc="y", natural_key_column="id", cdc_p_days=7)

    pipeline.extractor = MagicMock()
    mock_df = MagicMock()
    pipeline.extractor.read.return_value = mock_df
    pipeline._freeze_schema_if_identity = MagicMock()
    pipeline._build_governed_cdc_filter = MagicMock(return_value="updated_at >= '2026-08-25'")
    pipeline._validate_raw_cdc_key_window = MagicMock()
    pipeline._commit_governed_cdc_window = MagicMock()

    pipeline.loader = MagicMock()
    pipeline.loader.write_target.side_effect = RuntimeError("Target DB connection severed during UPSERT")

    with patch("src.orchestrator.etl.Transformer") as mock_tf_cls, \
         patch("src.orchestrator.etl.Reconciler") as mock_rec, \
         patch("src.orchestrator.etl.SchemaValidator"):
        mock_tf = mock_tf_cls.return_value
        mock_tf.add_source_identification_key.return_value = mock_df
        mock_tf.deduplicate.return_value = mock_df
        mock_tf.add_business_hash.return_value = mock_df
        mock_tf.add_audit_columns.return_value = mock_df
        mock_tf.cast_columns.return_value = mock_df
        mock_tf.select_target_columns.return_value = mock_df
        mock_rec.count_and_cache.side_effect = [10, 10]

        with pytest.raises(RuntimeError, match="Target DB connection severed during UPSERT"):
            pipeline._process_gold_table(meta, run_id="run-err-01", log_id=456, logger=mock_logger)

    # Cursor advancement MUST NOT have been called
    pipeline._commit_governed_cdc_window.assert_not_called()


def test_reconciliation_failure_does_not_advance_cursor():
    mock_cm = MagicMock()
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = mock_cm
    mock_logger = MagicMock()

    meta = _make_meta_row(is_cdc="y", natural_key_column="id", cdc_p_days=7)

    mock_contract = MagicMock()
    mock_contract.cdc_policy = _make_valid_timestamp_candidate()
    mock_contract.contract_fingerprint = "fp_contract_01"
    meta.table_contract = mock_contract

    mock_window = MagicMock()
    mock_window.lower_cursor.values = ("2026-09-01 00:00:00", 10)
    mock_window.upper_cursor.values = ("2026-09-02 00:00:00", 50)
    meta.resolved_cdc_window = mock_window

    pipeline.extractor = MagicMock()
    mock_df = MagicMock()
    pipeline.extractor.read.return_value = mock_df
    pipeline._freeze_schema_if_identity = MagicMock()
    pipeline._build_governed_cdc_filter = MagicMock(return_value="updated_at >= '2026-08-25'")
    pipeline._validate_raw_cdc_key_window = MagicMock()
    pipeline._commit_governed_cdc_window = MagicMock()

    pipeline.loader = MagicMock()
    pipeline.loader.write_target.return_value = (
        "dbo.ulog",
        {
            "target_persisted_rows": 10,
            "target_reconciliation": {
                "proof_mode": "STAGING_TARGET_LEFT_JOIN",
                "staging_table": "dbo.ulog_stg",
                "staged_rows": 10,
                "matched_rows": 10,
                "missing_rows": 0,
            },
        },
    )

    with patch("src.orchestrator.etl.Transformer") as mock_tf_cls, \
         patch("src.orchestrator.etl.Reconciler") as mock_rec, \
         patch("src.orchestrator.etl.SchemaValidator"), \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.freeze_persistence_manifest") as mock_mf, \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.reconcile_staged_target_convergence") as mock_recon:
        mock_tf = mock_tf_cls.return_value
        mock_tf.add_source_identification_key.return_value = mock_df
        mock_tf.deduplicate.return_value = mock_df
        mock_tf.add_business_hash.return_value = mock_df
        mock_tf.add_audit_columns.return_value = mock_df
        mock_tf.cast_columns.return_value = mock_df
        mock_tf.select_target_columns.return_value = mock_df
        mock_rec.count_and_cache.side_effect = [10, 10]
        mock_mf.return_value = MagicMock()
        mock_recon.side_effect = ReconciliationFailedError("Keyed convergence mismatch")

        with pytest.raises(ReconciliationFailedError):
            pipeline._process_gold_table(meta, run_id="run-recon-err", log_id=456, logger=mock_logger)

        pipeline._commit_governed_cdc_window.assert_not_called()
        # Ensure telemetry was recorded with REJECTED outcome
        metrics_calls = [c[0][1] for c in mock_logger.update_stage_metrics.call_args_list if "cdc" in c[0][1]]
        assert len(metrics_calls) > 0
        assert metrics_calls[-1]["cdc"]["reconciliation_outcome"] == "REJECTED"


# ─────────────────────────────────────────────────────────────────────────────
# 7. Audit telemetry payload verification
# ─────────────────────────────────────────────────────────────────────────────

def test_stage_metrics_contains_audit_telemetry():
    mock_cm = MagicMock()
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = mock_cm
    pipeline.spark = MagicMock()
    mock_logger = MagicMock()

    meta = _make_meta_row(is_cdc="y", natural_key_column="id", cdc_p_days=7)

    mock_contract = MagicMock()
    mock_contract.cdc_policy = _make_valid_timestamp_candidate()
    mock_contract.contract_fingerprint = "fp_contract_01"
    meta.table_contract = mock_contract

    mock_window = MagicMock()
    mock_window.lower_cursor.values = ("2026-09-01 00:00:00", 10)
    mock_window.upper_cursor.values = ("2026-09-02 00:00:00", 50)
    meta.resolved_cdc_window = mock_window

    pipeline.extractor = MagicMock()
    mock_df = MagicMock()
    pipeline.extractor.read.return_value = mock_df
    pipeline._freeze_schema_if_identity = MagicMock()
    pipeline._build_governed_cdc_filter = MagicMock(return_value="updated_at >= '2026-08-25'")
    pipeline._validate_raw_cdc_key_window = MagicMock()
    pipeline._commit_governed_cdc_window = MagicMock()

    pipeline.loader = MagicMock()
    pipeline.loader.write_target.return_value = (
        "dbo.ulog",
        {
            "target_persisted_rows": 10,
            "target_reconciliation": {
                "proof_mode": "STAGING_TARGET_LEFT_JOIN",
                "staging_table": "dbo.ulog_stg",
                "staged_rows": 10,
                "matched_rows": 10,
                "missing_rows": 0,
            },
        },
    )

    mock_recon_res = MagicMock()
    mock_recon_res.reconciliation_id = 999
    mock_recon_res.outcome = ReconciliationOutcome.CONVERGED

    with patch("src.orchestrator.etl.Transformer") as mock_tf_cls, \
         patch("src.orchestrator.etl.Reconciler") as mock_rec, \
         patch("src.orchestrator.etl.SchemaValidator"), \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.freeze_persistence_manifest") as mock_mf, \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.reconcile_staged_target_convergence") as mock_rc:
        mock_tf = mock_tf_cls.return_value
        mock_tf.add_source_identification_key.return_value = mock_df
        mock_tf.deduplicate.return_value = mock_df
        mock_tf.add_business_hash.return_value = mock_df
        mock_tf.add_audit_columns.return_value = mock_df
        mock_tf.cast_columns.return_value = mock_df
        mock_tf.select_target_columns.return_value = mock_df
        mock_rec.count_and_cache.side_effect = [10, 10]
        mock_rec.count_delta.return_value = 10
        mock_rec.count_parquet.return_value = 10
        mock_mf.return_value = MagicMock()
        mock_rc.return_value = mock_recon_res

        pipeline._process_gold_table(meta, run_id="run-audit-01", log_id=789, logger=mock_logger)

        metrics_calls = [c[0][1] for c in mock_logger.update_stage_metrics.call_args_list if "cdc" in c[0][1]]
        assert len(metrics_calls) > 0
        cdc_payload = metrics_calls[-1]["cdc"]
        assert cdc_payload["rendered_filter"] == "updated_at >= '2026-08-25'"
        assert cdc_payload["lower_cursor"] == ["2026-09-01 00:00:00", 10]
        assert cdc_payload["upper_cursor"] == ["2026-09-02 00:00:00", 50]
        assert cdc_payload["overlap_days"] == 7
        assert cdc_payload["reconciliation_outcome"] == "CONVERGED"
        pipeline._commit_governed_cdc_window.assert_called_once_with(meta, "run-audit-01", 789)
        phases = [call.args[1] for call in mock_logger.update_phase.call_args_list]
        assert "GOLD_TRANSFORM" in phases
        assert "TARGET_PREFLIGHT" in phases
        pipeline.loader.provision_target_schema_preflight.assert_not_called()


def test_gold_cdc_progress_uses_current_batch_rows_not_full_target_count():
    """CDC progress must reflect this run's atomic upsert, not target history."""
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = MagicMock()
    pipeline.conn_manager.get.return_value = SimpleNamespace(schema="dbo", db_type="postgres")
    pipeline.conn_manager.resolve_jdbc.return_value = SimpleNamespace(schema="dbo", db_type="postgres")
    pipeline.spark = MagicMock()
    pipeline.extractor = MagicMock()
    pipeline._freeze_schema_if_identity = MagicMock()
    pipeline._build_governed_cdc_filter = MagicMock(return_value="updated_at >= '2026-09-01'")
    pipeline._validate_raw_cdc_key_window = MagicMock()
    pipeline._commit_governed_cdc_window = MagicMock()

    meta = _make_meta_row(is_cdc="y", natural_key_column="id", cdc_p_days=7)
    meta.table_contract = SimpleNamespace(
        cdc_policy=_make_valid_timestamp_candidate(),
        contract_fingerprint="fp_contract_telemetry",
    )
    meta.resolved_cdc_window = SimpleNamespace(
        window_id="window-telemetry",
        lower_cursor=SimpleNamespace(values=("2026-09-01", 1)),
        upper_cursor=SimpleNamespace(values=("2026-09-02", 10)),
    )

    dataframe = MagicMock()
    pipeline.extractor.read.return_value = dataframe
    pipeline.loader = MagicMock()
    pipeline.loader.write_target.return_value = (
        "jdbc:dbo.ulog",
        {
            "target_persisted_rows": 10,
            "target_reconciliation": {
                "proof_mode": "STAGING_TARGET_LEFT_JOIN",
                "staging_table": "dbo.ulog_stg",
                "staged_rows": 10,
                "matched_rows": 10,
                "missing_rows": 0,
            },
        },
    )

    target_df = MagicMock()
    target_df.columns = ["id"]
    target_df.count.return_value = 3_548_812
    target_reader = MagicMock()
    target_reader.option.return_value = target_reader
    target_reader.load.return_value = target_df
    logger = MagicMock()
    reconciliation = SimpleNamespace(reconciliation_id=999, outcome=ReconciliationOutcome.CONVERGED)

    with patch("src.orchestrator.etl.Transformer") as transformer_cls, \
         patch("src.orchestrator.etl.Reconciler") as reconciler, \
         patch("src.orchestrator.etl.SchemaValidator"), \
         patch("src.orchestrator.etl.apply_jdbc_options", return_value=target_reader), \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.freeze_persistence_manifest", return_value=MagicMock()), \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.reconcile_staged_target_convergence", return_value=reconciliation):
        transformer = transformer_cls.return_value
        transformer.add_source_identification_key.return_value = dataframe
        transformer.deduplicate.return_value = dataframe
        transformer.add_business_hash.return_value = dataframe
        transformer.add_audit_columns.return_value = dataframe
        transformer.cast_columns.return_value = dataframe
        transformer.select_target_columns.return_value = dataframe
        reconciler.count_and_cache.side_effect = [10, 10]

        result = pipeline._process_gold_table(meta, run_id="run-telemetry", log_id=99, logger=logger)

    progress_calls = [call for call in logger.update_chunk_progress.call_args_list if call.args and call.args[0] == 99]
    assert progress_calls
    assert progress_calls[-1].kwargs["actual_chunk_rows"] == 10
    assert progress_calls[-1].kwargs["actual_chunk_rows"] != target_df.count.return_value
    assert result[0] == 10
    assert result[1] == 10
    assert result[3] == 0


def test_target_count_failure_does_not_advance_governed_cdc_cursor():
    """The cursor stays uncommitted until every final target verification passes."""
    pipeline = ETL.__new__(ETL)
    pipeline.conn_manager = MagicMock()
    target_cfg = SimpleNamespace(schema="dbo", db_type="postgres")
    pipeline.conn_manager.get.return_value = target_cfg
    pipeline.conn_manager.resolve_jdbc.return_value = target_cfg
    pipeline.spark = MagicMock()
    pipeline.extractor = MagicMock()
    pipeline._freeze_schema_if_identity = MagicMock()
    pipeline._build_governed_cdc_filter = MagicMock(return_value='"id" > 0')
    pipeline._validate_raw_cdc_key_window = MagicMock()
    pipeline._commit_governed_cdc_window = MagicMock()
    pipeline.loader = MagicMock()

    meta = _make_meta_row(is_cdc="y", natural_key_column="id")
    meta.table_contract = SimpleNamespace(
        cdc_policy=object(),
        contract_fingerprint="contract-fingerprint",
    )
    meta.resolved_cdc_window = SimpleNamespace(
        window_id="window-1",
        lower_cursor=SimpleNamespace(values=(10,)),
        upper_cursor=SimpleNamespace(values=(20,)),
    )
    dataframe = MagicMock()
    pipeline.extractor.read.return_value = dataframe
    pipeline.loader.write_target.return_value = (
        "jdbc:dbo.ulog",
        {
            "target_persisted_rows": 10,
            "target_reconciliation": {
                "proof_mode": "STAGING_TARGET_LEFT_JOIN",
                "staging_table": "dbo.ulog_stg",
                "staged_rows": 10,
                "matched_rows": 10,
                "missing_rows": 0,
            },
        },
    )
    reconciliation = SimpleNamespace(
        reconciliation_id=uuid.uuid4(),
        outcome=ReconciliationOutcome.CONVERGED,
    )
    reader = MagicMock()
    reader.option.return_value = reader
    reader.load.side_effect = RuntimeError("target count query failed")

    with patch("src.orchestrator.etl.Transformer") as transformer_cls, \
         patch("src.orchestrator.etl.Reconciler") as reconciler, \
         patch("src.orchestrator.etl.SchemaValidator"), \
         patch("src.orchestrator.etl.apply_jdbc_options", return_value=reader), \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.freeze_persistence_manifest", return_value=MagicMock()), \
         patch("src.validation.strategy_reconciler.StrategyAwareReconciler.reconcile_staged_target_convergence", return_value=reconciliation):
        transformer = transformer_cls.return_value
        transformer.add_source_identification_key.return_value = dataframe
        transformer.deduplicate.return_value = dataframe
        transformer.add_business_hash.return_value = dataframe
        transformer.add_audit_columns.return_value = dataframe
        transformer.cast_columns.return_value = dataframe
        transformer.select_target_columns.return_value = dataframe
        reconciler.count_and_cache.side_effect = [10, 10]

        with pytest.raises(RuntimeError, match="target count query failed"):
            pipeline._process_gold_table(meta, run_id="run-count-failure", log_id=91, logger=MagicMock())

    pipeline._commit_governed_cdc_window.assert_not_called()
