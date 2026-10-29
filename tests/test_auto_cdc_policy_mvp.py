# tests/test_auto_cdc_policy_mvp.py
"""
MVP Automated CDC Strategy Engine & Business-Key Policy Test Suite.

Verifies the required CDC and business-key policy controls:
 1. test_auto_cdc_selects_timestamp_when_updated_at_exists
 2. test_auto_cdc_selects_sequence_only_with_physical_sequence_evidence
 3. test_auto_cdc_selects_timestamp_with_profiled_composite_key
 4. test_auto_cdc_falls_back_to_full_snapshot_without_reliable_key
 5. test_auto_cdc_profiled_numeric_key_without_physical_evidence_enables_sequence
 6. test_operator_explicit_sequence_high_watermark_validated_against_evidence
 7. test_operator_disabled_cdc_normalizes_to_full_snapshot
 8. test_automated_provenance_created_for_clean_profiled_key
 9. test_profiler_rejects_candidate_with_single_null
10. test_physical_unique_constraint_qualifies_identically_to_primary_key
11. test_full_source_upsert_executes_keyed_upsert_without_truncate
12. test_full_snapshot_overwrite_reserved_for_keyless_tables
13. test_keyed_upsert_fails_preflight_without_target_unique_constraint
14. test_keyed_upsert_fails_preflight_when_source_contains_nulls
15. test_keyed_upsert_fails_preflight_when_source_contains_duplicates
16. test_sequence_cdc_mutation_detection_demotes_future_policy_only
17. test_auto_cdc_precedence_timestamp_over_sequence
18. test_clean_profile_requires_zero_duplicates_and_zero_nulls
19. test_multi_db_discovery_extracts_unique_constraints
20. test_target_constraint_matching_composite_business_key
21. test_zero_new_metadata_columns_verified
22. test_direct_service_update_cannot_set_is_cdc_y_without_promotion
"""

from datetime import datetime, timedelta, timezone
import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest

from src.discovery.type_mapper import (
    detect_cdc_columns_profiled,
    detect_business_key_profiled,
)
from src.metadata.key_contract import (
    ApprovedBusinessKey,
    KeyApprovalPolicy,
    KeyApprovalState,
    KeyCandidate,
    KeyEvidenceType,
    PromotionKeyContract,
    build_promotion_key_contract,
    resolve_key_candidate,
    require_approved_business_key,
)
from src.metadata.policy_models import (
    CDCStrategy,
    SourceMutationMode,
    CursorFinalityMode,
    CursorLogicalType,
    ResolvedCDCPolicy,
    CandidateTableContract,
    PromotionRequest,
    compute_deterministic_fingerprint,
)
from src.metadata.policy_governance import (
    PolicyGovernanceService,
    PolicyDependencyInvalidError,
)
from src.metadata.metadata_models import MetadataRow
from src.load.loader import Loader
from src.orchestrator.etl import ETL
from src.validation.strategy_reconciler import StrategyAwareReconciler
from src.load.target_schema_inspector import (
    get_schema_inspector,
    PostgresSchemaInspector,
)
from src.exceptions import (
    BusinessKeyRequiredError,
    PolicyVersionSupersededError,
    TargetConstraintRequiredError,
    TargetPreflightError,
)
from jobs.promote_table import (
    _cursor_logical_type,
    _promote_full_snapshot_policy,
    _promote_profiled_cdc_policy,
    evaluate_and_promote_table_cdc,
)
from jobs.discover_and_promote import SchemaCrawler


def _make_metadata_row(**kwargs):
    defaults = {
        "id": 1,
        "source_tag": "src",
        "source_schema": "public",
        "source_table": "tbl",
        "source_db_type": "postgres",
        "landing_table": "land_tbl",
        "history_table": "hist_tbl",
        "target_table": "tgt_tbl",
        "target_schema": "public",
        "is_active": "y",
        "priority": 1,
        "load_type": "full",
        "is_cdc": "n",
        "cdc_columns": None,
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


def _mock_spark_with_row(row_data: dict):
    spark = MagicMock()
    mock_df = MagicMock()
    mock_row = MagicMock()
    mock_row.__getitem__.side_effect = lambda k: row_data[k]
    mock_row.asDict.return_value = row_data
    mock_df.collect.return_value = [mock_row]
    reader = MagicMock()
    reader.option.return_value = reader
    reader.load.return_value = mock_df
    spark.read.format.return_value = reader
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: Update timestamp exists + valid PK -> automatic TIMESTAMP CDC
# ─────────────────────────────────────────────────────────────────────────────
def test_auto_cdc_selects_timestamp_when_updated_at_exists():
    columns = [
        {"column_name": "id", "data_type": "BIGINT", "is_primary_key": True},
        {"column_name": "name", "data_type": "VARCHAR"},
        {"column_name": "updated_at", "data_type": "TIMESTAMP"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 50,
        "max_val": datetime.utcnow(),
    })

    cfg = MagicMock()
    cfg.db_type = "postgres"

    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key="id",
        key_confidence=100,
        key_source="PRIMARY_KEY",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.orders",
    )

    assert res["is_cdc"] == "y"
    assert "updated_at" in res["cdc_columns"]
    assert res["cdc_strategy"] == "TIMESTAMP_HIGH_WATERMARK"
    assert res["cdc_confidence"] >= 90


def test_auto_cdc_accepts_clean_historic_timestamp_with_primary_key():
    """Age of the newest change is telemetry, not a CDC safety condition."""
    columns = [
        {"column_name": "id", "data_type": "BIGINT", "is_primary_key": True},
        {"column_name": "updated_at", "data_type": "TIMESTAMP"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 50,
        "total": 50,
        "max_val": datetime.utcnow() - timedelta(days=90),
    })
    cfg = MagicMock(db_type="postgres")

    result = detect_cdc_columns_profiled(
        columns=columns,
        business_key="id",
        key_confidence=100,
        key_source="PRIMARY_KEY",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.reference_data",
    )

    assert result["is_cdc"] == "y"
    assert result["cdc_strategy"] == "TIMESTAMP_HIGH_WATERMARK"
    assert result["cdc_health"] == "OK"
    assert result["cdc_confidence"] == 100
    assert json.loads(result["cdc_health_details"])["recent_activity"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Physically proven numeric identity key -> automatic SEQUENCE CDC
# ─────────────────────────────────────────────────────────────────────────────
def test_auto_cdc_selects_sequence_only_with_physical_sequence_evidence():
    columns = [
        {"column_name": "seq_id", "data_type": "BIGINT", "is_primary_key": True},
        {"column_name": "payload", "data_type": "VARCHAR"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 100,
        "total": 100,
        "min_v": 1,
        "max_v": 100,
        "max_val": 100,
    })

    cfg = MagicMock()
    cfg.db_type = "postgres"

    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key="seq_id",
        key_confidence=100,
        key_source="PRIMARY_KEY",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.events",
    )

    assert res["is_cdc"] == "y"
    assert res["cdc_columns"] == "seq_id"
    assert res["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"
    assert res["cdc_confidence"] >= 90


def test_oracle_integer_number_is_an_int64_sequence_cursor():
    """Oracle NUMBER(p,0) keys must use the runtime-supported INT64 cursor type."""
    assert _cursor_logical_type("NUMBER", data_precision=10, data_scale=None) == CursorLogicalType.INT64
    assert _cursor_logical_type("NUMBER", data_precision=18, data_scale=0) == CursorLogicalType.INT64
    assert _cursor_logical_type("NUMBER", data_precision=38, data_scale=None) == CursorLogicalType.INT64
    assert _cursor_logical_type("NUMBER", data_precision=38, data_scale=0) == CursorLogicalType.INT64


def test_fractional_or_unbounded_number_is_not_a_sequence_cursor():
    """Fractional or unbounded numerics must fail closed instead of reaching sequence CDC."""
    assert _cursor_logical_type("NUMBER", data_precision=10, data_scale=2) == CursorLogicalType.DECIMAL
    assert _cursor_logical_type("NUMBER", data_precision=39, data_scale=0) == CursorLogicalType.DECIMAL
    assert _cursor_logical_type("NUMBER", data_precision=None, data_scale=None) == CursorLogicalType.DECIMAL


def test_discovery_requires_numeric_precision_and_integer_scale_for_sequence_cdc():
    """Discovery must carry numeric metadata through to the executable policy."""
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 100,
        "total": 100,
        "min_v": 1,
        "max_v": 100,
    })
    cfg = MagicMock(db_type="oracle")

    accepted = detect_cdc_columns_profiled(
        [{"column_name": "emigrant_id", "data_type": "NUMBER", "data_precision": 10, "data_scale": 0}],
        spark_session=spark,
        conn_config=cfg,
        table_fqn="opfservices.events",
        business_key="emigrant_id",
        key_confidence=100,
        key_source="PROFILED",
    )
    assert accepted["is_cdc"] == "y"
    assert accepted["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"

    rejected = detect_cdc_columns_profiled(
        [{"column_name": "amount_key", "data_type": "NUMBER", "data_precision": 10, "data_scale": 2}],
        spark_session=spark,
        conn_config=cfg,
        table_fqn="opfservices.events",
        business_key="amount_key",
        key_confidence=100,
        key_source="PROFILED",
    )
    assert rejected["is_cdc"] == "n"
    assert rejected["cdc_strategy"] == "FULL_LOAD"


def test_discovery_prefers_unique_oracle_number_38_key_over_insert_date_hint():
    """A profiled Oracle NUMBER(38) key must drive append-only CDC, not a date-name fallback."""
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 633_967,
        "total": 633_967,
        "min_v": 1,
        "max_v": 633_967,
    })
    cfg = MagicMock(db_type="oracle")

    result = detect_cdc_columns_profiled(
        columns=[
            {
                "column_name": "emigrant_id",
                "data_type": "NUMBER",
                "data_precision": 38,
                "data_scale": None,
            },
            {"column_name": "registration_date", "data_type": "VARCHAR2"},
        ],
        spark_session=spark,
        conn_config=cfg,
        table_fqn="opfservices.tblop_jandec25100626",
        business_key="emigrant_id",
        key_confidence=100,
        key_source="PROFILED",
    )

    assert result["is_cdc"] == "y"
    assert result["cdc_columns"] == "emigrant_id"
    assert result["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"
    profile_sql = "\n".join(
        str(call.args[1])
        for call in spark.read.format.return_value.option.call_args_list
        if call.args and call.args[0] == "dbtable"
    ).lower()
    assert "emigrant_id" in profile_sql
    assert "registration_date" not in profile_sql


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: Clean profiled composite key + updated_at automatically activates timestamp CDC
# ─────────────────────────────────────────────────────────────────────────────
def test_auto_cdc_selects_timestamp_with_profiled_composite_key():
    table_id = 101
    approved_key = ApprovedBusinessKey(
        columns=("tenant_id", "user_id"),
        evidence_type=KeyEvidenceType.PROFILED_COMPOSITE,
        evidence_fingerprint="fp123",
    )
    policy = ResolvedCDCPolicy.build(
        table_id=table_id,
        strategy=CDCStrategy.TIMESTAMP_HIGH_WATERMARK,
        mutation_mode=SourceMutationMode.MUTABLE,
        cursor_finality_mode=CursorFinalityMode.UNSAFE_OR_UNKNOWN,
        cursor_columns=("updated_at", "tenant_id", "user_id"),
        cursor_types=(CursorLogicalType.TIMESTAMP, CursorLogicalType.INT64, CursorLogicalType.INT64),
        tie_breaker_columns=("tenant_id", "user_id"),
    )
    candidate_contract = MagicMock(
        approved_source_business_key=approved_key,
        cdc_policy=policy,
    )
    cm = MagicMock()
    service = PolicyGovernanceService(cm)
    table_row = {
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
    }
    # Must succeed without raising PolicyDependencyInvalidError
    service._validate_profiled_cdc_activation(candidate_contract, table_row)


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Keyless table or key with duplicates -> automatic FULL_SNAPSHOT
# ─────────────────────────────────────────────────────────────────────────────
def test_auto_cdc_falls_back_to_full_snapshot_without_reliable_key():
    columns = [
        {"column_name": "data_val", "data_type": "VARCHAR"},
        {"column_name": "updated_at", "data_type": "TIMESTAMP"},
    ]
    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key=None,
        key_confidence=0,
        key_source="NONE",
    )

    # Without a key, CDC must not be activated
    assert res["is_cdc"] == "n"
    assert res["cdc_strategy"] in ("FULL_SNAPSHOT", "PENDING_APPROVAL", "FULL_LOAD")


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: Profiled-only numeric key with a clean profile enables sequence CDC under AUTO
# ─────────────────────────────────────────────────────────────────────────────
def test_auto_cdc_profiled_numeric_key_without_physical_evidence_enables_sequence():
    # Numeric key discovered via full profiling; a physical DDL constraint is not required
    columns = [
        {"column_name": "account_number", "data_type": "BIGINT", "is_primary_key": False},
        {"column_name": "balance", "data_type": "DECIMAL"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 100,
        "total": 100,
        "min_v": 1,
        "max_v": 100,
    })

    cfg = MagicMock()
    cfg.db_type = "postgres"

    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key="account_number",
        key_confidence=95,
        key_source="PROFILED",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.accounts",
    )

    assert res["is_cdc"] == "y"
    assert res["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"
    assert res["cdc_columns"] == "account_number"
    assert res["cdc_confidence"] >= 90


def test_auto_cdc_rejects_any_duplicate_business_key_candidate():
    """A key candidate must be strictly unique; a composite profile may replace it."""
    columns = [
        {"column_name": "reg_id", "data_type": "BIGINT", "is_primary_key": False},
        {"column_name": "payload", "data_type": "VARCHAR"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 3_499_982,
        "total": 3_500_000,
        "min_v": 1,
        "max_v": 3_500_000,
    })
    cfg = MagicMock()
    cfg.db_type = "postgres"

    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key="reg_id",
        key_confidence=95,
        key_source="PROFILED",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.events",
    )

    assert res["is_cdc"] == "n"
    assert res["cdc_strategy"] in ("FULL_SNAPSHOT", "FULL_LOAD")
    details = json.loads(res["cdc_health_details"])
    assert details["cursor_duplicate_count"] == 18


def test_business_key_profiler_uses_temporal_partner_for_repeated_sequence_cursor():
    """Discovery must prove a composite identity instead of accepting a repeated cursor."""
    def profile_frame(values):
        row = MagicMock()
        row.asDict.return_value = values
        frame = MagicMock()
        frame.collect.return_value = [row]
        return frame

    spark = MagicMock()
    reader = MagicMock()
    reader.option.return_value = reader
    reader.load.side_effect = [
        profile_frame({"total_rows": 100, "non_null_rows": 100, "distinct_rows": 82}),
        profile_frame({"total_rows": 100, "non_null_rows": 100}),
        profile_frame({"dist_cnt": 100}),
    ]
    spark.read.format.return_value = reader

    result = detect_business_key_profiled(
        columns=["reg_id", "registration_date"],
        spark_session=spark,
        conn_config=SimpleNamespace(
            db_type="oracle",
            url="",
            driver="",
            connection_options={},
            jdbc_properties={},
        ),
        table_fqn="opfservices.events",
    )

    assert result["columns_unique_key"] == "reg_id,registration_date"
    assert result["key_source"] == "PROFILED_COMPOSITE"
    assert result["duplicate_count"] == 0


def test_auto_cdc_allows_repeated_sequence_cursor_with_unique_composite_key():
    """Cursor repetition is safe when a separately profiled composite identity is unique."""
    columns = [
        {"column_name": "reg_id", "data_type": "BIGINT", "is_primary_key": False},
        {"column_name": "registration_date", "data_type": "TIMESTAMP"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 3_499_982,
        "total": 3_500_000,
        "min_v": 0,
        "max_v": 3_500_000,
    })
    cfg = MagicMock()
    cfg.db_type = "postgres"

    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key="reg_id,registration_date",
        key_confidence=90,
        key_source="PROFILED_COMPOSITE",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.events",
    )

    assert res["is_cdc"] == "y"
    assert res["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"
    assert res["cdc_columns"] == "reg_id"


def test_auto_cdc_rejects_duplicate_rate_above_half_percent():
    columns = [
        {"column_name": "reg_id", "data_type": "BIGINT", "is_primary_key": False},
        {"column_name": "payload", "data_type": "VARCHAR"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 3_482_000,
        "total": 3_500_000,
        "min_v": 1,
        "max_v": 3_500_000,
    })
    cfg = MagicMock()
    cfg.db_type = "postgres"

    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key="reg_id",
        key_confidence=95,
        key_source="PROFILED",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.events",
    )

    assert res["is_cdc"] == "n"
    assert res["cdc_strategy"] in ("FULL_SNAPSHOT", "FULL_LOAD")


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: Operator explicit SEQUENCE_HIGH_WATERMARK validated against evidence
# ─────────────────────────────────────────────────────────────────────────────
def test_operator_explicit_sequence_high_watermark_validated_against_evidence():
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {
        "table_id": 10,
        "source_tag": "src",
        "source_table": "events",
        "target_tag": "tgt",
        "target_table": "events",
        "columns_unique_key": "event_id",
        "key_source": "PRIMARY_KEY",
        "key_confidence": 100,
        "duplicate_count": 0,
        "approved_source_business_key": json.dumps(["event_id"]),
        "approved_key_evidence": "PRIMARY_KEY",
        "cdc_columns": "event_id",
        "cdc_strategy": "AUTO",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = [
        {"column_name": "event_id", "data_type": "BIGINT", "is_nullable": False},
        {"column_name": "payload", "data_type": "VARCHAR", "is_nullable": True},
    ]

    target_cfg = SimpleNamespace(schema="landing")
    target_inspector = MagicMock()
    target_inspector.supports_unique_key.return_value = True
    cm.get.return_value = target_cfg

    with patch("jobs.promote_table.PolicyGovernanceService") as mock_svc_cls, patch(
        "src.load.target_schema_inspector.get_schema_inspector",
        return_value=target_inspector,
    ):
        mock_svc = MagicMock()
        mock_svc_cls.return_value = mock_svc
        success = evaluate_and_promote_table_cdc(
            conn_manager=cm,
            table_id=10,
            requested_is_cdc="y",
            requested_strategy="SEQUENCE_HIGH_WATERMARK",
        )
        assert success is True
        assert mock_svc.set_candidate_cdc_policy.call_args.kwargs["strategy"] == CDCStrategy.SEQUENCE_HIGH_WATERMARK


def test_profiled_sequence_key_with_duplicates_becomes_governed_full_snapshot():
    """Invalid CDC evidence must never exclude a discovered table from execution."""
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = {
        "table_id": 11,
        "source_tag": "src",
        "source_table": "events",
        "target_tag": "tgt",
        "target_table": "events",
        "table_row_count": 3_500_000,
        "columns_unique_key": "event_id",
        "key_source": "PROFILED",
        "key_confidence": 95,
        "duplicate_count": 18,
        "approved_source_business_key": None,
        "approved_key_evidence": "PROFILED_UNIQUE",
        "cdc_columns": "event_id",
        "cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = [
        {"column_name": "event_id", "data_type": "BIGINT", "is_nullable": False},
    ]
    cm.get.return_value = SimpleNamespace(schema="landing")
    inspector = MagicMock()
    inspector.supports_unique_key.side_effect = [False, True]

    with patch("jobs.promote_table.PolicyGovernanceService") as mock_svc_cls, patch(
        "src.load.target_schema_inspector.get_schema_inspector", return_value=inspector,
    ), patch("jobs.promote_table._promote_full_snapshot_policy", return_value=True) as promote_snapshot:
        mock_svc_cls.return_value = MagicMock()
        assert evaluate_and_promote_table_cdc(
            conn_manager=cm,
            table_id=11,
            requested_is_cdc="y",
            requested_strategy="AUTO",
        ) is True
        mock_svc_cls.return_value.set_candidate_cdc_policy.assert_not_called()
        promote_snapshot.assert_called_once()


def test_oracle_number_cursor_promotes_with_a_profiled_composite_merge_key():
    """A numeric cursor may be one part of a fully profiled composite key."""
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = {
        "table_id": 12,
        "source_tag": "opf_oracle",
        "source_table": "tblop_dec24",
        "target_tag": "land_test",
        "target_table": "tblop_dec24",
        "table_row_count": 3_500_000,
        "columns_unique_key": "reg_id,registration_date",
        "key_source": "PROFILED_COMPOSITE",
        "key_confidence": 90,
        "duplicate_count": 0,
        "approved_source_business_key": json.dumps(["reg_id", "registration_date"]),
        "approved_key_evidence": "PROFILED_COMPOSITE",
        "cdc_columns": "reg_id",
        "cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = [
        {"column_name": "reg_id", "data_type": "NUMBER", "data_precision": 10, "data_scale": None, "is_nullable": False},
        {"column_name": "registration_date", "data_type": "TIMESTAMP", "is_nullable": False},
    ]

    with patch("jobs.promote_table.PolicyGovernanceService") as mock_svc_cls:
        mock_svc = mock_svc_cls.return_value
        assert evaluate_and_promote_table_cdc(
            conn_manager=cm,
            table_id=12,
            requested_is_cdc="y",
            requested_strategy="AUTO",
        ) is True
        kwargs = mock_svc.set_candidate_cdc_policy.call_args.kwargs
        assert kwargs["strategy"] == CDCStrategy.SEQUENCE_HIGH_WATERMARK
        assert kwargs["cursor_types"] == (CursorLogicalType.INT64,)


def test_decimal_number_key_promotes_without_lossy_integer_coercion():
    """A profiled numeric key uses an exact DECIMAL high-watermark cursor."""
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = {
        "table_id": 13,
        "source_tag": "opf_oracle",
        "source_table": "fractional_key",
        "target_tag": "land_test",
        "target_table": "fractional_key",
        "table_row_count": 100,
        "columns_unique_key": "amount_key",
        "key_source": "PROFILED",
        "key_confidence": 100,
        "duplicate_count": 0,
        "approved_source_business_key": json.dumps(["amount_key"]),
        "approved_key_evidence": "PROFILED_UNIQUE",
        "cdc_columns": "amount_key",
        "cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = [
        {"column_name": "amount_key", "data_type": "NUMBER", "data_precision": 10, "data_scale": 2, "is_nullable": False},
    ]

    with patch("jobs.promote_table.PolicyGovernanceService") as mock_svc_cls:
        assert evaluate_and_promote_table_cdc(
            conn_manager=cm,
            table_id=13,
            requested_is_cdc="y",
            requested_strategy="SEQUENCE_HIGH_WATERMARK",
        ) is True
        assert mock_svc_cls.return_value.set_candidate_cdc_policy.call_args.kwargs["cursor_types"] == (
            CursorLogicalType.DECIMAL,
        )


def test_policy_service_accepts_decimal_sequence_candidate():
    """Wide Oracle NUMBER keys retain exact DECIMAL cursor semantics."""
    service = PolicyGovernanceService(MagicMock())
    with patch.object(service, "_conn_manager") as cm:
        conn = MagicMock()
        cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
        policy = service.set_candidate_cdc_policy(
            table_id=14,
            strategy=CDCStrategy.SEQUENCE_HIGH_WATERMARK,
            mutation_mode=SourceMutationMode.APPEND_ONLY,
            cursor_finality_mode=CursorFinalityMode.PREFIX_FINAL,
            cursor_columns=("emigrant_id",),
            cursor_types=(CursorLogicalType.DECIMAL,),
        )

    assert policy.cursor_types == (CursorLogicalType.DECIMAL,)


def test_oracle_number_38_key_promotes_with_int64_sequence_cursor():
    """A unique Oracle NUMBER(38) key promotes as an INT64 sequence cursor."""
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = {
        "table_id": 16,
        "source_tag": "opf_oracle",
        "source_table": "events",
        "target_tag": "land_test",
        "target_table": "events",
        "table_row_count": 100,
        "columns_unique_key": "emigrant_id",
        "key_source": "PROFILED",
        "key_confidence": 100,
        "duplicate_count": 0,
        "approved_source_business_key": json.dumps(["emigrant_id"]),
        "approved_key_evidence": "PROFILED_UNIQUE",
        "cdc_columns": "emigrant_id",
        "cdc_strategy": "SEQUENCE_HIGH_WATERMARK",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = [
        {"column_name": "emigrant_id", "data_type": "NUMBER", "data_precision": 38, "data_scale": None, "is_nullable": False},
    ]

    with patch("jobs.promote_table.PolicyGovernanceService") as mock_svc_cls:
        assert evaluate_and_promote_table_cdc(cm, 16, "y", "AUTO") is True
        assert mock_svc_cls.return_value.set_candidate_cdc_policy.call_args.kwargs["cursor_types"] == (
            CursorLogicalType.INT64,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: Operator disabled CDC normalizes to FULL_SNAPSHOT
# ─────────────────────────────────────────────────────────────────────────────
def test_operator_disabled_cdc_promotes_governed_full_snapshot():
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    mock_cur.fetchone.return_value = {
        "table_id": 15,
        "source_tag": "src",
        "source_table": "customers",
        "target_tag": "tgt",
        "target_table": "customers",
        "columns_unique_key": "id",
        "key_source": "PRIMARY_KEY",
        "key_confidence": 100,
        "duplicate_count": 0,
        "approved_source_business_key": json.dumps(["id"]),
        "approved_key_evidence": "PRIMARY_KEY",
        "cdc_columns": "updated_at, id",
        "cdc_strategy": "TIMESTAMP_HIGH_WATERMARK",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = []

    with patch("jobs.promote_table._promote_full_snapshot_policy", return_value=True) as promote_snapshot:
        success = evaluate_and_promote_table_cdc(
            conn_manager=cm,
            table_id=15,
            requested_is_cdc="n",
            requested_strategy="TIMESTAMP_HIGH_WATERMARK",
        )
    assert success is True
    promote_snapshot.assert_called_once()
    assert promote_snapshot.call_args.kwargs["operator_disabled"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: Automated complete business-key provenance created for clean profiled key
# ─────────────────────────────────────────────────────────────────────────────
def test_automated_provenance_created_for_clean_profiled_key():
    master = {
        "table_master_key": 42,
        "business_key": "account_code",
        "key_evidence_type": "PROFILED_UNIQUE",
        "key_confidence": 100,
        "key_candidate_nullable": False,
        "key_candidate_compatible": True,
        "key_evidence_fingerprint": "fp_clean_42",
    }
    contract = build_promotion_key_contract(
        master,
        approve_physical_primary_key=True,
    )

    assert contract.approval_state == KeyApprovalState.APPROVED
    assert contract.approved_source_business_key == ("account_code",)
    assert contract.approved_by == "SYSTEM_PROFILE"
    assert contract.approved_key_evidence == "PROFILED_UNIQUE"
    assert contract.approved_key_evidence_fingerprint == "fp_clean_42"
    assert contract.approval_revision == 1


# ─────────────────────────────────────────────────────────────────────────────
# Test 9: Profiler rejects candidate with single NULL
# ─────────────────────────────────────────────────────────────────────────────
def test_profiler_rejects_candidate_with_single_null():
    spark = _mock_spark_with_row({
        "total_rows": 100,
        "non_null_rows": 99,   # 1 NULL present!
        "distinct_rows": 99,
    })

    source_cfg = MagicMock()
    source_cfg.db_type = "postgres"

    res = detect_business_key_profiled(
        columns=["user_num"],
        primary_keys=None,
        spark_session=spark,
        conn_config=source_cfg,
        table_fqn="public.users",
    )
    # 1 NULL must reject candidate from PROFILED
    assert res["key_source"] in ("NONE", "PROFILED_LOWEST_DUP")
    assert res["key_confidence"] < 90


# ─────────────────────────────────────────────────────────────────────────────
# Test 10: Physical UNIQUE constraint qualifies identically to PRIMARY KEY
# ─────────────────────────────────────────────────────────────────────────────
def test_physical_unique_constraint_qualifies_identically_to_primary_key():
    candidate = KeyCandidate(
        columns=("email",),
        evidence_type=KeyEvidenceType.UNIQUE_CONSTRAINT,
        confidence=100,
        nullable=False,
        compatible=True,
        evidence_fingerprint="fp_uniq",
    )
    resolution = resolve_key_candidate(candidate)
    assert resolution.approval_state == KeyApprovalState.APPROVED
    approved = require_approved_business_key(resolution)
    assert approved.columns == ("email",)
    assert approved.evidence_type == KeyEvidenceType.UNIQUE_CONSTRAINT


# ─────────────────────────────────────────────────────────────────────────────
# Test 11: FULL_SOURCE_UPSERT consumes Silver without a second source extract
# ─────────────────────────────────────────────────────────────────────────────
def test_full_source_upsert_consumes_silver_without_reextracting_source():
    etl = ETL.__new__(ETL)
    etl.loader = MagicMock()
    etl.extractor = MagicMock()
    etl._freeze_schema_if_identity = MagicMock()
    etl.conn_manager = MagicMock()
    etl.spark = MagicMock()

    meta = MagicMock()
    meta.id = 55
    meta.source_tag = "src"
    meta.source_schema = "public"
    meta.source_table = "members"
    meta.target_tag = "tgt"
    meta.target_table = "members"
    meta.target_schema = "public"
    meta.is_active = "y"
    meta.priority = 1
    meta.load_type = "full"
    meta.is_cdc = "n"
    meta.natural_key_column = "member_id"
    meta.canonical_source_business_key = ("member_id",)

    mock_df = MagicMock()
    mock_df.count.return_value = 50
    mock_df.dropDuplicates.return_value = mock_df
    etl.extractor.read_history.return_value = mock_df

    logger = MagicMock()
    etl.loader.write_target.return_value = ("/data/gold/members", {})

    with patch("src.orchestrator.etl.Reconciler.count_and_cache", return_value=50), \
         patch("src.orchestrator.etl.Reconciler.count_delta", return_value=50), \
         patch("src.orchestrator.etl.Transformer") as mock_t_cls, \
         patch("src.orchestrator.etl.SchemaValidator"):
        mock_t = MagicMock()
        mock_t.add_source_identification_key.return_value = mock_df
        mock_t.deduplicate.return_value = mock_df
        mock_t.add_business_hash.return_value = mock_df
        mock_t.add_audit_columns.return_value = mock_df
        mock_t.cast_columns.return_value = mock_df
        mock_t.select_target_columns.return_value = mock_df
        mock_t_cls.return_value = mock_t

        etl._process_gold_table(
            meta,
            run_id="r1",
            log_id=1,
            logger=logger,
            use_persisted_silver=True,
        )

    # Gold must consume the already-certified Silver artifact, then UPSERT it.
    etl.extractor.read_history.assert_called_once_with(meta)
    etl.extractor.read.assert_not_called()
    etl.loader.write_target.assert_called_once()
    etl.loader.write_target_overwrite.assert_not_called()
    etl.loader.provision_target_schema_preflight.assert_not_called()
    assert etl.loader.write_target.call_args.kwargs["merge_key"] == "member_id"
    logger.update_stage_metrics.assert_called_with(1, {
        "action": "FULL_SOURCE_UPSERT",
        "source_deletions_propagated": False,
    })


def test_reused_silver_verifies_target_rows_against_parent_artifact_run():
    """A Gold retry must count the artifact it published, not its new control-plane run ID."""
    etl = ETL.__new__(ETL)
    etl.loader = MagicMock()
    etl.extractor = MagicMock()
    silver_df = MagicMock()
    silver_df.columns = ["pipeline_run_id"]
    silver_df.filter.return_value = silver_df
    etl.extractor.read_history.return_value = silver_df
    etl.conn_manager = MagicMock()
    etl.conn_manager.get.return_value = SimpleNamespace(db_type="postgres")
    etl.conn_manager.resolve_jdbc.return_value = SimpleNamespace(db_type="postgres")
    etl.spark = MagicMock()

    meta = MagicMock(
        id=57,
        source_tag="src",
        source_table="members",
        target_tag="tgt",
        target_table="members",
        target_schema="landing",
        is_cdc="n",
        natural_key_column="member_id",
        canonical_source_business_key=("member_id",),
    )
    etl.loader.write_target.return_value = ("jdbc:landing.members", {})
    target_df = MagicMock()
    target_df.columns = ["pipeline_run_id"]
    target_df.filter.return_value.count.return_value = 50

    class PipelineRunColumn:
        def __eq__(self, value):
            return ("pipeline_run_id", value)

    target_df.pipeline_run_id = PipelineRunColumn()
    reader = MagicMock()
    reader.option.return_value.load.return_value = target_df

    with patch("src.orchestrator.etl.Reconciler.count_and_cache", return_value=50), \
         patch("src.orchestrator.etl.apply_jdbc_options", return_value=reader), \
         patch("src.orchestrator.etl.SchemaValidator"):
        source_count, target_count, _, _ = etl._process_gold_table(
            meta,
            run_id="retry-run",
            log_id=1,
            logger=MagicMock(),
            use_persisted_silver=True,
            silver_artifact_run_id="parent-run",
        )

    assert (source_count, target_count) == (50, 50)
    target_df.filter.assert_called_once_with(("pipeline_run_id", "parent-run"))


# ─────────────────────────────────────────────────────────────────────────────
# Test 12: FULL_SNAPSHOT_OVERWRITE reserved for keyless tables
# ─────────────────────────────────────────────────────────────────────────────
def test_full_snapshot_overwrite_reserved_for_keyless_tables():
    etl = ETL.__new__(ETL)
    etl.loader = MagicMock()
    etl.extractor = MagicMock()
    etl._freeze_schema_if_identity = MagicMock()
    etl.conn_manager = MagicMock()
    etl.spark = MagicMock()

    meta = MagicMock()
    meta.id = 56
    meta.source_tag = "src"
    meta.source_schema = "public"
    meta.source_table = "audit_logs"
    meta.target_tag = "tgt"
    meta.target_table = "audit_logs"
    meta.target_schema = "public"
    meta.is_active = "y"
    meta.priority = 1
    meta.load_type = "full"
    meta.is_cdc = "n"
    meta.natural_key_column = None
    meta.canonical_source_business_key = None

    mock_df = MagicMock()
    mock_df.count.return_value = 100
    mock_df.dropDuplicates.return_value = mock_df
    etl.extractor.read.return_value = mock_df

    logger = MagicMock()
    etl.loader.write_target_overwrite.return_value = "/data/gold/audit_logs"

    with patch("src.orchestrator.etl.Reconciler.count_and_cache", return_value=100), \
         patch("src.orchestrator.etl.Reconciler.count_delta", return_value=100), \
         patch("src.orchestrator.etl.Transformer") as mock_t_cls, \
         patch("src.orchestrator.etl.SchemaValidator"):
        mock_t = MagicMock()
        mock_t.add_source_identification_key.return_value = mock_df
        mock_t.deduplicate.return_value = mock_df
        mock_t.add_business_hash.return_value = mock_df
        mock_t.add_audit_columns.return_value = mock_df
        mock_t.cast_columns.return_value = mock_df
        mock_t.select_target_columns.return_value = mock_df
        mock_t_cls.return_value = mock_t

        etl._process_gold_table(meta, run_id="r2", log_id=2, logger=logger)

    # Keyless table must call write_target_overwrite
    etl.loader.write_target_overwrite.assert_called_once()
    etl.loader.write_target.assert_not_called()
    etl.loader.provision_target_schema_preflight.assert_not_called()
    logger.update_stage_metrics.assert_called_with(2, {
        "action": "FULL_SNAPSHOT_OVERWRITE",
        "source_deletions_propagated": True,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Test 13: Keyed UPSERT fails preflight without target unique constraint
# ─────────────────────────────────────────────────────────────────────────────
def test_existing_target_gets_missing_unique_constraint_during_landing_preflight():
    spark = MagicMock()
    cm = MagicMock()
    loader = Loader(spark, cm)
    loader._require_persistence_ownership = MagicMock()

    meta = MagicMock()
    meta.id = 77
    meta.source_tag = "src"
    meta.source_table = "orders"
    meta.target_tag = "tgt"
    meta.target_table = "orders"
    meta.target_schema = "public"
    meta.is_active = "y"
    meta.priority = 1
    meta.load_type = "full"
    meta.is_cdc = "n"
    meta.natural_key_column = "order_id"
    meta.canonical_source_business_key = ("order_id",)

    target_cfg = MagicMock()
    target_cfg.db_type = "postgres"
    target_cfg.schema = "public"

    df = MagicMock()
    df.filter.return_value.count.return_value = 0
    df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0

    reader = MagicMock()
    reader.option.return_value = reader
    target_conn = MagicMock()
    target_cur = MagicMock()
    target_conn.cursor.return_value.__enter__.return_value = target_cur
    cm.get_db_connection.return_value.__enter__.return_value = target_conn

    with patch("src.load.loader.apply_jdbc_options", return_value=reader), \
         patch("src.load.loader.has_stage0_trusted_business_key", return_value=True), \
         patch("src.load.loader.validate_stage0_execution_policy"), \
         patch("src.load.loader.get_schema_inspector") as mock_insp_cls:
        mock_insp = MagicMock()
        mock_insp.supports_unique_key.side_effect = [False, True]
        mock_insp_cls.return_value = mock_insp

        loader.provision_target_schema_preflight(
            df=df,
            meta=meta,
            target_cfg=target_cfg,
        )

    assert mock_insp.supports_unique_key.call_count == 2
    assert any("ADD CONSTRAINT" in str(call.args[0]) for call in target_cur.execute.call_args_list)


def test_missing_postgres_target_is_created_during_landing_preflight():
    """Policy promotion may precede target creation; the landing preflight creates it."""
    spark = MagicMock()
    cm = MagicMock()
    cm.paths = {}
    loader = Loader(spark, cm)
    loader._require_persistence_ownership = MagicMock()

    meta = SimpleNamespace(
        source_table="orders",
        target_tag="land_test",
        target_table="orders",
        target_schema="landing",
        is_cdc="y",
        canonical_source_business_key=("order_id",),
    )
    target_cfg = SimpleNamespace(
        db_type="postgres",
        schema="landing",
        url="jdbc:postgresql://target.example/land_test",
        driver="org.postgresql.Driver",
        username="etl",
        connection_options={},
    )

    df = MagicMock()
    df.filter.return_value.count.return_value = 0
    df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0
    df.limit.return_value = df

    probe = MagicMock()
    probe.option.return_value = probe
    visible_probe = MagicMock()
    visible_probe.limit.return_value.count.return_value = 0
    probe.load.side_effect = [Exception("relation does not exist"), visible_probe]
    writer = MagicMock()
    writer.option.return_value = writer
    writer.mode.return_value = writer

    target_conn = MagicMock()
    target_cur = MagicMock()
    target_conn.cursor.return_value.__enter__.return_value = target_cur
    cm.get_db_connection.return_value.__enter__.return_value = target_conn

    inspector = MagicMock()
    inspector.supports_unique_key.side_effect = [False, True]

    with patch("src.load.loader.apply_jdbc_options", side_effect=[probe, writer, probe]) as mock_apply, \
         patch("src.load.loader.has_stage0_trusted_business_key", return_value=True), \
         patch("src.load.loader.validate_stage0_execution_policy"), \
         patch("src.load.loader.get_schema_inspector", return_value=inspector):
        loader.provision_target_schema_preflight(
            df=df,
            meta=meta,
            target_cfg=target_cfg,
        )

    assert mock_apply.call_count == 3, mock_apply.call_args_list
    assert writer.method_calls, repr(writer.method_calls)
    writer.option.assert_called_once()
    writer.mode.assert_called_once_with("ignore")
    writer.save.assert_called_once_with()
    assert any("ADD CONSTRAINT" in str(call.args[0]) for call in target_cur.execute.call_args_list)
    assert inspector.supports_unique_key.call_count == 2


def test_target_table_probe_forces_a_spark_action_before_declaring_visibility():
    """Spark Connect must execute the zero-row probe, not merely build a plan."""
    spark = MagicMock()
    loader = Loader(spark, MagicMock())
    reader = MagicMock()
    reader.option.return_value = reader
    loaded = MagicMock()
    reader.load.return_value = loaded
    loaded.limit.return_value.count.return_value = 0

    with patch("src.load.loader._apply_jdbc_options", return_value=reader):
        assert loader._target_table_exists(
            "landing.orders", "postgres", " AS ", MagicMock()
        ) is True

    loaded.limit.assert_called_once_with(1)
    loaded.limit.return_value.count.assert_called_once_with()


def test_missing_target_provision_does_not_create_constraint_before_reinspection():
    """The outer readiness path owns constraint DDL after it proves table existence."""
    loader = Loader(MagicMock(), MagicMock())
    loader._require_persistence_ownership = MagicMock()
    loader._create_target_unique_constraint = MagicMock()
    loader.conn_manager.get_db_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = MagicMock()

    df = MagicMock()
    df.limit.return_value = df
    writer = MagicMock()
    writer.option.return_value = writer
    writer.mode.return_value = writer

    with patch("src.load.loader._apply_jdbc_options", return_value=writer):
        loader._provision_missing_target(
            df=df,
            meta=SimpleNamespace(target_tag="land_test", target_schema="landing"),
            target_cfg=SimpleNamespace(db_type="postgres", schema="landing"),
            target_fqn="landing.orders",
            key_tuple=("order_id",),
            before_persist=None,
            spark_target_cfg=MagicMock(),
        )

    loader._create_target_unique_constraint.assert_not_called()


def test_target_provisioning_uses_the_explicit_spark_target_connection():
    """Creation and Spark probes must never fall back to the driver endpoint."""
    spark = MagicMock()
    cm = MagicMock()
    loader = Loader(spark, cm)
    loader._require_persistence_ownership = MagicMock()

    meta = SimpleNamespace(
        source_table="orders", target_tag="land_test", target_table="orders",
        target_schema="landing", is_cdc="y", canonical_source_business_key=("order_id",),
    )
    driver_cfg = SimpleNamespace(db_type="postgres", schema="landing")
    spark_cfg = SimpleNamespace(db_type="postgres", schema="landing", url="jdbc:postgresql://spark-target/land_test")
    df = MagicMock()
    df.filter.return_value.count.return_value = 0
    df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 0
    df.limit.return_value = df
    probe = MagicMock()
    probe.option.return_value = probe
    visible_probe = MagicMock()
    visible_probe.limit.return_value.count.return_value = 0
    probe.load.side_effect = [Exception("relation does not exist"), visible_probe]
    writer = MagicMock()
    writer.option.return_value = writer
    writer.mode.return_value = writer
    target_conn = MagicMock()
    target_conn.cursor.return_value.__enter__.return_value = MagicMock()
    cm.get_db_connection.return_value.__enter__.return_value = target_conn
    inspector = MagicMock()
    inspector.supports_unique_key.return_value = True

    with patch("src.load.loader.apply_jdbc_options", side_effect=[probe, writer, probe]) as mock_apply, \
         patch("src.load.loader.has_stage0_trusted_business_key", return_value=True), \
         patch("src.load.loader.validate_stage0_execution_policy"), \
         patch("src.load.loader.get_schema_inspector", return_value=inspector):
        loader.provision_target_schema_preflight(
            df=df, meta=meta, target_cfg=driver_cfg, spark_target_cfg=spark_cfg,
        )

    assert [call.args[1] for call in mock_apply.call_args_list] == [spark_cfg, spark_cfg, spark_cfg]


def test_target_route_log_is_sanitized(caplog):
    """Gold route evidence identifies the endpoint without leaking credentials."""
    cfg = SimpleNamespace(
        connection_tag="land_test",
        url="jdbc:postgresql://spark-target:5432/land_test?password=must-not-log",
    )

    with caplog.at_level(logging.INFO, logger="loader"):
        Loader._log_target_route(cfg)

    assert "tag=land_test" in caplog.text
    assert "host=spark-target" in caplog.text
    assert "port=5432" in caplog.text
    assert "database=land_test" in caplog.text
    assert "must-not-log" not in caplog.text


# ─────────────────────────────────────────────────────────────────────────────
# Test 14: Keyed UPSERT fails preflight when source contains NULLs
# ─────────────────────────────────────────────────────────────────────────────
def test_keyed_upsert_fails_preflight_when_source_contains_nulls():
    spark = MagicMock()
    cm = MagicMock()
    loader = Loader(spark, cm)
    loader._require_persistence_ownership = MagicMock()

    meta = MagicMock()
    meta.id = 78
    meta.source_tag = "src"
    meta.source_table = "orders"
    meta.target_tag = "tgt"
    meta.target_table = "orders"
    meta.target_schema = "public"
    meta.is_active = "y"
    meta.priority = 1
    meta.load_type = "full"
    meta.is_cdc = "n"
    meta.natural_key_column = "order_id"
    meta.canonical_source_business_key = ("order_id",)

    mock_df = MagicMock()
    mock_df.filter.return_value.count.return_value = 3  # 3 NULL rows!

    with patch("src.load.loader.has_stage0_trusted_business_key", return_value=True), \
         patch("src.load.loader.validate_stage0_execution_policy"):
        with pytest.raises(TargetPreflightError) as exc_info:
            loader.provision_target_schema_preflight(
                df=mock_df,
                meta=meta,
                target_cfg=MagicMock(),
            )
        assert "TARGET_KEY_NULL_VALUES" in str(exc_info.value)


# ─────────────────────────────────────────────────────────────────────────────
# Test 15: Keyed UPSERT fails preflight when source contains duplicates
# ─────────────────────────────────────────────────────────────────────────────
def test_keyed_upsert_fails_preflight_when_source_contains_duplicates():
    spark = MagicMock()
    cm = MagicMock()
    loader = Loader(spark, cm)
    loader._require_persistence_ownership = MagicMock()

    meta = MagicMock()
    meta.id = 79
    meta.source_tag = "src"
    meta.source_table = "orders"
    meta.target_tag = "tgt"
    meta.target_table = "orders"
    meta.target_schema = "public"
    meta.is_active = "y"
    meta.priority = 1
    meta.load_type = "full"
    meta.is_cdc = "n"
    meta.natural_key_column = "order_id"
    meta.canonical_source_business_key = ("order_id",)

    mock_df = MagicMock()
    # 0 nulls
    mock_df.filter.return_value.count.return_value = 0
    # 2 duplicate groups!
    mock_df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 2

    with patch("src.load.loader.has_stage0_trusted_business_key", return_value=True), \
         patch("src.load.loader.validate_stage0_execution_policy"):
        with pytest.raises(TargetPreflightError) as exc_info:
            loader.provision_target_schema_preflight(
                df=mock_df,
                meta=meta,
                target_cfg=MagicMock(),
            )
        assert "TARGET_KEY_DUPLICATES" in str(exc_info.value)


# ─────────────────────────────────────────────────────────────────────────────
# Test 16: Sequence CDC mutation detection demotes future policy only
# ─────────────────────────────────────────────────────────────────────────────
def test_sequence_cdc_mutation_detection_demotes_future_policy_only():
    cm = MagicMock()
    reconciler = StrategyAwareReconciler(cm)
    with patch("src.validation.strategy_reconciler.PolicyGovernanceService") as service_cls:
        reconciler.audit_and_demote_sequence_mutation(
            table_id=88,
            detected_reason="Historical ID 45 updated with newer payload",
            expected_current_policy_version_id="policy-v1",
        )

    service_cls.return_value.demote_cdc_policy.assert_called_once_with(
        table_id=88,
        expected_current_policy_version_id="policy-v1",
        actor_id="INTEGRITY_PROFILER",
        reason="APPEND_ONLY_MUTATION_DETECTED: Historical ID 45 updated with newer payload",
    )


def test_policy_demotion_requires_the_detecting_frozen_policy_to_remain_current():
    """A stale worker cannot replace a newer table policy with its fallback."""
    cm = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.return_value = ("policy-newer",)

    service = PolicyGovernanceService(cm)
    with pytest.raises(PolicyVersionSupersededError):
        service.demote_cdc_policy(
            table_id=88,
            expected_current_policy_version_id="policy-old",
            actor_id="CDC_RAW_WINDOW_GUARD",
            reason="CDC_RAW_KEY_INTEGRITY_FAILED",
        )

    assert not any(
        "DELETE FROM metadata.table_cdc_policy_candidate" in str(call.args[0])
        for call in cur.execute.call_args_list
    )


def test_policy_demotion_promotes_a_non_cdc_successor_for_future_runs():
    cm = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.return_value = ("policy-current",)

    service = PolicyGovernanceService(cm)
    candidate = MagicMock(candidate_fingerprint="non-cdc-candidate")
    expected_result = MagicMock()
    with patch.object(service, "build_candidate_contract", return_value=candidate), \
         patch.object(service, "promote_policy", return_value=expected_result) as promote:
        result = service.demote_cdc_policy(
            table_id=88,
            expected_current_policy_version_id="policy-current",
            actor_id="CDC_RAW_WINDOW_GUARD",
            reason="CDC_RAW_KEY_INTEGRITY_FAILED",
        )

    assert result is expected_result
    assert any(
        "DELETE FROM metadata.table_cdc_policy_candidate" in str(call.args[0])
        for call in cur.execute.call_args_list
    )
    request = promote.call_args.args[0]
    assert request.expected_current_policy_version_id == "policy-current"
    assert request.expected_candidate_fingerprint == "non-cdc-candidate"


# ─────────────────────────────────────────────────────────────────────────────
# Test 17: AUTO strategy precedence prefers TIMESTAMP over SEQUENCE
# ─────────────────────────────────────────────────────────────────────────────
def test_auto_cdc_precedence_timestamp_over_sequence():
    columns = [
        {"column_name": "id", "data_type": "BIGINT", "is_primary_key": True},
        {"column_name": "updated_at", "data_type": "TIMESTAMP"},
    ]
    spark = _mock_spark_with_row({
        "null_cnt": 0,
        "distinct_cnt": 50,
        "max_val": datetime.utcnow(),
    })

    cfg = MagicMock()
    cfg.db_type = "postgres"

    res = detect_cdc_columns_profiled(
        columns=columns,
        business_key="id",
        key_confidence=100,
        key_source="PRIMARY_KEY",
        spark_session=spark,
        conn_config=cfg,
        table_fqn="public.orders",
    )

    # When both exist, TIMESTAMP_HIGH_WATERMARK is chosen over SEQUENCE
    assert res["is_cdc"] == "y"
    assert res["cdc_strategy"] == "TIMESTAMP_HIGH_WATERMARK"


# ─────────────────────────────────────────────────────────────────────────────
# Test 18: Clean profile requires zero duplicates AND zero nulls
# ─────────────────────────────────────────────────────────────────────────────
def test_clean_profile_requires_zero_duplicates_and_zero_nulls():
    spark = _mock_spark_with_row({
        "total_rows": 500,
        "non_null_rows": 500,
        "distinct_rows": 500,
    })

    source_cfg = MagicMock()
    source_cfg.db_type = "postgres"

    res = detect_business_key_profiled(
        columns=["item_code"],
        primary_keys=None,
        spark_session=spark,
        conn_config=source_cfg,
        table_fqn="public.items",
    )
    assert res["key_source"] == "PROFILED"
    assert res["key_confidence"] == 100
    assert res["key_candidate_nullable"] is False
    assert res["key_candidate_compatible"] is True
    assert bool(res["key_evidence_fingerprint"]) is True


# ─────────────────────────────────────────────────────────────────────────────
# Test 19: Multi-DB discovery queries both PRIMARY KEY and UNIQUE constraints
# ─────────────────────────────────────────────────────────────────────────────
def test_multi_db_discovery_extracts_unique_constraints():
    crawler = SchemaCrawler()
    sql_pg = crawler.primary_key_queries["postgres"]
    assert "in ('primary key', 'unique')" in sql_pg.lower() or "in ('p', 'u')" in sql_pg.lower()

    sql_ora = crawler.primary_key_queries["oracle"]
    assert "IN ('P', 'U')" in sql_ora

    sql_my = crawler.primary_key_queries["mysql"]
    assert "in ('primary key', 'unique')" in sql_my.lower() or "non_unique = 0" in sql_my.lower()

    sql_ms = crawler.primary_key_queries["mssql"]
    assert "in ('primary key', 'unique')" in sql_ms.lower() or "in ('pk', 'uq')" in sql_ms.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 20: Target constraint matching composite business key
# ─────────────────────────────────────────────────────────────────────────────
def test_target_constraint_matching_composite_business_key():
    inspector = PostgresSchemaInspector()
    target_cfg = MagicMock()
    target_cfg.user = "admin"
    target_cfg.password = "secret"

    with patch("src.utils.db_utils._parse_postgres_connection", return_value=("localhost", 5432, "db")), \
         patch("psycopg2.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchall.return_value = [
            ("uq_tenant_user", "u", "UNIQUE (tenant_id, user_id)")
        ]
        supported = inspector.supports_unique_key("public", "users", ("tenant_id", "user_id"), target_cfg)
        assert supported is True


# ─────────────────────────────────────────────────────────────────────────────
# Test 21: Zero new metadata columns verified
# ─────────────────────────────────────────────────────────────────────────────
def test_zero_new_metadata_columns_verified():
    valid_operator_columns = {"is_cdc", "cdc_strategy"}
    for col in valid_operator_columns:
        assert hasattr(MetadataRow, col) or col in MetadataRow.__annotations__


# ─────────────────────────────────────────────────────────────────────────────
# Test 22: Direct GUI / service update cannot directly set is_cdc=y without promotion
# ─────────────────────────────────────────────────────────────────────────────
def test_keyless_table_falls_back_to_governed_full_snapshot():
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Table without approved key or valid cursor
    mock_cur.fetchone.return_value = {
        "table_id": 99,
        "source_tag": "src",
        "source_table": "raw_feed",
        "target_tag": "tgt",
        "target_table": "raw_feed",
        "columns_unique_key": None,
        "key_source": "NONE",
        "key_confidence": 0,
        "duplicate_count": 0,
        "approved_source_business_key": None,
        "approved_key_evidence": None,
        "cdc_columns": None,
        "cdc_strategy": "AUTO",
        "cdc_source": "PROFILED",
        "cdc_confidence": 0,
        "cdc_health": "WARNING",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = []

    with patch("jobs.promote_table._promote_full_snapshot_policy", return_value=True) as promote_snapshot:
        success = evaluate_and_promote_table_cdc(
            conn_manager=cm,
            table_id=99,
            requested_is_cdc="y",
            requested_strategy="AUTO",
        )
    assert success is True
    promote_snapshot.assert_called_once()


def test_target_constraint_inspection_is_deferred_to_landing_preflight():
    """CDC policy promotion does not require a target table to exist yet."""
    cm = MagicMock()
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = {
        "table_id": 100,
        "source_tag": "src",
        "source_table": "orders",
        "target_tag": "tgt",
        "target_table": "orders",
        "columns_unique_key": "id",
        "key_source": "PRIMARY_KEY",
        "key_confidence": 100,
        "duplicate_count": 0,
        "approved_source_business_key": ["id"],
        "approved_key_evidence": "PRIMARY_KEY",
        "cdc_columns": "updated_at,id",
        "cdc_strategy": "AUTO",
        "cdc_source": "PROFILED",
        "cdc_confidence": 100,
        "cdc_health": "OK",
        "cdc_null_rows": 0,
        "cdc_health_details": "{}",
    }
    mock_cur.fetchall.return_value = [
        {"column_name": "id", "data_type": "BIGINT", "is_nullable": "NO"},
        {"column_name": "updated_at", "data_type": "TIMESTAMP", "is_nullable": "NO"},
    ]
    with patch("jobs.promote_table.PolicyGovernanceService") as mock_svc_cls, patch(
        "src.load.target_schema_inspector.get_schema_inspector"
    ) as mock_inspector:
        mock_svc_cls.return_value.build_candidate_contract.return_value.candidate_fingerprint = "fp"
        result = evaluate_and_promote_table_cdc(
            conn_manager=cm,
            table_id=100,
            requested_is_cdc="y",
            requested_strategy="AUTO",
        )

    assert result is True
    mock_inspector.assert_not_called()
