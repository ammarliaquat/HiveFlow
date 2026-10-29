# tests/test_cdc_handler.py
import datetime
import json
from unittest.mock import MagicMock

import pytest

from src.cdc.cdc_handler import CDCHandler
from src.discovery.type_mapper import detect_cdc_columns_profiled
from src.exceptions import CDCPolicyNotMigratedError
from src.metadata.metadata_models import MetadataRow

class MockMetadataReader:
    def __init__(self):
        self.updated_id = None
        self.updated_ts = None
        
    def update_watermark(self, table_id, timestamp):
        self.updated_id = table_id
        self.updated_ts = timestamp

def test_build_cdc_filter_disabled():
    reader = MockMetadataReader()
    handler = CDCHandler(reader)
    
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="n", cdc_columns="updated_at", cdc_p_days=7, delta_days=1,
        last_success_timestamp=datetime.datetime(2026, 1, 10, 12, 0, 0), is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="id", source_key_prefix="usr-", natural_key_column="id"
    )
    
    filt = handler.build_cdc_filter(meta)
    assert filt is None

def test_legacy_cdc_filter_fails_closed():
    reader = MockMetadataReader()
    handler = CDCHandler(reader)
    
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="y", cdc_columns="updated_at,created_at", cdc_p_days=5, delta_days=1,
        last_success_timestamp=datetime.datetime(2026, 1, 10, 12, 0, 0), is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="id", source_key_prefix="usr-", natural_key_column="id"
    )
    
    with pytest.raises(CDCPolicyNotMigratedError, match="CDC_POLICY_NOT_MIGRATED"):
        handler.build_cdc_filter(meta)

def test_legacy_cdc_skip_decision_fails_closed():
    mock_reader = MagicMock()
    handler = CDCHandler(mock_reader)

    meta_rerun = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="incremental", is_cdc="y", cdc_columns="updated_at", cdc_p_days=7, delta_days=2,
        last_success_timestamp=datetime.datetime(2026, 1, 10, 12, 0, 0), is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=2,
        partition_column="id", source_key_prefix="usr-", natural_key_column="id", rerun_flag='y'
    )
    with pytest.raises(CDCPolicyNotMigratedError, match="CDC_POLICY_NOT_MIGRATED"):
        handler.should_skip(meta_rerun)
    mock_reader.update_watermark.assert_not_called()


def test_legacy_watermark_writer_is_disabled():
    reader = MagicMock()
    handler = CDCHandler(reader)
    meta = MagicMock(source_table="users")

    with pytest.raises(CDCPolicyNotMigratedError, match="CDC_POLICY_NOT_MIGRATED"):
        handler.update_watermark(meta)

    reader.update_watermark.assert_not_called()


def test_cdc_strategy_validation():
    """Verify CDC strategy assignment uses canonical governed strategy names."""
    from src.discovery.type_mapper import detect_cdc_columns_profiled

    # 1. updated_at -> timestamp high-watermark
    res_update = detect_cdc_columns_profiled(["id", "updated_at", "name"])
    assert res_update["cdc_columns"] == "updated_at"
    assert res_update["cdc_strategy"] == "PENDING_APPROVAL"
    assert res_update["is_cdc"] == "n"
    assert res_update["cdc_source"] == "PROFILE_REQUIRED"
    assert res_update["cdc_health"] == "WARNING"

    # 2. created_at -> insert timestamp
    res_create = detect_cdc_columns_profiled(["id", "created_at", "name"])
    assert res_create["cdc_columns"] == "created_at"
    assert res_create["cdc_strategy"] == "PENDING_APPROVAL"
    assert res_create["is_cdc"] == "n"

    # 3. registration_date -> insert timestamp
    res_reg = detect_cdc_columns_profiled(["id", "registration_date", "name"])
    assert res_reg["cdc_columns"] == "registration_date"
    assert res_reg["cdc_strategy"] == "PENDING_APPROVAL"

    # 4. No temporal column -> FULL_LOAD
    res_full = detect_cdc_columns_profiled(["id", "name"])
    assert res_full["cdc_columns"] is None
    assert res_full["cdc_strategy"] == "FULL_LOAD"
    assert res_full["is_cdc"] == "n"

    # 5. Approved single-column business key -> sequence high-watermark
    res_pk = detect_cdc_columns_profiled(
        [
            {"column_name": "id", "data_type": "BIGINT"},
            {"column_name": "name", "data_type": "VARCHAR"},
        ],
        configured_cdc="id",
        configured_source="METADATA",
        business_key="id",
        key_confidence=100,
    )
    assert res_pk["cdc_columns"] == "id"
    assert res_pk["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"


def test_full_profile_auto_enables_only_confident_physical_sequence_cursor():
    """A complete numeric primary-key profile is the sole automatic CDC path."""
    from types import SimpleNamespace
    from src.discovery.type_mapper import detect_cdc_columns_profiled

    class _ProfileRow:
        def asDict(self):
            return {
                "total": 100,
                "pop": 100,
                "dist_cnt": 100,
                "min_v": 1,
                "max_v": 100,
            }

    class _ProfileFrame:
        def collect(self):
            return [_ProfileRow()]

    class _JdbcReader:
        def option(self, *_args):
            return self

        def load(self):
            return _ProfileFrame()

    class _SparkRead:
        def format(self, _format_name):
            return _JdbcReader()

    result = detect_cdc_columns_profiled(
        [
            {"column_name": "id", "data_type": "BIGINT"},
            {"column_name": "payload", "data_type": "VARCHAR"},
        ],
        spark_session=SimpleNamespace(read=_SparkRead()),
        conn_config=SimpleNamespace(db_type="mysql", url=""),
        table_fqn="membership.ulog",
        business_key="id",
        key_confidence=100,
        key_source="PRIMARY_KEY",
    )

    assert result["cdc_columns"] == "id"
    assert result["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"
    assert result["cdc_confidence"] == 100
    assert result["is_cdc"] == "y"

    untrusted_key = detect_cdc_columns_profiled(
        ["id", "payload"],
        spark_session=SimpleNamespace(read=_SparkRead()),
        conn_config=SimpleNamespace(db_type="mysql", url=""),
        table_fqn="membership.ulog",
        business_key="id",
        key_confidence=95,
        key_source="PROFILED",
    )

    assert untrusted_key["is_cdc"] == "n"
    assert untrusted_key["cdc_strategy"] == "FULL_LOAD"


def test_oracle_sequence_profile_uses_case_safe_column_identifier():
    """Oracle discovery must not quote a lower-case catalog name as case-sensitive."""
    from types import SimpleNamespace

    class _ProfileRow:
        def asDict(self):
            return {"total": 100, "pop": 100, "dist_cnt": 100, "min_v": 1, "max_v": 100}

    class _ProfileFrame:
        def collect(self):
            return [_ProfileRow()]

    class _JdbcReader:
        def __init__(self):
            self.options = []

        def option(self, key, value):
            self.options.append((key, value))
            return self

        def load(self):
            return _ProfileFrame()

    reader = _JdbcReader()

    class _SparkRead:
        def format(self, _format_name):
            return reader

    result = detect_cdc_columns_profiled(
        [{"column_name": "emigrant_id", "data_type": "NUMBER", "data_precision": 10, "data_scale": 0}],
        spark_session=SimpleNamespace(read=_SparkRead()),
        conn_config=SimpleNamespace(db_type="oracle", url=""),
        table_fqn="opfservices.tblop_jandec25100626",
        business_key="emigrant_id",
        key_confidence=100,
        key_source="PROFILED",
    )

    assert result["is_cdc"] == "y"
    assert result["cdc_strategy"] == "SEQUENCE_HIGH_WATERMARK"
    dbtable = dict(reader.options)["dbtable"]
    assert '"emigrant_id"' not in dbtable
    assert "COUNT(emigrant_id)" in dbtable


def test_sequence_profile_query_failure_is_reported_without_exposing_error_text():
    from types import SimpleNamespace

    class _JdbcReader:
        def option(self, *_args):
            return self

        def load(self):
            raise RuntimeError("secret=should-not-be-persisted")

    class _SparkRead:
        def format(self, _format_name):
            return _JdbcReader()

    result = detect_cdc_columns_profiled(
        [{"column_name": "emigrant_id", "data_type": "NUMBER", "data_precision": 10, "data_scale": 0}],
        spark_session=SimpleNamespace(read=_SparkRead()),
        conn_config=SimpleNamespace(db_type="oracle", url=""),
        table_fqn="opfservices.tblop_jandec25100626",
        business_key="emigrant_id",
        key_confidence=100,
        key_source="PROFILED",
    )

    details = json.loads(result["cdc_health_details"])
    assert result["is_cdc"] == "n"
    assert result["cdc_source"] == "PROFILE_ERROR"
    assert details["reason"] == "Sequence profile query failed"
    assert details["exception_type"] == "RuntimeError"
    assert "secret" not in result["cdc_health_details"]


def test_full_profile_auto_enables_timestamp_cdc_with_physical_key_tie_breaker():
    """A profiled update timestamp uses the physical business key to break ties."""
    from types import SimpleNamespace
    from src.discovery.type_mapper import detect_cdc_columns_profiled

    class _ProfileRow:
        def asDict(self):
            return {
                "total": 100,
                "pop": 100,
                "dist_cnt": 100,
                "min_v": datetime.datetime(2026, 9, 1, 10, 0, 0),
                "max_v": datetime.datetime(2026, 9, 2, 10, 0, 0),
            }

    class _ProfileFrame:
        def collect(self):
            return [_ProfileRow()]

    class _JdbcReader:
        def option(self, *_args):
            return self

        def load(self):
            return _ProfileFrame()

    class _SparkRead:
        def format(self, _format_name):
            return _JdbcReader()

    result = detect_cdc_columns_profiled(
        ["id", "updated_at", "payload"],
        spark_session=SimpleNamespace(read=_SparkRead()),
        conn_config=SimpleNamespace(db_type="mysql", url=""),
        table_fqn="membership.events",
        business_key="id",
        key_confidence=100,
        key_source="PRIMARY_KEY",
    )

    assert result["cdc_columns"] == "updated_at,id"
    assert result["cdc_strategy"] == "TIMESTAMP_HIGH_WATERMARK"
    assert result["cdc_confidence"] == 100
    assert result["is_cdc"] == "y"
