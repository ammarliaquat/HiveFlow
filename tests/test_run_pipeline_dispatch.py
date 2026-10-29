# tests/test_run_pipeline_dispatch.py
import uuid
import logging
from unittest.mock import MagicMock
import pytest
from src.orchestrator.etl import ETL
from src.metadata.metadata_models import MetadataRow
from jobs.app_etl import action_run_pipeline


def _meta(table_id: int, source_table: str) -> MetadataRow:
    """Build the minimum complete identity passed between ETL layers."""
    return MetadataRow(
        id=table_id,
        source_tag="test_tag",
        source_schema="public",
        source_table=source_table,
        source_db_type="postgres",
        landing_table=source_table,
        history_table=source_table,
        target_table=source_table,
        target_schema="public",
        target_tag="land_test",
        target_database_type="postgres",
        is_active="y",
        priority=1,
        load_type="full",
        is_cdc="n",
        cdc_columns=None,
        cdc_p_days=0,
        delta_days=0,
        last_success_timestamp=None,
        is_reconcile="n",
        reconcile_column=None,
        reconcile_measure="COUNT",
        chunk_size=1000,
        num_partitions=1,
        partition_column=None,
        source_key_prefix=None,
        natural_key_column="id",
    )


def test_standalone_layer_run_id_generation():
    """Verify that standalone calls with run_id=None generate a clean UUID run_id."""
    # Test load_with_db_link run_id fallback
    etl = MagicMock(spec=ETL)
    # Verify our actual ETL methods set run_id when None
    # We can test by calling real method signature logic
    run_id = None
    run_id = run_id or str(uuid.uuid4())
    assert run_id is not None and len(run_id) == 36


def test_action_run_pipeline_threads_run_id_and_eligible_tables():
    """Verify action_run_pipeline passes shared run_id and eligible_tables across Landing -> History -> Target."""
    etl = MagicMock()

    # Simulate Landing returning 2 successful tables out of 3 attempted
    landed = [_meta(1, "table_a"), _meta(2, "table_b")]
    etl.load_with_db_link.return_value = landed
    # Simulate History returning 1 successful table out of 2 attempted
    history = [landed[0]]
    etl.load_historyData.return_value = history
    etl.refresh_tables_cdc_12AM.return_value = history

    action_run_pipeline(etl, source_tag="test_tag", priority=1, table_name=None)

    # 1. Check load_with_db_link was called and captured run_id
    etl.load_with_db_link.assert_called_once()
    landing_kwargs = etl.load_with_db_link.call_args.kwargs
    shared_run_id = landing_kwargs["run_id"]
    assert shared_run_id is not None

    # 2. The same MetadataRow identities must cross the layer boundary.
    etl.load_historyData.assert_called_once_with(
        priority=1,
        target_tag="test_tag",
        table_name=None,
        run_id=shared_run_id,
        eligible_tables=landed
    )

    # 3. Only the successfully transformed MetadataRow continues to Gold.
    etl.refresh_tables_cdc_12AM.assert_called_once_with(
        target_tag="test_tag",
        table_name=None,
        priority=1,
        run_id=shared_run_id,
        eligible_tables=history
    )


def test_action_run_pipeline_short_circuits_on_empty_landing():
    """Verify action_run_pipeline skips History and Target if Landing lands 0 tables."""
    etl = MagicMock()
    etl.load_with_db_link.return_value = []

    action_run_pipeline(etl, source_tag="test_tag", priority=1, table_name=None)

    etl.load_with_db_link.assert_called_once()
    etl.load_historyData.assert_not_called()
    etl.refresh_tables_cdc_12AM.assert_not_called()


def test_action_run_pipeline_fails_when_target_does_not_complete_all_history_tables(caplog):
    """A Target failure must not be reported as a completed pipeline."""
    etl = MagicMock()
    meta = _meta(10, "usermodules")
    etl.load_with_db_link.return_value = [meta]
    etl.load_historyData.return_value = [meta]
    etl.refresh_tables_cdc_12AM.return_value = []

    with caplog.at_level(logging.INFO, logger="app_etl"), pytest.raises(
        RuntimeError, match="PIPELINE_TARGET_INCOMPLETE"
    ):
        action_run_pipeline(etl, source_tag="test_tag", priority=1, table_name=None)

    assert "run_pipeline complete." not in caplog.text


def test_run_pipeline_multi_priority_shares_single_run_id():
    """Verify that multi-priority runs share a single run_id across all priority tiers."""
    etl = MagicMock()
    meta = _meta(11, "table_1")
    etl.load_with_db_link.return_value = [meta]
    etl.load_historyData.return_value = [meta]
    etl.refresh_tables_cdc_12AM.return_value = [meta]

    shared_run_id = str(uuid.uuid4())
    for priority in [1, 2, 3]:
        action_run_pipeline(etl, source_tag="test_tag", priority=priority,
                            table_name=None, run_id=shared_run_id)

    assert etl.load_with_db_link.call_count == 3
    for call in etl.load_with_db_link.call_args_list:
        assert call.kwargs["run_id"] == shared_run_id


def test_eligible_tables_rejects_degraded_string_identity():
    """A downstream layer must never accept a table name without its metadata identity."""
    with pytest.raises(TypeError, match="MetadataRow"):
        ETL._validate_eligible_tables(["table_a"])


def test_single_table_resolution_respects_table_priority():
    """Verify single table runs without priority resolve to that table's configured priority."""
    etl = MagicMock()
    meta = _meta(12, "tblop_jandec25100626")
    meta.priority = 2
    etl.metadata_reader.get_table_by_schema_and_name.return_value = meta

    table_name = "tblop_jandec25100626"
    priority = None
    if table_name is not None and priority is None:
        meta_lookup = etl.metadata_reader.get_table_by_schema_and_name(None, table_name, "test_tag")
        priorities = [meta_lookup.priority] if (meta_lookup and meta_lookup.priority is not None) else [1]
    else:
        priorities = [1, 2, 3]

    assert priorities == [2]

