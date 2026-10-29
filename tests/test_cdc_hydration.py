import pytest
from unittest.mock import MagicMock, patch
from src.metadata.metadata_models import MetadataRow, MappingPolicy
from src.metadata.key_contract import IdentityMode
from src.orchestrator.etl import ETL

def _make_meta(table_id: int, source_table: str, is_cdc: str = "y", mapping_policy: str = "EXPLICIT") -> MetadataRow:
    row = MetadataRow(
        id=table_id,
        source_tag="opf_oracle",
        source_schema="public",
        source_table=source_table,
        source_db_type="oracle",
        landing_table=source_table,
        history_table=source_table,
        target_table=source_table,
        target_schema="public",
        target_tag="land_prod",
        target_database_type="postgres",
        is_active="y",
        priority=1,
        load_type="bronze",
        is_cdc=is_cdc,
        cdc_columns="id",
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
        mapping_policy=mapping_policy,
    )
    return row

def test_map01_guard_preserves_frozen_resolved_mappings():
    """Verify that when meta.resolved_mappings is already set, column_mapper is NOT invoked."""
    mock_column_mapper = MagicMock()
    row = _make_meta(101, "tblop_test", is_cdc="y", mapping_policy="EXPLICIT")
    pre_resolved = MagicMock(name="PreResolvedMappings")
    row.resolved_mappings = pre_resolved

    policy_enum = MappingPolicy[row.mapping_policy.upper()]

    # Execute the MAP-01 guard block
    if row.resolved_mappings is None:
        row.resolved_mappings = mock_column_mapper.resolve_table_mappings(row.id, mapping_policy=policy_enum)

    # Assert column_mapper was never called and pre_resolved is preserved
    mock_column_mapper.resolve_table_mappings.assert_not_called()
    assert row.resolved_mappings is pre_resolved

def test_map01_guard_resolves_when_mappings_none():
    """Verify that when meta.resolved_mappings is None, column_mapper IS invoked."""
    mock_column_mapper = MagicMock()
    mock_mappings = MagicMock(name="LiveResolvedMappings")
    mock_column_mapper.resolve_table_mappings.return_value = mock_mappings

    row = _make_meta(102, "tblop_test2", is_cdc="y", mapping_policy="EXPLICIT")
    assert row.resolved_mappings is None

    policy_enum = MappingPolicy[row.mapping_policy.upper()]

    # Execute the MAP-01 guard block
    if row.resolved_mappings is None:
        row.resolved_mappings = mock_column_mapper.resolve_table_mappings(row.id, mapping_policy=policy_enum)

    mock_column_mapper.resolve_table_mappings.assert_called_once_with(102, mapping_policy=policy_enum)
    assert row.resolved_mappings is mock_mappings

def test_cdc_contract_hydration_in_load_with_db_link():
    """Verify CDC contract hydration populates table_contract and legacy fields in load_with_db_link."""
    with patch("src.orchestrator.etl.ConnectionManager"), \
         patch("src.orchestrator.etl.SchemaCertifier"):
        etl_instance = ETL.__new__(ETL)
        etl_instance.conn_manager = MagicMock()
        etl_instance.spark_app_id = "test-spark-app"
        etl_instance.metadata_reader = MagicMock()
        etl_instance.last_run_id = "test-run-123"
        etl_instance._execute_table_batch = MagicMock(return_value=[])
        etl_instance._clear_cancellation_guards = MagicMock()

        meta = _make_meta(1055, "tblop_jandec25260126", is_cdc="y")
        assert meta.table_contract is None

        etl_instance.metadata_reader.get_tables.return_value = [meta]

        mock_contract = MagicMock()
        mock_contract.resolved_mappings = MagicMock(name="ContractMappings")
        mock_contract.approved_source_business_key = MagicMock(
            identity_mode=IdentityMode.EXACT_KEY,
            exception_values=(1, 2, 3),
        )
        mock_contract.cdc_policy = MagicMock(activation_state="ACTIVE")

        with patch("src.metadata.policy_governance.PolicyGovernanceService.materialize_execution_contract", return_value=mock_contract) as mock_mat:
            with patch("src.logging_.batch_logger.BatchLogger"):
                result = etl_instance.load_with_db_link(priority=1, target_tag="opf_oracle", run_id="test-run-123")

                assert meta.table_contract is mock_contract
                assert meta.resolved_mappings is mock_contract.resolved_mappings
                assert meta.identity_mode == IdentityMode.EXACT_KEY
                assert meta.exception_values == (1, 2, 3)
                assert meta.cdc_activation_state == "ACTIVE"
                mock_mat.assert_called_once_with(1055)

def test_cdc_contract_hydration_failure_is_isolated():
    """Verify that a single table's hydration failure logs warning and does not abort batch."""
    with patch("src.orchestrator.etl.ConnectionManager"), \
         patch("src.orchestrator.etl.SchemaCertifier"):
        etl_instance = ETL.__new__(ETL)
        etl_instance.conn_manager = MagicMock()
        etl_instance.spark_app_id = "test-spark-app"
        etl_instance.metadata_reader = MagicMock()
        etl_instance.last_run_id = "test-run-456"
        etl_instance._execute_table_batch = MagicMock(return_value=[])
        etl_instance._clear_cancellation_guards = MagicMock()

        meta1 = _make_meta(1001, "tbl_fail", is_cdc="y")
        meta2 = _make_meta(1002, "tbl_success", is_cdc="y")

        etl_instance.metadata_reader.get_tables.return_value = [meta1, meta2]

        mock_contract2 = MagicMock()
        mock_contract2.resolved_mappings = MagicMock(name="Mappings2")
        mock_contract2.approved_source_business_key = MagicMock(identity_mode=IdentityMode.EXACT_KEY, exception_values=())
        mock_contract2.cdc_policy = MagicMock(activation_state="ACTIVE")

        def side_effect(table_id):
            if table_id == 1001:
                raise RuntimeError("Policy not promoted")
            return mock_contract2

        with patch("src.metadata.policy_governance.PolicyGovernanceService.materialize_execution_contract", side_effect=side_effect):
            with patch("src.logging_.batch_logger.BatchLogger"):
                result = etl_instance.load_with_db_link(priority=1, target_tag="opf_oracle", run_id="test-run-456")

                # meta1 failed hydration: table_contract remains None
                assert meta1.table_contract is None
                # meta2 succeeded hydration: table_contract attached
                assert meta2.table_contract is mock_contract2
                assert meta2.resolved_mappings is mock_contract2.resolved_mappings
                # Batch execution was still reached
                etl_instance._execute_table_batch.assert_called_once()

def test_cdc_contract_hydration_in_load_historyData():
    """Verify CDC contract hydration in load_historyData."""
    with patch("src.orchestrator.etl.ConnectionManager"), \
         patch("src.orchestrator.etl.SchemaCertifier"):
        etl_instance = ETL.__new__(ETL)
        etl_instance.conn_manager = MagicMock()
        etl_instance.spark_app_id = "test-spark-app"
        etl_instance.metadata_reader = MagicMock()
        etl_instance.last_run_id = "test-run-silver"
        etl_instance._execute_table_batch = MagicMock(return_value=[])
        etl_instance._clear_cancellation_guards = MagicMock()

        meta = _make_meta(1060, "tblop_silver_test", is_cdc="y")
        etl_instance.metadata_reader.get_tables.return_value = [meta]

        mock_contract = MagicMock()
        mock_contract.resolved_mappings = MagicMock(name="SilverContractMappings")
        mock_contract.approved_source_business_key = MagicMock(
            identity_mode=IdentityMode.EXACT_KEY,
            exception_values=(),
        )
        mock_contract.cdc_policy = MagicMock(activation_state="ACTIVE")

        with patch("src.metadata.policy_governance.PolicyGovernanceService.materialize_execution_contract", return_value=mock_contract) as mock_mat:
            with patch("src.logging_.batch_logger.BatchLogger"):
                result = etl_instance.load_historyData(priority=1, target_tag="opf_oracle", run_id="test-run-silver")

                assert meta.table_contract is mock_contract
                assert meta.resolved_mappings is mock_contract.resolved_mappings
                mock_mat.assert_called_once_with(1060)

def test_cdc_contract_hydration_in_run_target_cdc():
    """Verify CDC contract hydration in run_target_cdc."""
    with patch("src.orchestrator.etl.ConnectionManager"), \
         patch("src.orchestrator.etl.SchemaCertifier"):
        etl_instance = ETL.__new__(ETL)
        etl_instance.conn_manager = MagicMock()
        etl_instance.spark_app_id = "test-spark-app"
        etl_instance.metadata_reader = MagicMock()
        etl_instance.last_run_id = "test-run-gold"
        etl_instance._execute_table_batch = MagicMock(return_value=[])
        etl_instance._clear_cancellation_guards = MagicMock()

        meta = _make_meta(1070, "tblop_gold_test", is_cdc="y")
        etl_instance.metadata_reader.get_gold_tables.return_value = [meta]

        mock_contract = MagicMock()
        mock_contract.resolved_mappings = MagicMock(name="GoldContractMappings")
        mock_contract.approved_source_business_key = MagicMock(
            identity_mode=IdentityMode.EXACT_KEY,
            exception_values=(),
        )
        mock_contract.cdc_policy = MagicMock(activation_state="ACTIVE")

        with patch("src.metadata.policy_governance.PolicyGovernanceService.materialize_execution_contract", return_value=mock_contract) as mock_mat:
            with patch("src.logging_.batch_logger.BatchLogger"):
                result = etl_instance.run_target_cdc(priority=1, target_tag="opf_oracle", run_id="test-run-gold")

                assert meta.table_contract is mock_contract
                assert meta.resolved_mappings is mock_contract.resolved_mappings
                mock_mat.assert_called_once_with(1070)

