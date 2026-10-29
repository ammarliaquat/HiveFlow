"""
Unit tests for source_identification_key and source_key_prefix resolution.
Tests offline logic without requiring a live Spark Connect cluster.
"""
from datetime import datetime
from unittest.mock import MagicMock, patch
from src.metadata.metadata_models import MetadataRow
from src.constants import AuditColumns
from src.transform.transformer import Transformer


def test_metadata_row_from_frozen_table_populates_source_key_prefix():
    """Verify ETL._metadata_row_from_frozen_table sets source_key_prefix properly."""
    from src.orchestrator.etl import ETL
    
    mock_etl = MagicMock(spec=ETL)
    # Re-use the real method implementation
    real_method = ETL._metadata_row_from_frozen_table.__get__(mock_etl, ETL)
    
    # Setup mock contract
    mock_contract = MagicMock()
    mock_contract.approved_source_business_key.columns = ["emigrant_id"]
    mock_contract.resolved_mappings = MagicMock()
    mock_contract.cdc_policy = None
    mock_etl._contract_from_frozen_table.return_value = mock_contract
    
    frozen_table = MagicMock()
    frozen_table.table_id = 101
    frozen_table.source_tag = "opf_oracle"
    frozen_table.source_table = "tblop_jandec25260126"
    frozen_table.target_table = "tblop_jandec25260126"
    frozen_table.target_schema = "land"
    frozen_table.target_tag = "land_prod"
    frozen_table.priority = 1
    frozen_table.source_schema = "public"
    frozen_table.source_db_type = "oracle"
    frozen_table.landing_table = "tblop_jandec25260126"
    frozen_table.history_table = "tblop_jandec25260126"
    frozen_table.load_type = "full"
    frozen_table.is_cdc = "n"
    frozen_table.cdc_columns = None
    frozen_table.cdc_p_days = 0
    frozen_table.delta_days = 0
    frozen_table.last_success_timestamp = None
    frozen_table.last_success_date = None
    frozen_table.is_reconcile = "n"
    frozen_table.reconcile_column = None
    frozen_table.reconcile_measure = "count"
    frozen_table.reconcile_threshold = 0.0
    frozen_table.reconcile_action = "FAIL"
    frozen_table.target_rows_per_partition = 250000
    frozen_table.minimum_partition_threshold = 50000
    frozen_table.maximum_partitions = 32
    frozen_table.business_key_fingerprint = "fp123"
    frozen_table.cdc_strategy = None
    frozen_table.rerun_flag = "n"
    frozen_table.partition_lower_bound = None
    frozen_table.partition_upper_bound = None
    frozen_table.source_key_prefix = None

    meta = real_method(frozen_table)
    
    # Must be entity-scoped Semantic prefix: "opf_oracle_tblop_jandec25260126_"
    assert meta.source_key_prefix == "opf_oracle_tblop_jandec25260126_"

    assert meta.source_tag == "opf_oracle"
    assert meta.canonical_business_key_columns == ("emigrant_id",)



def test_transformer_parse_natural_keys_fallback():
    """Verify _parse_natural_keys falls back to canonical_business_key_columns if canonical_source_business_key is None."""
    transformer = Transformer("run-test-offline")
    
    # Meta without approved business key provenance, but with natural_key_column
    meta = MetadataRow(
        id=1, source_tag="test_src", source_schema="public", source_table="users",
        source_db_type="postgres", landing_table="l_users", history_table="h_users",
        target_table="t_users", target_schema="dw", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="test_src-", natural_key_column="user_id, org_id",
        key_approval_state="UNAPPROVED"
    )
    
    assert meta.canonical_source_business_key is None
    assert meta.canonical_business_key_columns == ("user_id", "org_id")
    
    mock_df = MagicMock()
    mock_df.columns = ["USER_ID", "ORG_ID", "NAME"]
    
    resolved = transformer._parse_natural_keys(mock_df, meta)
    assert resolved == ["USER_ID", "ORG_ID"]


def test_transformer_add_source_identification_key_prefix_resolution():
    """Verify add_source_identification_key resolves prefix from source_tag when prefix is empty or None."""
    transformer = Transformer("run-test-offline")
    
    # Meta with empty source_key_prefix
    meta = MetadataRow(
        id=1, source_tag="opf_oracle", source_schema="public", source_table="tblop",
        source_db_type="oracle", landing_table="l_tblop", history_table="h_tblop",
        target_table="tblop", target_schema="land", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="", natural_key_column="emigrant_id",
        key_approval_state="APPROVED", approved_source_business_key=["emigrant_id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="fp1",
        current_key_evidence_fingerprint="fp1", key_approved_by="test",
        key_approved_at=datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )
    
    mock_df = MagicMock()
    mock_df.columns = ["emigrant_id", "full_name"]
    mock_df.filter.return_value.count.return_value = 0
    
    with patch("src.transform.transformer.F") as mock_f:
        mock_f.col.return_value.cast.return_value = "emigrant_id_str"
        mock_f.lit.return_value = "prefix_lit"
        mock_f.concat.return_value = "concat_expr"
        
        transformer.add_source_identification_key(mock_df, meta)
        
        # lit should have been called with "opf_oracle_tblop_" because source_key_prefix was ""
        mock_f.lit.assert_called_with("opf_oracle_tblop_")

        mock_df.withColumn.assert_called_with(AuditColumns.SOURCE_IDENTIFICATION_KEY, "concat_expr")



def test_transformer_add_source_identification_key_keyless():
    """Verify add_source_identification_key generates synthetic URN for keyless tables."""
    transformer = Transformer("run-test-offline")
    
    meta_keyless = MetadataRow(
        id=2, source_tag="opf_oracle", source_schema="public", source_table="keyless",
        source_db_type="oracle", landing_table="l_k", history_table="h_k",
        target_table="keyless", target_schema="land", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="", natural_key_column=None,
    )
    
    mock_df = MagicMock()
    mock_df.columns = ["col1", "col2"]
    
    with patch("src.transform.transformer.F") as mock_f:
        mock_f.concat.return_value = "synthetic_urn_expr"
        transformer.add_source_identification_key(mock_df, meta_keyless)
        mock_f.lit.assert_any_call("opf_oracle_keyless_synthetic_")

        mock_df.withColumn.assert_called_with(AuditColumns.SOURCE_IDENTIFICATION_KEY, "synthetic_urn_expr")



def test_metadata_row_from_frozen_table_empty_string_prefix():
    """Verify that even if frozen_table has source_key_prefix="", it defaults to source_tag + "-"."""
    from src.orchestrator.etl import ETL
    
    mock_etl = MagicMock(spec=ETL)
    real_method = ETL._metadata_row_from_frozen_table.__get__(mock_etl, ETL)
    
    mock_contract = MagicMock()
    mock_contract.approved_source_business_key.columns = ["id"]
    mock_contract.resolved_mappings = MagicMock()
    mock_contract.cdc_policy = None
    mock_etl._contract_from_frozen_table.return_value = mock_contract
    
    frozen_table = MagicMock()
    frozen_table.table_id = 102
    frozen_table.source_tag = "hr_mysql"
    frozen_table.source_table = "employees"
    frozen_table.target_table = "employees"
    frozen_table.target_schema = "land"
    frozen_table.target_tag = "land_prod"
    frozen_table.priority = 1
    frozen_table.source_schema = "public"
    frozen_table.source_db_type = "mysql"
    frozen_table.landing_table = "employees"
    frozen_table.history_table = "employees"
    frozen_table.load_type = "full"
    frozen_table.is_cdc = "n"
    frozen_table.cdc_columns = None
    frozen_table.cdc_p_days = 0
    frozen_table.delta_days = 0
    frozen_table.last_success_timestamp = None
    frozen_table.last_success_date = None
    frozen_table.is_reconcile = "n"
    frozen_table.reconcile_column = None
    frozen_table.reconcile_measure = "count"
    frozen_table.reconcile_threshold = 0.0
    frozen_table.reconcile_action = "FAIL"
    frozen_table.target_rows_per_partition = 250000
    frozen_table.minimum_partition_threshold = 50000
    frozen_table.maximum_partitions = 32
    frozen_table.business_key_fingerprint = "fp456"
    frozen_table.cdc_strategy = None
    frozen_table.rerun_flag = "n"
    frozen_table.partition_lower_bound = None
    frozen_table.partition_upper_bound = None
    frozen_table.source_key_prefix = ""  # explicitly empty string

    meta = real_method(frozen_table)
    assert meta.source_key_prefix == "hr_mysql_employees_"




def test_transformer_add_source_identification_key_composite_prefix_none():
    """Verify composite keys join with '-' and use source_tag and source_table prefix when source_key_prefix is None."""
    transformer = Transformer("run-test-offline")
    
    meta = MetadataRow(
        id=3, source_tag="billing_pg", source_schema="public", source_table="invoices",
        source_db_type="postgres", landing_table="l_inv", history_table="h_inv",
        target_table="invoices", target_schema="land", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix=None, natural_key_column="inv_year, inv_no",
        key_approval_state="APPROVED", approved_source_business_key=["inv_year", "inv_no"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="fp2",
        current_key_evidence_fingerprint="fp2", key_approved_by="test",
        key_approved_at=datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )
    
    mock_df = MagicMock()
    mock_df.columns = ["inv_year", "inv_no", "amount"]
    mock_df.filter.return_value.count.return_value = 0
    
    with patch("src.transform.transformer.F") as mock_f:
        mock_f.col.return_value.cast.return_value = "col_str"
        mock_f.concat_ws.return_value = "joined_keys"
        mock_f.lit.return_value = "prefix_lit"
        mock_f.concat.return_value = "final_res"
        
        transformer.add_source_identification_key(mock_df, meta)
        
        mock_f.concat_ws.assert_called_with("_", mock_f.coalesce.return_value, mock_f.coalesce.return_value)
        mock_f.lit.assert_called_with("billing_pg_invoices_")

        mock_df.withColumn.assert_called_with(AuditColumns.SOURCE_IDENTIFICATION_KEY, "final_res")



def test_transformer_add_audit_columns_source_identification_table():
    """Verify add_audit_columns builds SOURCE_IDENTIFICATION_TABLE with proper fallback."""
    transformer = Transformer("run-test-offline")
    
    meta = MetadataRow(
        id=4, source_tag="test_src", source_schema="public", source_table="orders",
        source_db_type="postgres", landing_table="l_orders", history_table="h_orders",
        target_table="orders", target_schema="land", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="", natural_key_column="order_id",
        key_approval_state="UNAPPROVED"
    )
    
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    
    with patch("src.transform.transformer.F") as mock_f:
        transformer.add_audit_columns(mock_df, meta)
        # Should build "test_src-orders-order_id" even when UNAPPROVED due to fallback to canonical_business_key_columns
        mock_f.lit.assert_any_call("test_src-orders-order_id")


def test_semantic_namespaced_urn_cross_table_collision_prevention():
    """
    Verify Pattern 1: Semantic Namespaced URN (Entity-Scoped Natural Key).
    Two tables in the same database (e.g. memberships and ulog) with the same PK value (e.g. id=12)
    MUST generate distinct, non-colliding source_identification_key values:
      - 'msopforg_opforg-memberships-12'
      - 'msopforg_opforg-ulog-12'
    """
    transformer = Transformer("run-urn-test")

    meta_memberships = MetadataRow(
        id=10, source_tag="msopforg_opforg", source_schema="public", source_table="memberships",
        source_db_type="mysql", landing_table="memberships", history_table="memberships",
        target_table="memberships", target_schema="land", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="", natural_key_column="id",
        key_approval_state="APPROVED", approved_source_business_key=["id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="fp1",
        current_key_evidence_fingerprint="fp1", key_approved_by="test",
        key_approved_at=datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )

    meta_ulog = MetadataRow(
        id=11, source_tag="msopforg_opforg", source_schema="public", source_table="ulog",
        source_db_type="mysql", landing_table="ulog", history_table="ulog",
        target_table="ulog", target_schema="land", is_active="y", priority=1,
        load_type="full", is_cdc="n", cdc_columns=None, cdc_p_days=0, delta_days=0,
        last_success_timestamp=None, is_reconcile="n", reconcile_column=None,
        reconcile_measure="count", chunk_size=1000, num_partitions=1,
        partition_column=None, source_key_prefix="", natural_key_column="id",
        key_approval_state="APPROVED", approved_source_business_key=["id"],
        approved_key_evidence="test", approved_key_evidence_fingerprint="fp2",
        current_key_evidence_fingerprint="fp2", key_approved_by="test",
        key_approved_at=datetime.now(), key_approval_revision=1,
        key_candidate_nullable=False, key_candidate_compatible=True
    )

    df_memberships = MagicMock()
    df_memberships.columns = ["id", "name"]
    df_memberships.filter.return_value.count.return_value = 0

    df_ulog = MagicMock()
    df_ulog.columns = ["id", "url"]
    df_ulog.filter.return_value.count.return_value = 0

    with patch("src.transform.transformer.F") as mock_f:
        mock_f.col.return_value.cast.return_value = "id_val"

        # memberships table
        transformer.add_source_identification_key(df_memberships, meta_memberships)
        mock_f.lit.assert_called_with("source_membership_dbs_")

        # ulog table
        transformer.add_source_identification_key(df_ulog, meta_ulog)
        mock_f.lit.assert_called_with("msopforg_opforg_ulog_")




