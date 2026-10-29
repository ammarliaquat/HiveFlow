import pytest
from unittest.mock import MagicMock, patch
from src.contracts.claim_models import FrozenTableEntry
from src.metadata.metadata_models import MetadataRow
from src.transform.transformer import Transformer


def test_frozen_table_entry_landing_mode_roundtrip():
    """Verify FrozenTableEntry serializes and deserializes landing_mode."""
    entry = FrozenTableEntry(
        table_id=101,
        source_tag="test_src",
        source_table="plots",
        target_tag="land_housing",
        target_schema="land_housing",
        target_table="plots",
        priority=1,
        authority_mode="GOVERNED",
        policy_version_id="pv-1",
        policy_fingerprint="fp-1",
        column_mapping_version_id="mv-1",
        column_mapping_fingerprint="mfp-1",
        business_key_version_id="bv-1",
        business_key_fingerprint="bfp-1",
        business_key_columns=["plot_no"],
        deployment_profile_fingerprint="dp-1",
        landing_mode="DIRECT",
    )
    d = entry.to_dict()
    assert d["landing_mode"] == "DIRECT"

    restored = FrozenTableEntry.from_dict(d)
    assert restored.landing_mode == "DIRECT"


def test_frozen_table_entry_landing_mode_default():
    """Verify FrozenTableEntry defaults landing_mode to METADATA when omitted."""
    d = {
        "table_id": 102,
        "source_tag": "test_src",
        "source_table": "transhist",
        "target_tag": "land_housing",
        "target_schema": "land_housing",
        "target_table": "transhist",
        "priority": 1,
        "authority_mode": "GOVERNED",
        "policy_version_id": "pv-1",
        "policy_fingerprint": "fp-1",
        "column_mapping_version_id": "mv-1",
        "column_mapping_fingerprint": "mfp-1",
        "business_key_version_id": "bv-1",
        "business_key_fingerprint": "bfp-1",
        "business_key_columns": ["id"],
        "deployment_profile_fingerprint": "dp-1",
    }
    restored = FrozenTableEntry.from_dict(d)
    assert restored.landing_mode == "METADATA"


def test_transformer_direct_landing_mode_bypasses():
    """Verify Transformer bypasses natural key injection, dedup, and column stripping in DIRECT mode."""
    meta = MagicMock(spec=MetadataRow)
    meta.landing_mode = "DIRECT"
    meta.source_tag = "opf_oracle"
    meta.source_table = "plots"
    meta.canonical_source_business_key = ()
    meta.canonical_business_key_columns = ()
    meta.skip_dedup = "n"

    transformer = Transformer(run_id="run-test-123")
    mock_df = MagicMock()

    # 1. add_source_identification_key returns df directly without searching for keys
    res_key = transformer.add_source_identification_key(mock_df, meta)
    assert res_key is mock_df

    # 2. deduplicate returns df directly without window function
    res_dedup = transformer.deduplicate(mock_df, meta)
    assert res_dedup is mock_df

    # 3. select_target_columns returns df directly without schema filtering
    res_select = transformer.select_target_columns(mock_df, meta)
    assert res_select is mock_df


def test_loader_preflight_direct_bypasses_business_key_check():
    """Verify Loader preflight does not fail on missing or unapproved keys when landing_mode is DIRECT."""
    from src.load.loader import Loader

    meta = MagicMock(spec=MetadataRow)
    meta.landing_mode = "DIRECT"
    meta.source_table = "plots"
    meta.target_table = "plots"
    meta.target_schema = "land_housing"
    meta.canonical_source_business_key = ()
    meta.canonical_business_key_columns = ()
    meta.is_cdc = "n"

    target_cfg = MagicMock()
    target_cfg.db_type = "postgres"
    target_cfg.schema = "land_housing"

    loader = Loader(spark=MagicMock(), conn_manager=MagicMock())

    # Preflight should not raise BusinessKeyRequiredError or TargetPreflightError
    with patch.object(loader, "_target_table_exists", return_value=True):
        # Must execute without raising exceptions
        loader.provision_target_schema_preflight(
            df=MagicMock(),
            meta=meta,
            target_cfg=target_cfg,
            merge_key=None,
            before_persist=lambda: None,
        )


def test_extractor_partition_bound_formatting():
    """Verify Extractor formats decimal strings like '10155.0000000000' to '10155' for Spark JDBC."""
    from src.extract.extractor import Extractor

    meta = MagicMock(spec=MetadataRow)
    meta.chunk_size = 50000
    meta.partition_column = "id"
    meta.partition_lower_bound = "1.0000000000"
    meta.partition_upper_bound = "10155.0000000000"
    meta.num_partitions = 4

    reader_mock = MagicMock()
    reader_mock.option.return_value = reader_mock

    extractor = Extractor(spark=MagicMock(), conn_manager=MagicMock())
    conn_mock = MagicMock()

    extractor._add_parallel_read_options(reader_mock, meta, conn_mock)

    # Verify options set on reader
    options_called = {call[0][0]: call[0][1] for call in reader_mock.option.call_args_list}
    assert options_called.get("lowerBound") == "1"
    assert options_called.get("upperBound") == "10155"
    assert options_called.get("numPartitions") == "4"


def test_loader_direct_landing_mode_prunes_row_hash():
    """Verify write_target_overwrite in DIRECT mode prunes non-target columns like row_hash."""
    from src.load.loader import Loader
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    conn_mgr = MagicMock()
    target_conn = MagicMock()
    target_conn.schema = "land"
    target_conn.db_type = "postgres"
    conn_mgr.get.return_value = target_conn

    target_cfg = MagicMock()
    target_cfg.db_type = "postgres"
    target_cfg.driver = "org.postgresql.Driver"
    conn_mgr.resolve_jdbc.return_value = target_cfg

    loader = Loader(spark=MagicMock(), conn_manager=conn_mgr)

    meta = MagicMock(spec=MetadataRow)
    meta.landing_mode = "DIRECT"
    meta.source_table = "tblop_dec24"
    meta.target_table = "tblop_dec24"
    meta.target_schema = "land"
    meta.target_tag = "land_prod"
    meta.canonical_source_business_key = ()
    meta.is_cdc = "n"

    # Mock target table having only reg_id, cnic (no row_hash)
    target_schema_fields = [
        StructField("reg_id", IntegerType(), True),
        StructField("cnic", StringType(), True),
    ]
    target_schema_df = MagicMock()
    target_schema_df.schema.fields = target_schema_fields

    # Mock source df having reg_id, cnic, AND row_hash
    source_df = MagicMock()
    source_df.columns = ["reg_id", "cnic", "row_hash"]
    source_df.select.return_value = source_df

    with patch.object(loader, "provision_target_schema_preflight"):
        with patch("src.load.loader._apply_jdbc_options") as mock_apply:
            mock_reader = MagicMock()
            mock_reader.option.return_value = mock_reader
            mock_reader.load.return_value = target_schema_df
            mock_apply.return_value = mock_reader

            with patch("src.load.loader.safe_execute_jdbc"):
                with patch.object(loader, "_require_persistence_ownership"):
                    mock_writer = MagicMock()
                    mock_writer.option.return_value = mock_writer
                    mock_writer.mode.return_value = mock_writer
                    mock_apply.side_effect = [mock_reader, mock_writer]

                    loader.write_target_overwrite(
                        df=source_df,
                        meta=meta,
                        before_target_dml=lambda: None,
                    )

                    # Verify that source_df.select was called with only matching columns (reg_id, cnic)
                    # and NOT row_hash
                    select_args = source_df.select.call_args[0]
                    # We expect 2 columns selected
                    assert len(select_args) == 2


def test_restore_persisted_cdc_context_bypassed_in_direct_mode():
    """Verify that _restore_persisted_cdc_context returns cleanly without error when landing_mode is DIRECT even if is_cdc is 'y'."""
    from src.orchestrator.etl import ETL

    etl = ETL.__new__(ETL)

    meta = MagicMock(spec=MetadataRow)
    meta.landing_mode = "DIRECT"
    meta.is_cdc = "y"
    meta.source_table = "zonefivepayments"

    # Artifact lacking cdc_window_id
    artifact = MagicMock()
    artifact.cdc_window_id = None

    # Must NOT raise RuntimeError("GOVERNED_CDC_WINDOW_MISSING: ...")
    etl._restore_persisted_cdc_context(meta, artifact)


def test_restore_persisted_cdc_context_still_enforces_in_metadata_mode():
    """Verify that _restore_persisted_cdc_context still fails closed when landing_mode is METADATA and cdc_window_id is missing."""
    from src.orchestrator.etl import ETL

    etl = ETL.__new__(ETL)

    meta = MagicMock(spec=MetadataRow)
    meta.landing_mode = "METADATA"
    meta.is_cdc = "y"
    meta.source_table = "zonefivepayments"

    # Artifact lacking cdc_window_id
    artifact = MagicMock()
    artifact.cdc_window_id = None

    with pytest.raises(RuntimeError, match="GOVERNED_CDC_WINDOW_MISSING"):
        etl._restore_persisted_cdc_context(meta, artifact)



