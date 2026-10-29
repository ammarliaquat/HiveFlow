import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from src.exceptions import CDCPolicyNotMigratedError
from src.metadata.metadata_models import MetadataRow
from src.load.loader import Loader

def test_single_target_preflight_ownership_cdc(spark):
    """
    H-01: Verify Loader owns preflight for CDC write_target.
    etl.py should NOT call it. It should be called EXACTLY ONCE.
    """
    df = spark.createDataFrame([(1, "A")], ["id", "val"])
    
    meta = MagicMock()
    meta.id = 1
    meta.source_tag = "test_src"
    meta.source_table = "test_tbl"
    meta.target_table = "test_tbl"
    meta.target_schema = "public"
    meta.landing_mode = "DIRECT"
    meta.is_cdc = "y"
    meta.canonical_business_key_columns = ("id",)
    
    target_cfg = MagicMock()
    target_cfg.db_type = "postgres"
    target_cfg.schema = "public"
    target_cfg.driver = "org.postgresql.Driver"
    target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"
    
    conn_mgr = MagicMock()
    conn_mgr.paths = {"landing_base": "C:/tmp", "history_base": "C:/tmp", "target_base": "C:/tmp"}
    conn_mgr.get.return_value = target_cfg
    target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"
    conn_mgr.resolve_jdbc.return_value = target_cfg
    
    loader = Loader(spark=spark, conn_manager=conn_mgr)
    
    with patch.object(loader, 'provision_target_schema_preflight', wraps=loader.provision_target_schema_preflight) as mock_preflight, \
         patch('src.load.loader.safe_execute_jdbc') as mock_safe_execute, \
         patch('src.load.loader.execute_upsert', return_value={"rows_upserted": 1}) as mock_upsert, \
         patch('src.utils.db_utils.apply_jdbc_options') as mock_apply, \
         patch('src.load.loader.apply_jdbc_options') as mock_loader_apply, \
         patch('src.load.loader.SchemaValidator.validate_dataframe') as mock_val_df, \
         patch('src.load.loader.SchemaValidator.validate_and_align_schema', return_value=(df, ["id", "val"])) as mock_align:
         
         # The preflight uses get_schema_inspector, we'll mock that to avoid real DB connections inside preflight
         with patch('src.load.target_schema_inspector.get_schema_inspector') as mock_inspector:
             mock_inspector_instance = MagicMock()
             mock_inspector_instance.supports_unique_key.return_value = True
             mock_inspector.return_value = mock_inspector_instance
             
             with pytest.raises(CDCPolicyNotMigratedError):
                 loader.write_target(
                     df,
                     meta,
                     merge_key="id",
                     before_target_dml=lambda: None,
                 )

def test_single_target_preflight_ownership_snapshot(spark):
    """
    H-01: Verify Loader owns preflight for Full Snapshot write_target_overwrite.
    """
    df = spark.createDataFrame([(1, "A")], ["id", "val"])
    
    meta = MagicMock()
    meta.id = 1
    meta.source_tag = "test_src"
    meta.source_table = "test_tbl"
    meta.target_table = "test_tbl"
    meta.target_schema = "public"
    meta.landing_mode = "DIRECT"
    meta.is_cdc = "n"
    meta.canonical_business_key_columns = None
    
    conn_mgr = MagicMock()
    conn_mgr.paths = {"landing_base": "C:/tmp", "history_base": "C:/tmp", "target_base": "C:/tmp"}
    target_conn = MagicMock()
    target_conn.schema = "public"
    conn_mgr.get.return_value = target_conn
    target_cfg = MagicMock()
    target_cfg.db_type = "postgres"
    target_cfg.schema = "public"
    target_cfg.driver = "org.postgresql.Driver"
    target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"
    conn_mgr.resolve_jdbc.return_value = target_cfg
    
    loader = Loader(spark=spark, conn_manager=conn_mgr)
    
    with patch.object(loader, 'provision_target_schema_preflight', wraps=loader.provision_target_schema_preflight) as mock_preflight, \
         patch('src.load.loader.safe_execute_jdbc') as mock_safe_execute, \
         patch('src.utils.db_utils.apply_jdbc_options') as mock_apply, \
         patch('src.load.loader.apply_jdbc_options') as mock_loader_apply, \
         patch('src.load.loader.SchemaValidator.validate_dataframe'), \
         patch('src.load.loader.SchemaValidator.validate_and_align_schema', return_value=(df, ["id", "val"])):
         
         with patch('src.load.target_schema_inspector.get_schema_inspector') as mock_inspector:
             mock_inspector_instance = MagicMock()
             mock_inspector.return_value = mock_inspector_instance
             
             loader.write_target_overwrite(
                 df,
                 meta,
                 before_target_dml=lambda: None,
             )
             
             assert mock_preflight.call_count == 1
