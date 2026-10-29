import unittest
from unittest.mock import MagicMock, patch
from src.extract.extractor import Extractor
from src.metadata.metadata_models import MetadataRow


class TestOracleLobExtraction(unittest.TestCase):
    def setUp(self):
        self.mock_spark = MagicMock()
        self.mock_conn_mgr = MagicMock()
        self.extractor = Extractor(self.mock_spark, self.mock_conn_mgr)

    def test_oracle_blob_and_bfile_projection_sql(self):
        """Verify Oracle projection handles BLOB (2000), BFILE (NULL cast), and CLOB (4000)."""
        meta = MagicMock(spec=MetadataRow)
        meta.id = 999
        meta.source_tag = "opf_oracle_housing"
        meta.source_table = "special_table"
        meta.source_schema = "housing1"
        meta.full_source_table = "housing1.special_table"
        meta.target_tag = "land_housing"
        meta.target_table = "special_table"
        meta.landing_mode = "DIRECT"
        meta.partition_column = None
        meta.num_partitions = 1
        meta.chunk_size = 50000

        mock_conn = MagicMock()
        mock_conn.db_type = "oracle"
        self.mock_conn_mgr.get.return_value = mock_conn

        # Mock metadata database containing BLOB, BFILE, CLOB, and regular columns
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("ID", "number"),
            ("DOC_FILE", "bfile"),
            ("PHOTO", "blob"),
            ("NOTES", "clob"),
        ]
        mock_meta_conn = MagicMock()
        mock_meta_conn.__enter__.return_value = mock_meta_conn
        mock_meta_conn.cursor.return_value.__enter__.return_value = mock_cursor
        self.mock_conn_mgr.get_metadata_db_connection.return_value = mock_meta_conn

        mock_reader = MagicMock()
        self.mock_spark.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        with patch.object(self.extractor.optimizer, "fetch_historical_stats", return_value={"average_size_mb": 0.0, "average_row_count": 0}):
            df = self.extractor.read(meta)

        # Check options passed to Spark JDBC reader
        option_calls = {call[0][0]: call[0][1] for call in mock_reader.option.call_args_list if len(call[0]) >= 2}
        dbtable = option_calls.get("dbtable", "")
        custom_schema = option_calls.get("customSchema", "")

        # Verify SQL projections
        self.assertIn('CAST(NULL AS VARCHAR2(500)) AS "DOC_FILE"', dbtable)
        self.assertIn('DBMS_LOB.SUBSTR("PHOTO", 2000, 1) AS "PHOTO"', dbtable)
        self.assertIn('DBMS_LOB.SUBSTR("NOTES", 4000, 1) AS "NOTES"', dbtable)
        self.assertIn('"ID"', dbtable)

        # Verify customSchema
        self.assertIn("DOC_FILE STRING", custom_schema)
        self.assertIn("PHOTO STRING", custom_schema)
        self.assertIn("NOTES STRING", custom_schema)


if __name__ == "__main__":
    unittest.main()
