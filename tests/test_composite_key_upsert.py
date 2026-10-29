# tests/test_composite_key_upsert.py
import pytest
from unittest.mock import MagicMock, patch

from src.utils.db_utils import normalize_and_validate_merge_keys
from src.load.loader import Loader, GoldWriteError


class TestStage1KeyNormalizationAndValidation:

    def test_single_key_string(self):
        result = normalize_and_validate_merge_keys("customer_id")
        assert result == ["customer_id"]

    def test_single_key_list(self):
        result = normalize_and_validate_merge_keys(["customer_id"])
        assert result == ["customer_id"]

    def test_composite_key_string_with_whitespace(self):
        result = normalize_and_validate_merge_keys(" customer_id , account_id ")
        assert result == ["customer_id", "account_id"]

    def test_composite_key_list_with_whitespace(self):
        result = normalize_and_validate_merge_keys([" customer_id ", " account_id "])
        assert result == ["customer_id", "account_id"]

    def test_reject_empty_string(self):
        with pytest.raises(ValueError, match="Merge key cannot be empty"):
            normalize_and_validate_merge_keys("")

    def test_reject_empty_list(self):
        with pytest.raises(ValueError, match="Merge key cannot be empty"):
            normalize_and_validate_merge_keys([])

    def test_reject_none(self):
        with pytest.raises(ValueError, match="Merge key cannot be empty"):
            normalize_and_validate_merge_keys(None)

    def test_reject_malformed_middle_empty_part(self):
        with pytest.raises(ValueError, match="contains empty column element"):
            normalize_and_validate_merge_keys("a,,b")

    def test_reject_malformed_leading_empty_part(self):
        with pytest.raises(ValueError, match="contains empty column element"):
            normalize_and_validate_merge_keys(",a")

    def test_reject_malformed_trailing_empty_part(self):
        with pytest.raises(ValueError, match="contains empty column element"):
            normalize_and_validate_merge_keys("a,")

    def test_reject_duplicate_column_elements(self):
        with pytest.raises(ValueError, match="contains duplicate column elements"):
            normalize_and_validate_merge_keys("customer_id, customer_id")

    def test_unknown_column_validation(self):
        columns = ["customer_id", "balance", "status"]
        with pytest.raises(ValueError, match="not found in target columns"):
            normalize_and_validate_merge_keys("customer_id, non_existent_col", columns=columns)

    def test_case_insensitive_matching_returns_canonical_column_name(self):
        columns = ["customer_id", "account_id", "balance"]
        result = normalize_and_validate_merge_keys("CUSTOMER_ID, Account_Id", columns=columns)
        assert result == ["customer_id", "account_id"]


class TestStage1StagingDuplicateDetection:

    def test_staging_duplicate_detection_raises_error(self, spark):
        # Create a PySpark DataFrame with duplicate business keys
        data = [
            (101, "A", 1000.0),
            (101, "A", 1500.0),  # Duplicate business key (101, "A")
            (102, "B", 2000.0),
        ]
        columns = ["customer_id", "account_id", "balance"]
        df = spark.createDataFrame(data, columns)

        meta = MagicMock()
        meta.source_table = "test_bank_accounts"
        meta.target_table = "test_bank_accounts"
        meta.target_schema = "public"
        meta.landing_mode = "DIRECT"
        meta.is_cdc = "y"
        meta.canonical_source_business_key = ("customer_id", "account_id")

        target_cfg = MagicMock()
        target_cfg.schema = "public"
        target_cfg.driver = "org.postgresql.Driver"
        target_cfg.db_type = "postgres"
        target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"

        conn_mgr = MagicMock()
        conn_mgr.paths = {"landing_base": "C:/tmp/landing", "history_base": "C:/tmp/history"}
        conn_mgr.get.return_value = target_cfg

        loader = Loader(spark=spark, conn_manager=conn_mgr)
    
        with patch('src.validation.stage0_policy.has_stage0_trusted_business_key', return_value=True), \
             patch('src.validation.stage0_policy.validate_stage0_execution_policy'), \
             patch('src.load.loader.apply_jdbc_options') as mock_apply, \
             patch('src.load.target_schema_inspector.get_schema_inspector') as mock_inspector:
            
            mock_inspector_instance = MagicMock()
            mock_inspector_instance.supports_unique_key.return_value = True
            mock_inspector.return_value = mock_inspector_instance
            
            with pytest.raises(Exception) as excinfo:
                loader._upsert_via_staging(df, meta, target_cfg, merge_key="customer_id, account_id", before_target_dml=lambda: None)

        assert "TARGET_KEY_DUPLICATES" in str(excinfo.value)
        assert "customer_id" in str(excinfo.value)

    def test_staging_unique_keys_pass_validation(self, spark):
        # Create a PySpark DataFrame with unique business keys
        data = [
            (101, "A", 1000.0),
            (101, "B", 1500.0),
            (102, "A", 2000.0),
        ]
        columns = ["customer_id", "account_id", "balance"]
        df = spark.createDataFrame(data, columns)

        meta = MagicMock()
        meta.source_table = "test_bank_accounts"
        meta.target_table = "test_bank_accounts"
        meta.target_schema = "public"
        meta.landing_mode = "DIRECT"
        meta.canonical_source_business_key = ("customer_id", "account_id")

        target_cfg = MagicMock()
        target_cfg.schema = "public"
        target_cfg.driver = "org.postgresql.Driver"
        target_cfg.db_type = "postgres"
        target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"

        conn_mgr = MagicMock()
        conn_mgr.paths = {"landing_base": "C:/tmp/landing", "history_base": "C:/tmp/history"}

        loader = Loader(spark=spark, conn_manager=conn_mgr)
        loader.provision_target_schema_preflight = MagicMock()

        # Mock execute_upsert to avoid real DB execution
        import src.load.loader as loader_module
        original_execute_upsert = getattr(loader_module, "execute_upsert", None)
        loader_module.execute_upsert = MagicMock(return_value={"rows_upserted": 3})

        try:
            path, metrics = loader._upsert_via_staging(df, meta, target_cfg, merge_key="customer_id, account_id")
            assert metrics == {"rows_upserted": 3}
        finally:
            if original_execute_upsert:
                loader_module.execute_upsert = original_execute_upsert


class TestStage2DialectSQLBuilders:

    # ── PostgreSQL Dialect Tests ──────────────────────────────────────────────
    def test_postgres_single_key(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.POSTGRES,
            stage_table="public.stg_accounts",
            target_table="public.accounts",
            merge_key="id",
            columns=["id", "balance", "status"]
        )
        assert 'ON CONFLICT ("id") DO UPDATE SET "balance" = EXCLUDED."balance", "status" = EXCLUDED."status"' in sql
        assert '"id" = EXCLUDED."id"' not in sql  # Key excluded from update

    def test_postgres_composite_key(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.POSTGRES,
            stage_table="public.stg_accounts",
            target_table="public.accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id", "balance", "status"]
        )
        assert 'ON CONFLICT ("customer_id", "account_id") DO UPDATE SET "balance" = EXCLUDED."balance", "status" = EXCLUDED."status"' in sql
        assert '"customer_id" = EXCLUDED."customer_id"' not in sql
        assert '"account_id" = EXCLUDED."account_id"' not in sql

    def test_postgres_three_column_composite_key(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.POSTGRES,
            stage_table="public.stg_accounts",
            target_table="public.accounts",
            merge_key="tenant_id, customer_id, account_id",
            columns=["tenant_id", "customer_id", "account_id", "balance"]
        )
        assert 'ON CONFLICT ("tenant_id", "customer_id", "account_id") DO UPDATE SET "balance" = EXCLUDED."balance"' in sql

    def test_postgres_key_only_table(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.POSTGRES,
            stage_table="public.stg_accounts",
            target_table="public.accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id"]
        )
        assert 'ON CONFLICT ("customer_id", "account_id") DO NOTHING' in sql
        assert 'DO UPDATE SET' not in sql

    # ── Oracle Dialect Tests ──────────────────────────────────────────────────
    def test_oracle_composite_key(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.ORACLE,
            stage_table="stg_accounts",
            target_table="accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id", "balance"]
        )
        assert "MERGE INTO accounts t USING stg_accounts s ON (t.customer_id = s.customer_id AND t.account_id = s.account_id)" in sql
        assert "WHEN MATCHED THEN UPDATE SET t.balance = s.balance" in sql
        assert "t.customer_id = s.customer_id" not in sql.split("WHEN MATCHED")[1]  # Key excluded from update

    def test_oracle_key_only_table(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.ORACLE,
            stage_table="stg_accounts",
            target_table="accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id"]
        )
        assert "MERGE INTO accounts t USING stg_accounts s ON (t.customer_id = s.customer_id AND t.account_id = s.account_id)" in sql
        assert "WHEN MATCHED THEN UPDATE" not in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    # ── MSSQL Dialect Tests ───────────────────────────────────────────────────
    def test_mssql_composite_key(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.MSSQL,
            stage_table="dbo.stg_accounts",
            target_table="dbo.accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id", "balance"]
        )
        assert 'MERGE "dbo"."accounts" AS t USING "dbo"."stg_accounts" AS s ON t."customer_id" = s."customer_id" AND t."account_id" = s."account_id"' in sql
        assert 'WHEN MATCHED THEN UPDATE SET t."balance" = s."balance"' in sql

    def test_mssql_key_only_table(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.MSSQL,
            stage_table="dbo.stg_accounts",
            target_table="dbo.accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id"]
        )
        assert "WHEN MATCHED THEN UPDATE" not in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    # ── MySQL Dialect Tests ───────────────────────────────────────────────────
    def test_mysql_composite_key(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.MYSQL,
            stage_table="stg_accounts",
            target_table="accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id", "balance", "status"]
        )
        assert "INSERT INTO `accounts` (`customer_id`, `account_id`, `balance`, `status`)" in sql
        assert "ON DUPLICATE KEY UPDATE `balance` = VALUES(`balance`), `status` = VALUES(`status`)" in sql

    def test_mysql_key_only_table(self):
        from src.utils.db_utils import build_upsert_sql, DBType
        sql = build_upsert_sql(
            db_type=DBType.MYSQL,
            stage_table="stg_accounts",
            target_table="accounts",
            merge_key="customer_id, account_id",
            columns=["customer_id", "account_id"]
        )
        assert "ON DUPLICATE KEY UPDATE `customer_id` = `customer_id`" in sql


class TestStage3IdempotencyConvergence:

    def test_stage3_repeated_write_convergence_and_cardinality_preservation(self, spark):
        """Verify 5 consecutive executions on identical source snapshot produce identical target business state."""
        data = [
            (101, "A", 1000.0),
            (101, "B", 1500.0),
            (102, "A", 2000.0),
        ]
        columns = ["customer_id", "account_id", "balance"]
        df = spark.createDataFrame(data, columns)

        meta = MagicMock()
        meta.source_table = "bank_accounts"
        meta.target_table = "bank_accounts"
        meta.target_schema = "public"
        meta.landing_mode = "DIRECT"
        meta.canonical_source_business_key = ("customer_id", "account_id")

        target_cfg = MagicMock()
        target_cfg.schema = "public"
        target_cfg.driver = "org.postgresql.Driver"
        target_cfg.db_type = "postgres"
        target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"

        conn_mgr = MagicMock()
        conn_mgr.paths = {"landing_base": "C:/tmp/landing", "history_base": "C:/tmp/history"}

        loader = Loader(spark=spark, conn_manager=conn_mgr)
        loader.provision_target_schema_preflight = MagicMock()

        # Simulate in-memory target DB table state
        target_db_state = {}

        def mock_execute_upsert(spark, df, target_cfg, target_table, merge_key, columns, db_type, **kwargs):
            rows = df.collect()
            for r in rows:
                key = (r["customer_id"], r["account_id"])
                target_db_state[key] = r["balance"]
            return {"rows_upserted": len(rows)}

        import src.load.loader as loader_module
        original_execute_upsert = getattr(loader_module, "execute_upsert", None)
        loader_module.execute_upsert = MagicMock(side_effect=mock_execute_upsert)

        try:
            # Run 1 to Run 5
            for run_num in range(1, 6):
                path, metrics = loader._upsert_via_staging(df, meta, target_cfg, merge_key="customer_id, account_id")
                assert len(target_db_state) == 3, f"Run {run_num}: Target cardinality must remain exactly 3"
                assert target_db_state[(101, "A")] == 1000.0
                assert target_db_state[(101, "B")] == 1500.0
                assert target_db_state[(102, "A")] == 2000.0
        finally:
            if original_execute_upsert:
                loader_module.execute_upsert = original_execute_upsert

    def test_stage3_non_key_update_value_convergence(self, spark):
        """Verify updating non-key business values updates target in-place without altering key set cardinality."""
        columns = ["customer_id", "account_id", "balance"]
        
        # Snapshot 1
        df1 = spark.createDataFrame([(101, "A", 1000.0)], columns)
        # Snapshot 2 (updated balance)
        df2 = spark.createDataFrame([(101, "A", 2500.0)], columns)

        meta = MagicMock()
        meta.source_table = "bank_accounts"
        meta.target_table = "bank_accounts"
        meta.target_schema = "public"
        meta.landing_mode = "DIRECT"
        meta.canonical_source_business_key = ("customer_id", "account_id")

        target_cfg = MagicMock()
        target_cfg.schema = "public"
        target_cfg.driver = "org.postgresql.Driver"
        target_cfg.db_type = "postgres"
        target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"

        conn_mgr = MagicMock()
        conn_mgr.paths = {"landing_base": "C:/tmp/landing", "history_base": "C:/tmp/history"}

        loader = Loader(spark=spark, conn_manager=conn_mgr)
        loader.provision_target_schema_preflight = MagicMock()

        target_db_state = {}

        def mock_execute_upsert(spark, df, target_cfg, target_table, merge_key, columns, db_type, **kwargs):
            rows = df.collect()
            for r in rows:
                key = (r["customer_id"], r["account_id"])
                target_db_state[key] = r["balance"]
            return {"rows_upserted": len(rows)}

        import src.load.loader as loader_module
        original_execute_upsert = getattr(loader_module, "execute_upsert", None)
        loader_module.execute_upsert = MagicMock(side_effect=mock_execute_upsert)

        try:
            # Write Snapshot 1
            loader._upsert_via_staging(df1, meta, target_cfg, merge_key="customer_id, account_id")
            assert len(target_db_state) == 1
            assert target_db_state[(101, "A")] == 1000.0

            # Write Snapshot 2
            loader._upsert_via_staging(df2, meta, target_cfg, merge_key="customer_id, account_id")
            assert len(target_db_state) == 1, "Cardinality must remain 1 after non-key update"
            assert target_db_state[(101, "A")] == 2500.0, "Non-key value must converge to 2500.0"
        finally:
            if original_execute_upsert:
                loader_module.execute_upsert = original_execute_upsert

    def test_stage3_acknowledgement_loss_retry_preserves_target_cardinality(self, spark):
        """Simulate DB write success followed by lost ACK; retry must preserve target business state & cardinality."""
        columns = ["customer_id", "account_id", "balance"]
        df = spark.createDataFrame([(101, "A", 1000.0), (102, "B", 2000.0)], columns)

        meta = MagicMock()
        meta.source_table = "bank_accounts"
        meta.target_table = "bank_accounts"
        meta.target_schema = "public"
        meta.landing_mode = "DIRECT"
        meta.canonical_source_business_key = ("customer_id", "account_id")

        target_cfg = MagicMock()
        target_cfg.schema = "public"
        target_cfg.driver = "org.postgresql.Driver"
        target_cfg.db_type = "postgres"
        target_cfg.url = "jdbc:postgresql://localhost:5432/mydb"

        conn_mgr = MagicMock()
        conn_mgr.paths = {"landing_base": "C:/tmp/landing", "history_base": "C:/tmp/history"}

        loader = Loader(spark=spark, conn_manager=conn_mgr)
        loader.provision_target_schema_preflight = MagicMock()

        target_db_state = {}
        call_count = [0]

        def mock_execute_upsert_ack_loss(spark, df, target_cfg, target_table, merge_key, columns, db_type, **kwargs):
            call_count[0] += 1
            rows = df.collect()
            for r in rows:
                key = (r["customer_id"], r["account_id"])
                target_db_state[key] = r["balance"]
            if call_count[0] == 1:
                # DB committed successfully, but network connection dropped before ACK returned
                raise Exception("Simulated connection timeout after commit (acknowledgement lost)")
            return {"rows_upserted": len(rows)}

        import src.load.loader as loader_module
        original_execute_upsert = getattr(loader_module, "execute_upsert", None)
        loader_module.execute_upsert = MagicMock(side_effect=mock_execute_upsert_ack_loss)

        try:
            # Attempt 1: DB commits but ACK lost -> raises exception
            with pytest.raises(Exception, match="acknowledgement lost"):
                loader._upsert_via_staging(df, meta, target_cfg, merge_key="customer_id, account_id")

            assert len(target_db_state) == 2, "DB committed 2 rows prior to ACK loss"

            # Retry Attempt 2: Pipeline retries same batch
            path, metrics = loader._upsert_via_staging(df, meta, target_cfg, merge_key="customer_id, account_id")
            assert metrics == {"rows_upserted": 2}
            assert len(target_db_state) == 2, "Retry must preserve cardinality = 2 without duplicate key insertion"
        finally:
            if original_execute_upsert:
                loader_module.execute_upsert = original_execute_upsert


