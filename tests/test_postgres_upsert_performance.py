"""Unit tests for PostgreSQL UPSERT performance optimizations and change detection."""

import pytest
from unittest.mock import MagicMock
from src.utils.db_utils import _build_postgres_upsert, build_bounded_exception_dml, execute_upsert, DBType


def test_build_postgres_upsert_includes_change_detection_where_clause():
    """Verify that _build_postgres_upsert generates a change-detection WHERE clause."""
    sql = _build_postgres_upsert(
        target_table="land.tblop_dec24",
        stage_table="land.tblop_dec24_stg",
        merge_keys=["emigrant_id"],
        columns=[
            "emigrant_id",
            "full_name",
            "salary_amount",
            "source_identification_key",
            "dml_timestamp",
            "landing_ingestion_timestamp",
            "pipeline_run_id",
        ],
    )

    # Must use table alias 'AS t'
    assert 'INSERT INTO "land"."tblop_dec24" AS t' in sql
    assert 'ON CONFLICT ("emigrant_id") DO UPDATE SET' in sql

    # Must update all non-key columns including timestamps
    assert '"full_name" = EXCLUDED."full_name"' in sql
    assert '"salary_amount" = EXCLUDED."salary_amount"' in sql
    assert '"source_identification_key" = EXCLUDED."source_identification_key"' in sql
    assert '"dml_timestamp" = EXCLUDED."dml_timestamp"' in sql
    assert '"landing_ingestion_timestamp" = EXCLUDED."landing_ingestion_timestamp"' in sql
    assert '"pipeline_run_id" = EXCLUDED."pipeline_run_id"' in sql

    # Change-detection WHERE clause must contain business and lineage columns
    assert "WHERE (" in sql
    assert 't."full_name"' in sql
    assert 't."salary_amount"' in sql
    assert 't."source_identification_key"' in sql

    # Crucial: Ephemeral run timestamps must NOT be in the change-detection WHERE clause
    # Extract the WHERE clause
    where_part = sql.split("WHERE (")[1]
    target_tuple = where_part.split(") IS DISTINCT FROM")[0]

    assert 't."dml_timestamp"' not in target_tuple
    assert 't."landing_ingestion_timestamp"' not in target_tuple
    assert 't."pipeline_run_id"' not in target_tuple


def test_build_postgres_upsert_key_only_table_emits_do_nothing():
    """Key-only table should emit DO NOTHING without WHERE clause."""
    sql = _build_postgres_upsert(
        target_table="land.test_keys",
        stage_table="land.test_keys_stg",
        merge_keys=["id"],
        columns=["id"],
    )
    assert 'ON CONFLICT ("id") DO NOTHING' in sql
    assert "DO UPDATE SET" not in sql
    assert "WHERE" not in sql


def test_build_bounded_exception_dml_includes_change_detection():
    """Verify bounded exception normal lane includes change detection predicate."""
    statements = build_bounded_exception_dml(
        db_type="postgres",
        normal_stage_table="land.events_normal_stg",
        exception_stage_table="land.events_exc_stg",
        target_table="land.events",
        merge_key="id",
        columns=["id", "val", "dml_timestamp"],
        exception_values=[0],
    )

    normal_sql = statements["normal_upsert"]
    assert 'INSERT INTO "land"."events" AS t' in normal_sql
    assert 'ON CONFLICT ("id") WHERE "id" NOT IN (0)' in normal_sql
    assert 'DO UPDATE SET "val" = EXCLUDED."val", "dml_timestamp" = EXCLUDED."dml_timestamp"' in normal_sql
    assert 'WHERE (t."val") IS DISTINCT FROM (EXCLUDED."val")' in normal_sql
    assert 't."dml_timestamp"' not in normal_sql.split("WHERE (t.")[1]


def test_execute_upsert_prepends_postgres_session_tuning(monkeypatch):
    """Verify execute_upsert prepends session settings and autovacuum disablement for PostgreSQL."""
    executed_sql = []

    def mock_safe_execute_jdbc(df, target_cfg, operation, spark):
        if operation == "execute_sql":
            executed_sql.append(target_cfg.get("sql"))

    monkeypatch.setattr("src.utils.db_utils.safe_execute_jdbc", mock_safe_execute_jdbc)
    monkeypatch.setattr("src.utils.db_utils._collect_postgres_merge_stats", lambda *a, **kw: None)
    monkeypatch.setattr("src.utils.db_utils._verify_staging_target_convergence", lambda **kw: {"matched_rows": 1, "missing_rows": 0})

    mock_df = MagicMock()
    mock_df.count.return_value = 1
    mock_writer = MagicMock()
    mock_df.write.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    execute_upsert(
        spark=MagicMock(),
        df=mock_df,
        target_cfg={"url": "jdbc:postgresql://localhost/test", "user": "u", "password": "p", "driver": "org.postgresql.Driver"},
        target_table="land.tblop_dec24",
        merge_key="emigrant_id",
        columns=["emigrant_id", "salary_amount"],
        db_type="postgres",
        before_target_dml=lambda: None,
    )

    assert len(executed_sql) == 1
    dml = executed_sql[0]
    assert "SET synchronous_commit = 'off';" in dml
    assert "SET work_mem = '64MB';" in dml
    assert 'ALTER TABLE "land"."tblop_dec24_stg" SET (autovacuum_enabled = false);' in dml
    assert 'INSERT INTO "land"."tblop_dec24" AS t' in dml


def test_build_postgres_upsert_resolves_uppercase_key_to_lowercase_target():
    """Verify that uppercase metadata key resolves to lowercase when target columns are lowercase."""
    sql = _build_postgres_upsert(
        target_table="eland.tbltaskreminder",
        stage_table="eland.tbltaskreminder_stg",
        merge_keys=["TASKREMINDER_ID"],
        columns=["taskreminder_id", "task_id", "reminder_date", "is_active"],
    )

    # Conflict target must use lowercase target column casing
    assert 'ON CONFLICT ("taskreminder_id") DO UPDATE SET' in sql
    # Conflict key must NOT be in update set clause
    assert '"taskreminder_id" = EXCLUDED."taskreminder_id"' not in sql
    # Non-key columns must be updated
    assert '"task_id" = EXCLUDED."task_id"' in sql
    assert '"reminder_date" = EXCLUDED."reminder_date"' in sql


def test_build_postgres_upsert_resolves_lowercase_key_to_uppercase_target():
    """Verify that lowercase metadata key resolves to uppercase when target columns are uppercase."""
    sql = _build_postgres_upsert(
        target_table="eland.tbllocation",
        stage_table="eland.tbllocation_stg",
        merge_keys=["loc_id"],
        columns=["LOC_ID", "LOC_NAME", "IS_ACTIVE"],
    )

    # Conflict target must use uppercase target column casing
    assert 'ON CONFLICT ("LOC_ID") DO UPDATE SET' in sql
    # Conflict key must NOT be in update set clause
    assert '"LOC_ID" = EXCLUDED."LOC_ID"' not in sql
    # Non-key columns must be updated
    assert '"LOC_NAME" = EXCLUDED."LOC_NAME"' in sql

