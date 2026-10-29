"""Unit coverage for bounded-exception target publication SQL."""

import pytest

from src.utils.db_utils import build_bounded_exception_dml


def test_postgres_normal_lane_uses_partial_index_predicate():
    statements = build_bounded_exception_dml(
        db_type="postgresql",
        normal_stage_table="landing.events_stage",
        exception_stage_table="landing.events_exceptions_stage",
        target_table="landing.events",
        merge_key="event_id",
        columns=["event_id", "payload"],
        exception_values=[0, 9],
    )

    assert 'ON CONFLICT ("event_id") WHERE "event_id" NOT IN (0, 9)' in statements["normal_upsert"]
    assert statements["exception_replace"].startswith("WITH deleted AS (")
    assert 'INSERT INTO "landing"."events"' in statements["exception_replace"]


def test_postgres_exception_lane_is_atomic_and_keyed():
    statements = build_bounded_exception_dml(
        db_type="postgres",
        normal_stage_table="landing.events_normal_stage",
        exception_stage_table="landing.events_exception_stage",
        target_table="landing.events",
        merge_key=["event_id"],
        columns=["event_id", "payload"],
        exception_values=[0],
    )

    sql = statements["exception_replace"]
    assert 'DELETE FROM "landing"."events" AS t' in sql
    assert 'WHERE t."event_id" IN (0)' in sql
    assert 'SELECT "event_id", "payload" FROM "landing"."events_exception_stage"' in sql


def test_postgres_exception_delete_is_independent_of_empty_stage_contents():
    """Frozen exception keys are removed even when the current stage has no rows."""
    statements = build_bounded_exception_dml(
        db_type="postgres",
        normal_stage_table="landing.events_normal_stage",
        exception_stage_table="landing.events_exception_stage",
        target_table="landing.events",
        merge_key="event_id",
        columns=["event_id", "payload"],
        exception_values=[0, 9],
    )

    delete_cte = statements["exception_replace"].split(" RETURNING 1", 1)[0]
    assert 'WHERE t."event_id" IN (0, 9)' in delete_cte
    assert 'events_exception_stage' not in delete_cte


@pytest.mark.parametrize("db_type", ["oracle", "mysql", "mssql"])
def test_non_postgres_adapters_fail_closed_for_atomic_exception_replace(db_type):
    statements = build_bounded_exception_dml(
        db_type=db_type,
        normal_stage_table="landing.events_normal_stage",
        exception_stage_table="landing.events_exception_stage",
        target_table="landing.events",
        merge_key="event_id",
        columns=["event_id"],
        exception_values=[0],
    )
    assert statements["exception_replace"] is None


def test_exception_values_are_required():
    with pytest.raises(ValueError, match="exception values"):
        build_bounded_exception_dml(
            db_type="postgres",
            normal_stage_table="landing.events_normal_stage",
            exception_stage_table="landing.events_exception_stage",
            target_table="landing.events",
            merge_key="event_id",
            columns=["event_id"],
            exception_values=[],
        )


@pytest.mark.parametrize("invalid", [["0"], [0.0], [True]])
def test_exception_values_are_strictly_typed(invalid):
    with pytest.raises(ValueError, match="exception values are invalid"):
        build_bounded_exception_dml(
            db_type="postgres",
            normal_stage_table="landing.events_normal_stage",
            exception_stage_table="landing.events_exception_stage",
            target_table="landing.events",
            merge_key="event_id",
            columns=["event_id"],
            exception_values=invalid,
        )


def test_bounded_exception_dml_no_update_cols_emits_do_nothing():
    statements = build_bounded_exception_dml(
        db_type="postgres",
        normal_stage_table="landing.events_stage",
        exception_stage_table="landing.events_exc_stage",
        target_table="landing.events",
        merge_key="event_id",
        columns=["event_id"],
        exception_values=[0],
    )
    assert "DO NOTHING" in statements["normal_upsert"]


def test_bounded_exception_dml_case_insensitive_key_matching():
    statements = build_bounded_exception_dml(
        db_type="postgres",
        normal_stage_table="landing.events_stage",
        exception_stage_table="landing.events_exc_stage",
        target_table="landing.events",
        merge_key="EVENT_ID",
        columns=["event_id", "status"],
        exception_values=[0],
    )
    assert 'ON CONFLICT ("EVENT_ID") WHERE "EVENT_ID" NOT IN (0)' in statements["normal_upsert"]
    assert 'DO UPDATE SET "status" = EXCLUDED."status"' in statements["normal_upsert"]


def test_execute_upsert_uses_partial_index_when_conditional_exceptions_provided(monkeypatch):
    from unittest.mock import MagicMock
    from src.utils.db_utils import execute_upsert

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
        merge_key="reg_id",
        columns=["reg_id", "name"],
        db_type="postgres",
        before_target_dml=lambda: None,
        conditional_exception_values=[0],
    )

    assert len(executed_sql) == 1
    assert 'ON CONFLICT ("reg_id") WHERE "reg_id" NOT IN (0)' in executed_sql[0]
    assert 'DO UPDATE SET "name" = EXCLUDED."name"' in executed_sql[0]

