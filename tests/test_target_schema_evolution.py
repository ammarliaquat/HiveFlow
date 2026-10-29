from types import SimpleNamespace
from unittest.mock import MagicMock

from src.load.loader import Loader


def _field(name, simple_type):
    data_type = SimpleNamespace(simpleString=lambda: simple_type)
    return SimpleNamespace(name=name, dataType=data_type)


def test_permissive_postgres_target_evolution_adds_all_frozen_projection_columns():
    df = SimpleNamespace(
        schema=SimpleNamespace(
            fields=[
                _field("emigrant_id", "decimal(38,10)"),
                _field("full_name", "string"),
                _field("pipeline_run_id", "string"),
            ]
        )
    )
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    manager = MagicMock()
    manager.get_db_connection.return_value.__enter__.return_value = conn
    loader = Loader.__new__(Loader)
    loader.conn_manager = manager
    meta = SimpleNamespace(
        target_tag="land_test",
        schema_evolution_policy="PERMISSIVE",
    )
    target_cfg = SimpleNamespace(db_type="postgres")

    changed = loader._evolve_target_schema(
        df=df,
        meta=meta,
        target_cfg=target_cfg,
        target_fqn="landing.events",
        target_schema_fields=[
            _field("emigrant_id", "decimal(38,10)"),
            _field("pipeline_run_id", "string"),
        ],
        before_persist=lambda: None,
    )

    assert changed is True
    sql = cursor.execute.call_args.args[0]
    assert 'ADD COLUMN IF NOT EXISTS "full_name" TEXT' in sql
    assert "emigrant_id" not in sql
    conn.commit.assert_called_once()
