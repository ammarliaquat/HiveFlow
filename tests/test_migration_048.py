"""Static contract tests for target-scoped Tier-2 subscription identity."""
from pathlib import Path

from src.connections.schema_certifier import CANONICAL_MIGRATIONS


def test_multi_target_tier2_subscription_migration_is_canonical_and_precise():
    """Migration 048 replaces only source-only Tier-2 identity rules."""
    migration_name = "048_multi_target_tier2_subscriptions.sql"
    assert CANONICAL_MIGRATIONS[-1] == migration_name

    sql = (Path(__file__).resolve().parents[1] / "migrations" / migration_name).read_text(encoding="utf-8")
    assert "DROP CONSTRAINT IF EXISTS uq_db_tables_etl_source" in sql
    assert "DROP INDEX IF EXISTS metadata.uq_db_tables_etl_source_master" in sql
    assert "UNIQUE (source_tag, source_schema, source_table, target_tag)" in sql
    assert "ON metadata.db_tables_etl (source_table_master_key, target_tag)" in sql
    assert "source_table_master_key IS NOT NULL" in sql
    assert "target_tag IS NOT NULL" in sql
