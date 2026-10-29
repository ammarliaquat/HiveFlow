# tests/test_delta_days_skip.py
"""Unit tests verifying delta_days scheduling and skip logic."""
from unittest.mock import MagicMock
import pytest
from src.metadata.metadata_reader import MetadataReader


def test_delta_days_sql_generation():
    """Verify that delta_days skip predicates are properly included in the SQL query."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.description = [('id',), ('source_tag',), ('source_schema',), ('source_table',)]
    mock_cur.fetchall.return_value = []

    mock_cm = MagicMock()
    mock_cm.get_metadata_db_connection.return_value.__enter__.return_value = mock_conn

    reader = MetadataReader(mock_cm)
    reader.get_tables("opf_oracle")

    call_args = mock_cur.execute.call_args[0]
    sql_query = call_args[0]

    # Check that daily check is present
    assert "etl.last_success_date < CURRENT_DATE" in sql_query
    # Check that weekly/monthly delta_days check is present
    assert "COALESCE(etl.delta_days, 1) > 1" in sql_query
    assert "etl.last_success_date <= CURRENT_DATE - (etl.delta_days * INTERVAL '1 day')" in sql_query
    # Check that rerun_flag and NULL last_success_date override delta_days
    assert "etl.rerun_flag = 'y'" in sql_query
    assert "etl.last_success_date IS NULL" in sql_query
