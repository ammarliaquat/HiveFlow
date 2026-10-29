"""REC-01 synchronous Gold DML lease-fencing tests."""

from unittest.mock import MagicMock

import pytest

from src.logging_.batch_logger import BatchLogger, ExecutionLeaseLostError


def _logger_with_cursor(cursor):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.get_metadata_db_connection.return_value.__enter__.return_value = conn
    logger = object.__new__(BatchLogger)
    logger._conn_manager = manager
    logger._attempt_numbers = {17: 3}
    return logger


def test_renew_active_lease_uses_running_attempt_fence_and_database_time():
    cursor = MagicMock()
    cursor.fetchone.return_value = (17,)
    logger = _logger_with_cursor(cursor)

    logger.renew_active_lease_or_raise(17)

    statement, params = cursor.execute.call_args.args
    assert "CURRENT_TIMESTAMP" in statement
    assert "status = 'RUNNING'" in statement
    assert "attempt_number = %s" in statement
    assert params == (17, 3)


def test_renew_active_lease_raises_when_reaper_or_retry_owns_the_row():
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    logger = _logger_with_cursor(cursor)

    with pytest.raises(ExecutionLeaseLostError, match="LEASE_FENCED"):
        logger.renew_active_lease_or_raise(17)
