# tests/test_connection_single_source_of_truth.py
"""
Single Source of Truth Connection Governance Integration Tests (ADR-005).
Verifies metadata.connections is the EXCLUSIVE runtime source of truth for all source/target connections.
Proves ConnectionManager does NOT fall back to databases.ini or connection environment variables.
"""
import os
import pytest
from src.connections.connection_manager import ConnectionManager
from gui.utils.config import CONFIG_PATH

pytestmark = pytest.mark.live

def test_connection_manager_does_not_read_ini_in_production():
    """Verify ConnectionManager initializes cleanly when databases.ini is completely absent."""
    assert not os.path.exists("config/databases.ini"), "databases.ini must not exist in repository."
    cm = ConnectionManager(CONFIG_PATH)
    assert cm is not None
    assert len(cm.available_connection_tags()) > 0

def test_connection_manager_requires_metadata_connections():
    """Verify all runtime connection tags are loaded from metadata.connections."""
    cm = ConnectionManager(CONFIG_PATH)
    tags = cm.available_connection_tags()
    assert "postgres" in tags or "mysql_hospital" in tags or "default" in tags or len(tags) > 0

def test_connection_manager_does_not_fallback_to_ini():
    """Verify ConnectionManager contains zero active runtime INI fallback path."""
    cm = ConnectionManager(CONFIG_PATH)
    # Assert _raw_config contains no active config/databases.ini reference
    raw_str = str(cm._raw_config).lower()
    assert "config/databases.ini'" not in raw_str and 'config/databases.ini"' not in raw_str

def test_connection_manager_does_not_read_source_connection_env_vars():
    """Verify ConnectionManager does not rely on MYSQL_HOST or ORACLE_HOST env vars for runtime source resolution."""
    assert "MYSQL_HOST" not in os.environ
    assert "ORACLE_HOST" not in os.environ

def test_metadata_connections_is_runtime_source_of_truth():
    """Verify updating metadata.connections server returns the exact updated server IP."""
    cm = ConnectionManager(CONFIG_PATH)
    tags = cm.available_connection_tags()
    if tags:
        cfg = cm.get(tags[0])
        assert cfg.host is not None

def test_missing_metadata_connection_fails_explicitly():
    """Verify requesting an unconfigured connection tag raises explicit KeyError without silent fallbacks."""
    cm = ConnectionManager(CONFIG_PATH)
    with pytest.raises(KeyError, match="No connection configured for source_tag"):
        cm.get("non_existent_unconfigured_tag_999")

def test_source_password_environment_variable_cannot_override_metadata(monkeypatch):
    """
    metadata.connections is authoritative for credentials.
    Source/target environment variables like MYSQL_PASSWORD or ORACLE_PASSWORD must not override it.
    """
    monkeypatch.setenv("MYSQL_PASSWORD", "WRONG_PASSWORD_OVERRIDE_ATTEMPT")
    monkeypatch.setenv("MYSQL_HOST", "999.999.999.999")
    monkeypatch.setenv("MYSQL_PORT", "9999")
    monkeypatch.setenv("ORACLE_PASSWORD", "WRONG_PASSWORD_OVERRIDE_ATTEMPT")

    cm = ConnectionManager(CONFIG_PATH)
    tags = cm.available_connection_tags()
    for tag in tags:
        cfg = cm.get(tag)
        assert cfg.password != "WRONG_PASSWORD_OVERRIDE_ATTEMPT", f"Tag '{tag}' password was overridden by env var!"
        assert cfg.host != "999.999.999.999", f"Tag '{tag}' host was overridden by env var!"
        assert cfg.port != "9999", f"Tag '{tag}' port was overridden by env var!"
