from src.connections.connection_manager import ConnectionConfig, ConnectionManager, ExecutionContext
from src.exceptions import ConnectionResolutionError
from src.utils.db_utils import _parse_postgres_connection
import pytest
from unittest.mock import MagicMock

def test_negative_endpoint():
    print("Testing negative endpoint resolution...")
    # 1. Create a dummy connection config with an unresolvable host
    cfg = ConnectionConfig(
        source_tag="test_tag",
        db_type="postgres",
        jdbc_url="jdbc:postgresql://this.host.does.not.exist:5432/mydb",
        username="user",
        credential_reference="secret",
        driver="org.postgresql.Driver"
    )
    
    # 2. Resolve it to a ResolvedJdbcConfig
    cm = ConnectionManager.__new__(ConnectionManager)
    resolved = cm.resolve_jdbc(cfg, ExecutionContext.DRIVER)
    
    # 3. Simulate parsing it in db_utils
    with pytest.raises(ConnectionResolutionError) as exc:
        _parse_postgres_connection(resolved)
        
    assert "TARGET_RESOLUTION_FAILED" in str(exc.value)
    assert "this.host.does.not.exist" in str(exc.value)


def test_parse_postgres_connection_strips_query_parameters():
    """Verify _parse_postgres_connection strips query params so psycopg2 gets a clean dbname."""
    url = "jdbc:postgresql://192.0.2.1:5432/opf?reWriteBatchedInserts=true&sslmode=prefer"
    cfg = {"url": url}
    host, port, db = _parse_postgres_connection(cfg)
    assert host == "192.0.2.1"
    assert port == 5432
    assert db == "opf"
