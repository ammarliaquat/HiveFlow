from unittest.mock import MagicMock, patch

from gui.services.health_service import HealthService


def test_infrastructure_diagnostics_persist_connection_health():
    """Step 7 diagnostics should run per-connection tests and persist health telemetry."""
    checker = MagicMock()
    checker.check_repository_db.return_value = (True, "ok")
    checker.check_schema_version.return_value = (True, "ok")
    checker.check_metadata_validation.return_value = (True, 0, 0, 0)
    checker.check_spark_connect.return_value = (True, "ok")

    profiles = [{
        "tag": "msopforg_opforg",
        "db_type": "mysql",
        "server": "resolved-source-db-host",
        "port": 3306,
        "supports_source": True,
        "supports_target": False,
    }]

    with patch("gui.services.health_service.HealthChecker", return_value=checker), \
         patch("gui.services.health_service.ConnectionManager"), \
         patch("gui.services.health_service.ConnectionService.list_connections", return_value=profiles), \
         patch("gui.services.health_service.ConnectionService.test_connection", return_value={
             "status": "SUCCESS",
             "latency_ms": 42,
             "error": "",
         }) as mock_test:
        diagnostics = HealthService.run_infrastructure_diagnostics()

    mock_test.assert_called_once_with(profiles[0], update_db=True)
    assert any(row["Diagnostic Check"] == "Connection: msopforg_opforg" for row in diagnostics)
