import pytest

from src.contracts.runtime_settings import RuntimeSettings
from src.exceptions import ExecutionProfileInvalidError
from src.migrations.migration_runner import _apply_migration, run_canonical_migrations


class RecordingCursor:
    def __init__(self, conn, fail_on=None):
        self.conn = conn
        self.fail_on = fail_on

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((self.conn.autocommit, sql, params))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("simulated execute failure")


class RecordingConnection:
    def __init__(self, autocommit=False, fail_on=None):
        self.autocommit = autocommit
        self.fail_on = fail_on
        self.executed = []
        self.commits = 0

    def cursor(self):
        return RecordingCursor(self, fail_on=self.fail_on)

    def commit(self):
        self.commits += 1


def test_runtime_settings_requires_every_key():
    with pytest.raises(ExecutionProfileInvalidError, match="Missing pipeline.runtime_settings"):
        RuntimeSettings.from_mapping({"dispatch_lease_timeout_sec": 1})


def test_runtime_settings_rejects_non_positive_values():
    with pytest.raises(ExecutionProfileInvalidError, match="must be greater than zero"):
        RuntimeSettings.from_mapping({
            "dispatch_lease_timeout_sec": 1,
            "worker_heartbeat_timeout_sec": 1,
            "publication_timeout_sec": 1,
            "target_lock_timeout_ms": 0,
        })


def test_apply_migration_respects_explicit_transaction_script():
    conn = RecordingConnection()

    _apply_migration(conn, "BEGIN;\nSELECT 1;\nCOMMIT;")

    assert conn.executed == [(True, "BEGIN;\nSELECT 1;\nCOMMIT;", None)]
    assert conn.commits == 0


def test_run_canonical_migrations_restores_autocommit_on_failure(tmp_path, monkeypatch):
    conn = RecordingConnection(autocommit=True)
    monkeypatch.setattr(
        "src.connections.schema_certifier.CANONICAL_MIGRATIONS",
        ["999_missing.sql"],
    )

    with pytest.raises(FileNotFoundError):
        run_canonical_migrations(conn=conn, migrations_dir=str(tmp_path))

    assert conn.autocommit is True
