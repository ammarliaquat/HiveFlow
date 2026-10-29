"""Static regression checks for the governed partial-success lifecycle contract."""

from pathlib import Path

from src.connections.schema_certifier import CANONICAL_MIGRATIONS
from src.orchestrator.lifecycle import TERMINAL_STATES, validate_transition_allowed


def test_partial_success_is_a_canonical_terminal_registry_migration():
    migration_name = "046_partial_success_job_lifecycle.sql"
    migration_path = Path("migrations") / migration_name

    assert migration_name in CANONICAL_MIGRATIONS
    assert CANONICAL_MIGRATIONS.index(migration_name) < CANONICAL_MIGRATIONS.index(
        "047_layer_artifact_manifest.sql"
    )
    assert migration_path.is_file()
    assert "'PARTIAL_SUCCESS'" in migration_path.read_text(encoding="utf-8")


def test_partial_success_is_reachable_only_from_running_and_terminal_afterward():
    validate_transition_allowed("RUNNING", "PARTIAL_SUCCESS")
    assert "PARTIAL_SUCCESS" in TERMINAL_STATES
    try:
        validate_transition_allowed("PARTIAL_SUCCESS", "RUNNING")
    except ValueError:
        pass
    else:
        raise AssertionError("PARTIAL_SUCCESS must be terminal")
