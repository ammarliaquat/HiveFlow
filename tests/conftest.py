# tests/conftest.py
"""
Offline-first test configuration.

Default run (no --live flag): offline guard active.
  - psycopg2.connect and sqlalchemy.create_engine are patched to raise
    OfflineTestViolation if called from any non-live test.
  - SPARK_CONNECT_URL is stripped so pyspark cannot silently enable
    external access.
  - No .env file is loaded.
  - The Spark session fixture is not instantiated.

Live run (--live flag):
  - .env is loaded per-test for @pytest.mark.live tests.
  - Real connections are permitted only inside @pytest.mark.live tests.
  - Session-scoped Spark fixture is available.

Patch timing:
  pytest_configure() runs before any fixture or collection, ensuring the
  offline guard is in place before session-scoped fixtures fire.
"""
from __future__ import annotations

import os
import sys
import unittest.mock as mock

import pytest

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


class OfflineTestViolation(RuntimeError):
    """Raised when a non-live test attempts a real DB or Spark connection."""


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--live",
        action="store_true",
        default=False,
        help="Enable live DB/Spark tests decorated with @pytest.mark.live",
    )


def pytest_configure(config: pytest.Config) -> None:
    """
    Called before fixture collection and session-scoped fixture instantiation.
    Installs connection-blocking patches for the offline run mode.
    """
    config.addinivalue_line("markers", "live: requires real DB/Spark infrastructure")
    config.addinivalue_line("markers", "migration: requires DISPOSABLE_PG_URL")
    config.addinivalue_line("markers", "integration: requires real service endpoints")
    config.addinivalue_line("markers", "spark: requires a Spark session")

    is_live = config.getoption("--live", default=False)
    if not is_live:
        # Strip SPARK_CONNECT_URL to prevent silent external Spark access
        os.environ.pop("SPARK_CONNECT_URL", None)

        config._offline_patches = [
            mock.patch(
                "psycopg2.connect",
                side_effect=OfflineTestViolation(
                    "psycopg2.connect called in an offline test. "
                    "Decorate with @pytest.mark.live and pass --live to enable."
                ),
            ),
            mock.patch(
                "sqlalchemy.create_engine",
                side_effect=OfflineTestViolation(
                    "sqlalchemy.create_engine called in an offline test. "
                    "Decorate with @pytest.mark.live and pass --live to enable."
                ),
            ),
        ]
        for p in config._offline_patches:
            p.start()


def pytest_unconfigure(config: pytest.Config) -> None:
    """Stop all offline patches at session teardown."""
    for p in getattr(config, "_offline_patches", []):
        p.stop()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark live-gate modules by filename so offline collection skips them early."""
    is_live_session = config.getoption("--live", default=False)
    live_skip = pytest.mark.skip(reason="Live test skipped in offline mode -- pass --live to enable")
    for item in items:
        filename = os.path.basename(str(item.fspath)).lower()
        is_live_item = (
            item.get_closest_marker("live") is not None
            or "_live" in filename
            or "live_gate" in filename
        )
        if is_live_item:
            item.add_marker(pytest.mark.live)
            if not is_live_session:
                item.add_marker(live_skip)


@pytest.fixture(autouse=True)
def _live_guard(request: pytest.FixtureRequest) -> None:
    """
    Per-test autouse guard.

    Offline session + live-marked test: skip with a clear message so the
    test does not reach the patched psycopg2/SQLAlchemy path and raise an
    opaque OfflineTestViolation.

    Live session + live-marked test: load .env so real credentials are
    available for that test.
    """
    is_live_session = request.config.getoption("--live", default=False)
    is_live_test = request.node.get_closest_marker("live") is not None

    if is_live_test and not is_live_session:
        pytest.skip("Live test skipped in offline mode — pass --live to enable")

    if is_live_session and is_live_test:
        try:
            from dotenv import load_dotenv  # type: ignore[import]
            load_dotenv(os.path.join(project_root, ".env"))
        except ImportError:
            pass
    yield


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest):
    """
    Session-scoped Spark fixture.

    Only instantiated when the session is started with --live.  Spark is
    a live infrastructure resource; it is not available in offline mode.
    """
    is_live_session = request.config.getoption("--live", default=False)
    if not is_live_session:
        pytest.skip("Spark fixture requires --live")

    from pyspark.sql import SparkSession  # type: ignore[import]

    try:
        from dotenv import load_dotenv  # type: ignore[import]
        load_dotenv(os.path.join(project_root, ".env"))
    except ImportError:
        pass

    connect_url = os.getenv("SPARK_CONNECT_URL")
    builder = SparkSession.builder.appName("ETL_UnitTests")
    if connect_url:
        builder = builder.remote(connect_url)
    else:
        builder = (
            builder.master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
        )
    session = builder.getOrCreate()
    yield session
    session.stop()
