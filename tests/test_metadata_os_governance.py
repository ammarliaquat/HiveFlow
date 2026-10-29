# tests/test_metadata_os_governance.py
"""
Tests for Metadata OS:
  - Discovery & Recommendation Engine (`_recommend_table_strategy`, `_upsert_recommendation`)
  - Governance Queue & Steward Review (`approve_recommendation`, `override_recommendation`)
  - Deterministic Promotion (`_promote_one_table` preservation of APPROVED/OVERRIDDEN status)
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pandas as pd

from gui.models.run import RunModel
from gui.services.execution_service import ExecutionService
from jobs.discover_schema import _recommend_table_strategy
from gui.services.metadata_service import approve_recommendation, override_recommendation


def test_recommend_table_strategy_timestamp_unindexed():
    """Verify strategy recommendation gives +30 points for timestamp and -15 for unindexed high row count."""
    cols = [
        {"column_name": "customer_uuid", "data_type": "VARCHAR2", "is_nullable": "N"},
        {"column_name": "registration_date", "data_type": "TIMESTAMP", "is_nullable": "Y"}
    ]
    indexed = set()
    rec = _recommend_table_strategy("tbl_large", "BASE TABLE", cols, indexed, row_count=5_000_000)
    
    assert rec["recommendation_type"] == "CDC_STRATEGY"
    assert rec["recommended_value"] == "registration_date"
    assert rec["recommended_type"] == "TIMESTAMP_HIGH_WATERMARK"
    assert rec["performance_risk"] == "HIGH"
    assert "timestamp_datatype" in rec["confidence_breakdown"]
    assert rec["confidence_breakdown"]["unindexed_watermark_risk"] == -15


def test_recommend_table_strategy_reference_small():
    """Verify small lookup tables get NONE (FULL_SNAPSHOT) strategy recommendation with low risk."""
    cols = [
        {"column_name": "status_code", "data_type": "VARCHAR2", "is_nullable": "N"},
        {"column_name": "description", "data_type": "VARCHAR2", "is_nullable": "Y"}
    ]
    rec = _recommend_table_strategy("ref_statuses", "BASE TABLE", cols, set(), row_count=150)
    assert rec["recommended_type"] == "NONE"
    assert "FULL_SNAPSHOT" in rec["recommended_value"]
    assert rec["performance_risk"] == "LOW"
    assert rec["confidence_breakdown"].get("table_classification") == "REFERENCE"


@patch("gui.services.metadata_service.get_engine")
@patch("gui.services.metadata_service.get_table_recommendation")
def test_approve_recommendation_updates_etl_and_rec(mock_get_rec, mock_get_engine):
    """Verify approve_recommendation sets status to APPROVED and writes exact strategy to db_tables_etl."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    
    mock_conn.execute.return_value.mappings.return_value.fetchone.return_value = {
        "source_tag": "test_tag", "source_table": "test_table"
    }
    mock_get_rec.return_value = {
        "recommendation_id": 99,
        "recommended_value": "updated_at",
        "recommended_type": "TIMESTAMP_HIGH_WATERMARK"
    }

    res = approve_recommendation(table_id=123, steward_user="steward_jane")
    assert res is True
    
    # Check that UPDATE queries executed for both table_recommendations and db_tables_etl
    assert mock_conn.execute.call_count == 3


@patch("gui.services.metadata_service.get_engine")
def test_override_recommendation_applies_custom_strategy(mock_get_engine):
    """Verify override_recommendation writes OVERRIDDEN status and custom strategy to db_tables_etl."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    
    mock_conn.execute.return_value.mappings.return_value.fetchone.return_value = {
        "source_tag": "test_tag", "source_table": "test_table", "table_master_key": 456
    }

    res = override_recommendation(table_id=123, custom_strategy="SEQUENCE_HIGH_WATERMARK", custom_column="user_id", steward_user="steward_bob")
    assert res is True
    assert mock_conn.execute.call_count >= 2


def test_governed_log_formatter_sanitizes_stream_output():
    """Verify GovernedLogFormatter automatically redacts sensitive keywords from log output stream."""
    import logging
    import io
    from src.security.data_governance import DataGovernanceService, GovernedLogFormatter

    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(GovernedLogFormatter("%(levelname)s: %(message)s"))

    test_logger = logging.getLogger("test_governance_logger")
    test_logger.setLevel(logging.INFO)
    test_logger.addHandler(handler)

    # 1. Log sensitive password and email
    test_logger.info("Connection failed for user test@example.com with password: mysecretpassword123")
    
    # 2. Log sensitive cnic and passport
    test_logger.error("Failed table check for Passport=AB1234567 and CNIC=12345-6789012-3")

    output = log_stream.getvalue()

    assert "<EMAIL_REDACTED>" in output
    assert "mysecretpassword123" not in output
    assert "<REDACTED>" in output
    assert "AB1234567" not in output


def test_activate_flag_disabled_per_adr003():
    """Verify action_sync exits with error when activate=True to enforce human steward promotion per ADR-003."""
    from jobs.app_etl import action_sync
    mock_etl = MagicMock()
    with pytest.raises(SystemExit) as exc_info:
        action_sync(mock_etl, source_tag="mysql_membership", dry_run=False, activate=True, deactivate_missing=False)
    assert exc_info.value.code == 1


def test_sync_allows_source_only_discovery_without_target_tag(monkeypatch):
    """Tier-1 discovery must not require or guess a promotion target."""
    from jobs.app_etl import action_sync

    class FakeCrawler:
        stats = {
            "tables_discovered": 0,
            "tables_inserted": 0,
            "columns_discovered": 0,
            "columns_inserted": 0,
            "tables_deactivated": 0,
            "columns_deactivated": 0,
        }

        def __init__(self, **kwargs):
            assert kwargs["target_tag"] is None

        def crawl(self, **kwargs):
            return None

    monkeypatch.setattr("jobs.discover_and_promote.DiscoveryCrawler", FakeCrawler)
    action_sync(
        MagicMock(),
        source_tag="mysql_membership",
        dry_run=True,
        activate=False,
        deactivate_missing=False,
        target_tag=None,
    )


def test_gui_column_promotion_uses_table_id_authority():
    """Column mappings must follow MAP-01 identity authority via table_id."""
    page = (Path(__file__).resolve().parents[1] / "gui" / "pages" / "04_Metadata_Discovery.py").read_text(encoding="utf-8")

    assert "INSERT INTO metadata.db_columns_etl" not in page
    assert "PromotionService.promote_catalog_datasets" in page


def test_execution_grid_uses_only_tier_two_execution_metadata():
    """The visible execution grid must not mix catalog or recommendation records."""
    root = Path(__file__).resolve().parents[1]
    page = (root / "gui" / "pages" / "04_Metadata_Discovery.py").read_text(encoding="utf-8")
    service = (root / "gui" / "services" / "metadata_service.py").read_text(encoding="utf-8")

    assert "get_governance_queue" not in page
    assert "get_pipeline_tables" in page
    assert 'execution_cols = ["source_table", "target_tag", "execution_authority_mode", "is_active", "is_cdc", "cdc_strategy", "columns_unique_key"]' in page
    assert "t.cdc_strategy" in service
    assert "t.columns_unique_key" in service


def test_metadata_workflow_has_only_real_operational_steps():
    """Do not retain an empty configuration tab in the onboarding workflow."""
    page = (Path(__file__).resolve().parents[1] / "gui" / "pages" / "04_Metadata_Discovery.py").read_text(encoding="utf-8")

    assert "tab_discover, tab_review, tab_promote = st.tabs([" in page
    assert "Configure Execution" not in page
    assert "Step 4 Execution Audit Logs" not in page


def test_global_app_shell_auto_refreshes_while_any_run_is_active():
    """Every page must refresh from the one app-shell live polling loop."""
    page = (Path(__file__).resolve().parents[1] / "gui" / "pages" / "04_Metadata_Discovery.py").read_text(encoding="utf-8")
    app = (Path(__file__).resolve().parents[1] / "gui" / "app.py").read_text(encoding="utf-8")

    assert "from streamlit_autorefresh import st_autorefresh" in app
    assert "ExecutionService.has_active_runs(ctx)" in app
    assert 'key="global_live_data_refresh"' in app
    assert "md_crawl_live_refresh" not in page
    assert "ExecutionService.get_run_status" in page
    assert "Crawler status:" in page
    assert "Crawler failed." in page
    assert "Crawler completed successfully in" in page


def test_page_headers_do_not_mutate_run_lifecycle_during_global_refresh():
    """Page rendering must remain read-only while the app shell refreshes live data."""
    header = (Path(__file__).resolve().parents[1] / "gui" / "components" / "page_header.py").read_text(encoding="utf-8")

    assert "check_and_recover_orphaned_runs" not in header


@patch("gui.services.execution_service.get_engine")
def test_active_run_probe_is_scoped_and_read_only(mock_get_engine):
    """The global refresh gate must not invoke reaping or disclose other projects."""
    mock_conn = MagicMock()
    mock_get_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.scalar.return_value = 1
    context = MagicMock(project_id="source_a", is_all_projects=False)

    assert ExecutionService.has_active_runs(context) is True

    query, params = mock_conn.execute.call_args.args
    assert "LOWER(source_tag) = LOWER(:source_tag)" in str(query)
    assert params == {"source_tag": "source_a"}


@patch.object(ExecutionService, "get_run_detail")
def test_active_run_status_calculates_live_elapsed_time(mock_get_run_detail):
    """Active jobs must report elapsed time even before a worker persists a final value."""
    mock_get_run_detail.return_value = RunModel(
        run_id="run-1",
        status="RUNNING",
        started_at=datetime.now() - timedelta(seconds=30),
        elapsed_seconds=0.0,
    )

    status = ExecutionService.get_run_status("run-1")

    assert 29 <= status["elapsed_seconds"] <= 32


@patch.object(ExecutionService, "get_run_detail")
def test_completed_run_status_derives_elapsed_time_from_persisted_boundaries(mock_get_run_detail):
    """Completed crawler banners must not display zero when batch telemetry is absent."""
    started = datetime(2026, 9, 14, 10, 0, 0)
    mock_get_run_detail.return_value = RunModel(
        run_id="run-completed",
        status="SUCCESS",
        started_at=started,
        ended_at=started + timedelta(seconds=173),
        elapsed_seconds=0.0,
    )

    status = ExecutionService.get_run_status("run-completed")

    assert status["elapsed_seconds"] == 173.0


@patch.object(ExecutionService, "get_run_detail")
def test_run_status_includes_the_persisted_failure_detail(mock_get_run_detail):
    mock_get_run_detail.return_value = RunModel(
        run_id="run-failed",
        status="FAILED",
        error_message="Target connection timed out.",
    )

    status = ExecutionService.get_run_status("run-failed")

    assert status["error_message"] == "Target connection timed out."


def test_metadata_page_keeps_target_selection_only_in_step_three():
    """Discovery is source-only; promotion is the sole target-selection step."""
    root = Path(__file__).resolve().parents[1]
    page = (root / "gui" / "pages" / "04_Metadata_Discovery.py").read_text(encoding="utf-8")
    admin = (root / "gui" / "pages" / "02_Admin.py").read_text(encoding="utf-8")
    step_one = page.split("with tab_review:", 1)[0]

    assert 'key="md_crawl_src"' in page
    assert 'key="md_crawl_target_display"' not in step_one
    assert "metadata_route_target" not in step_one
    assert "--target_tag" not in step_one
    assert 'key="md_promo_target"' in page
    assert "get_target_tags()" in page
    assert 'key="admin_metadata_target"' not in admin
    assert "metadata_route_target" not in admin
    assert "Step 7.1.1 - Select Source" in admin
    assert "Step 7.1.5 - Promote Datasets" not in admin
    assert "disabled=crawler_active" in step_one


def test_metadata_page_refreshes_policies_when_all_datasets_are_promoted():
    """Full dataset remains actionable for target-scoped policy reconciliation."""
    root = Path(__file__).resolve().parents[1]
    page = (root / "gui" / "pages" / "04_Metadata_Discovery.py").read_text(encoding="utf-8")

    assert "refresh_existing=promotion_scope == \"Full dataset\"" in page
    assert "Refresh Full Dataset Policies" in page
    assert "independent " in page
    assert "Tier-2 subscriptions for target" in page
    assert "Existing target subscriptions are unchanged." in page
    assert 'all_execution_df["target_tag"].fillna("").str.lower() == str(promo_target).lower()' in page
    assert "execution_view = all_execution_df" in page
    assert "No discovered datasets are available for this source." in page


def test_metadata_promotion_offers_full_or_selected_dataset_scope():
    """Step 3 must support one-click promotion of all unpromoted tables."""
    root = Path(__file__).resolve().parents[1]
    page = (root / "gui" / "pages" / "04_Metadata_Discovery.py").read_text(encoding="utf-8")

    assert '["Full dataset", "Selected datasets"]' in page
    assert 'if promotion_scope == "Full dataset":' in page
    assert "selected_tables = unpromoted_tables" in page


def test_promotion_column_upsert_matches_active_case_insensitive_index():
    """The promotion upsert must match the live expression/partial unique index."""
    root = Path(__file__).resolve().parents[1]
    source = (root / "jobs" / "promote_table.py").read_text(encoding="utf-8")

    assert "ON CONFLICT (table_id, LOWER(source_column))" in source
    assert "WHERE (is_active = 'y')" in source
    assert "ON CONFLICT (table_id, source_column)" not in source


def test_promotion_service_keeps_savepoint_transaction_open():
    """The service owns commit/release so helper calls must support deferred commit."""
    root = Path(__file__).resolve().parents[1]
    helper = (root / "jobs" / "promote_table.py").read_text(encoding="utf-8")
    service = (root / "gui" / "services" / "promotion_service.py").read_text(encoding="utf-8")

    assert "commit: bool = True" in helper
    assert "if commit:" in helper
    assert "commit=False" in service


@patch("gui.services.metadata_service.pd.read_sql")
@patch("gui.services.metadata_service.get_engine")
def test_governance_queue_normalizes_union_confidence_reasons(mock_get_engine, mock_read_sql):
    """Both governance queue branches must expose confidence reasons as text."""
    from gui.services.metadata_service import get_governance_queue

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    mock_read_sql.return_value = pd.DataFrame()
    get_governance_queue.clear()

    get_governance_queue(status_filter="(all)")

    query = str(mock_read_sql.call_args.args[0])
    assert "CAST(r.confidence_reasons AS TEXT) AS confidence_reasons" in query
    assert "CAST(c.cdc_health_details AS TEXT) AS confidence_reasons" in query




