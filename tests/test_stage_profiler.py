"""Unit tests for PERF-01 per-table telemetry envelopes."""

from unittest.mock import MagicMock, patch

from src.logging_.stage_profiler import StageProfiler
from src.constants import PipelinePhase


def test_complete_pipeline_records_total_measured_and_unaccounted_time():
    """The PERF-01 envelope makes framework time visible without another Spark action."""
    with patch("src.logging_.stage_profiler.time.perf_counter", side_effect=[100.0, 101.0, 102.0, 103.5]):
        profiler = StageProfiler(
            conn_manager=MagicMock(),
            run_id="run-1",
            source_tag="source",
            source_table="orders",
            layer="BRONZE",
        )
        profiler.start_stage("extract_sec")
        profiler.end_stage("extract_sec", row_count=5)
        envelope = profiler.complete_pipeline()

    assert envelope["schema_version"] == "perf-01.v1"
    assert envelope["total_sec"] == 3.5
    assert envelope["measured_stage_sec"] == 1.0
    assert envelope["unaccounted_overhead_sec"] == 2.5
    assert envelope["stages"]["extract_sec"]["rows"] == 5


def test_complete_pipeline_does_not_persist_without_a_batch_log_id():
    profiler = StageProfiler(
        conn_manager=MagicMock(),
        run_id="run-1",
        source_tag="source",
        source_table="orders",
        layer="SILVER",
    )

    envelope = profiler.complete_pipeline()

    assert envelope["telemetry_persistence"] == "not_requested"


def test_enum_lifecycle_phase_is_stored_as_a_json_safe_stage_name():
    profiler = StageProfiler(MagicMock(), "run-1", "source", "orders", "BRONZE")

    profiler.start_stage(PipelinePhase.EXTRACTING)
    profiler.end_stage(PipelinePhase.EXTRACTING, row_count=1)

    assert "EXTRACTING" in profiler.get_completed_stages()
