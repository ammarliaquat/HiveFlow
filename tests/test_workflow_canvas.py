"""Read-only workflow-canvas projections for governed layer telemetry."""

from gui.components.workflow_canvas import _merge_statuses, _node_markup, _rows_for_stage, _stage_statuses
from gui.models.run import RunModel, RunStageModel
from gui.services.execution_service import _load_persisted_layer_stages


def test_canvas_separates_bronze_silver_and_gold_persisted_state():
    run = RunModel(
        run_id="run-1",
        project_id="source_a",
        dataset_id="all_datasets",
        status="PARTIAL_SUCCESS",
        extracted_count=100,
        rows_written=99,
        stages=[
            RunStageModel("Extract", status="SUCCESS", rows_processed=100),
            RunStageModel("Landing", status="SUCCESS", rows_processed=100),
            RunStageModel("History", status="SUCCESS", rows_processed=99),
            RunStageModel("Target", status="FAILED", rows_processed=0),
        ],
    )

    statuses = _stage_statuses(run)

    assert statuses["bronze"] == "SUCCESS"
    assert statuses["silver"] == "SUCCESS"
    assert statuses["gold"] == "FAILED"
    assert statuses["monitor"] == "PARTIAL_SUCCESS"
    assert statuses["review"] == "PARTIAL_SUCCESS"
    assert _rows_for_stage(run, "bronze") == 100
    assert _rows_for_stage(run, "silver") == 99
    assert _rows_for_stage(run, "gold") == 0


def test_canvas_never_reports_layer_success_when_only_extract_completed():
    run = RunModel(
        run_id="run-2",
        project_id="source_a",
        dataset_id="orders",
        status="RUNNING",
        stages=[
            RunStageModel("Extract", status="SUCCESS", rows_processed=100),
            RunStageModel("Landing", status="PENDING", rows_processed=0),
        ],
    )

    statuses = _stage_statuses(run)

    assert statuses["bronze"] == "RUNNING"
    assert statuses["silver"] == "WAITING"
    assert statuses["gold"] == "WAITING"


def test_canvas_preserves_not_requested_layers_for_scoped_success():
    run = RunModel(
        run_id="run-scoped",
        project_id="source_a",
        dataset_id="orders",
        status="SUCCESS",
        stages=[
            RunStageModel("Bronze", status="SUCCESS", rows_processed=100),
            RunStageModel("Silver", status="NOT_REQUESTED", rows_processed=0),
            RunStageModel("Gold", status="NOT_REQUESTED", rows_processed=0),
        ],
    )

    statuses = _stage_statuses(run)

    assert statuses["bronze"] == "SUCCESS"
    assert statuses["silver"] == "NOT_REQUESTED"
    assert statuses["gold"] == "NOT_REQUESTED"


def test_merge_statuses_reports_not_requested_only_when_no_layer_work_ran():
    assert _merge_statuses(["NOT_REQUESTED", "NOT_REQUESTED"]) == "NOT_REQUESTED"
    assert _merge_statuses(["SUCCESS", "NOT_REQUESTED"]) == "SUCCESS"


def test_canvas_displays_em_dash_for_not_requested_layer_rows():
    run = RunModel(
        run_id="run-scoped",
        project_id="source_a",
        dataset_id="orders",
        status="SUCCESS",
        stages=[
            RunStageModel("Bronze", status="SUCCESS", rows_processed=100),
            RunStageModel("Silver", status="NOT_REQUESTED"),
            RunStageModel("Gold", status="NOT_REQUESTED"),
        ],
    )

    markup = _node_markup(run, _stage_statuses(run), None)

    assert "NOT REQUESTED &middot; &mdash;" in markup


def test_persisted_layer_rollup_drives_distinct_canvas_stages():
    class Result:
        def mappings(self):
            return self

        def fetchall(self):
            return [
                {"layer_name": "bronze", "layer_status": "SUCCESS", "rows_processed": 100, "throughput_rows_sec": 10.0},
                {"layer_name": "silver", "layer_status": "SUCCESS", "rows_processed": 99, "throughput_rows_sec": 8.0},
                {"layer_name": "gold", "layer_status": "RUNNING", "rows_processed": 50, "throughput_rows_sec": 2.0},
            ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement, params):
            assert params["rid"] == "run-3"
            return Result()

    class Engine:
        def connect(self):
            return Connection()

    stages = _load_persisted_layer_stages(Engine(), "run-3")

    assert [(stage.stage_name, stage.status, stage.rows_processed) for stage in stages] == [
        ("Bronze", "SUCCESS", 100),
        ("Silver", "SUCCESS", 99),
        ("Gold", "RUNNING", 50),
    ]
