"""Regression guard for the retired, pre-governed Airflow DAG."""
from pathlib import Path


def test_legacy_airflow_dag_is_inert_and_has_no_connection_fallbacks():
    source = Path("dags/generic_metadata_pipeline.py").read_text(encoding="utf-8")

    assert "psycopg2.connect" not in source
    assert "192.0.2.1" not in source
    assert "run_pipeline_job.py" not in source
    assert "GOVERNED_AIRFLOW_SCHEDULING_NOT_IMPLEMENTED" in source
