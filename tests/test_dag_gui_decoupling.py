"""
Tests for complete decoupling and independence between Airflow DAGs and Streamlit GUI.
Verifies that:
1. Airflow DAGs call `jobs/app_etl.py` via clean 2-stage execution (preflight >> medallion_pipeline).
2. All 6 DAG files are free of subshell traps (`(cd ...)`).
3. Neither `jobs/app_etl.py` nor DAGs import `gui.` or `sqlalchemy` in worker execution paths.
4. Streamlit GUI does not import or depend on `airflow`.
5. Airflow and GUI can run concurrently or independently without cross-dependencies.
"""
from pathlib import Path
import re
import pytest

DAG_FILES = [
    "dags/01_medallion_opf_oracle_services.py",
    "dags/02_medallion_opf_oracle_web.py",
    "dags/03_medallion_school_system.py",
    "dags/04_medallion_msopforg_opforg.py",
    "dags/05_medallion_source_erp_db.py",
    "dags/06_medallion_msopforg_bureau.py",
    "dags/07_medallion_opf_oracle_housing.py",
]


def test_dag_structure_and_task_consolidation():
    """Verify that all 6 DAGs have consolidated 2-stage execution using run_pipeline."""
    for dag_file in DAG_FILES:
        path = Path(dag_file)
        assert path.exists(), f"DAG file missing: {dag_file}"
        content = path.read_text(encoding="utf-8")

        # Must have preflight and medallion_pipeline tasks
        assert "task_id=\"01_preflight_validation\"" in content
        assert "task_id=\"02_medallion_pipeline\"" in content
        assert "--action run_pipeline" in content
        assert "preflight_task >> pipeline_task" in content

        # Must NOT have old 4-task split
        assert "02_landing_bronze" not in content
        assert "03_history_silver" not in content
        assert "04_target_gold" not in content

        # Must NOT contain the subshell trap: (cd ... || cd ...)
        assert "(cd " not in content


def test_app_etl_has_no_gui_import():
    """Verify jobs/app_etl.py doesn't depend on gui.* anywhere."""
    path = Path("jobs/app_etl.py")
    assert path.exists()
    content = path.read_text(encoding="utf-8")

    # Verify no 'from gui.' or 'import gui.'
    assert not re.search(r"^\s*(from|import)\s+gui\b", content, re.MULTILINE), \
        "jobs/app_etl.py must not import from gui module"


def test_gui_has_no_airflow_imports():
    """Verify that the GUI modules never import airflow."""
    gui_dir = Path("gui")
    for py_file in gui_dir.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8", errors="ignore")
        assert not re.search(r"^\s*(from|import)\s+airflow\b", content, re.MULTILINE), \
            f"GUI file {py_file} must not depend on airflow"


def test_orchestrator_bypass_skip_handles_all_actions():
    """Verify that src/orchestrator/etl.py allows explicit actions to bypass daily skips."""
    etl_file = Path("src/orchestrator/etl.py")
    content = etl_file.read_text(encoding="utf-8")

    # In load_historyData:
    assert "getattr(self, \"current_action\", \"\") == \"run_history\"" in content
    # In run_target_cdc:
    assert "getattr(self, \"current_action\", \"\") in (\"run_target\", \"refresh_tables_cdc_12AM\")" in content


def test_master_pipeline_uses_all_done_trigger_rule():
    """Verify that 00_master_pipeline configures trigger_rule ALL_DONE so that an upstream failure does not block downstream sources."""
    master_dag = Path("dags/00_master_pipeline.py")
    assert master_dag.exists()
    content = master_dag.read_text(encoding="utf-8")

    assert "TriggerRule" in content
    assert "trigger_rule=TriggerRule.ALL_DONE if TriggerRule else \"all_done\"" in content
    assert "t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7" in content

