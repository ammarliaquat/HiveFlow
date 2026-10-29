from datetime import datetime, timedelta
import logging

try:
    from airflow import DAG
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.utils.trigger_rule import TriggerRule
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = object
    TriggerDagRunOperator = None
    TriggerRule = None

log = logging.getLogger("airflow.task")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,           # Master DAG itself should NOT retry — individual DAGs handle their own retries
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="00_master_pipeline",
        default_args=default_args,
        description="Master Orchestrator: Runs all medallion pipelines sequentially",
        schedule_interval="0 20 * * *",   # 01:00 AM PKT — triggers the full nightly chain
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=12),  # Timeout requested by the user
        tags=["master", "medallion", "production"],
    ) as dag:

        def make_trigger(task_id, target_dag_id):
            return TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id=target_dag_id,
                wait_for_completion=True,
                poke_interval=60,
                reset_dag_run=False,           # P1-B Fix: never reset existing run_id state
                trigger_rule=TriggerRule.ALL_DONE if TriggerRule else "all_done",
            )

        t1 = make_trigger("trigger_01_opf_oracle_services",  "01_medallion_opf_oracle_services")
        t2 = make_trigger("trigger_02_opf_oracle_web",       "02_medallion_opf_oracle_web")
        t3 = make_trigger("trigger_03_school_system",        "03_medallion_school_system")
        t4 = make_trigger("trigger_04_msopforg_opforg",      "04_medallion_msopforg_opforg")
        t5 = make_trigger("trigger_05_source_erp_db",           "05_medallion_source_erp_db")
        t6 = make_trigger("trigger_06_msopforg_bureau",      "06_medallion_msopforg_bureau")
        t7 = make_trigger("trigger_07_opf_oracle_housing",   "07_medallion_opf_oracle_housing")

        t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
