# dags/05_medallion_source_erp_db.py
"""
Governed Medallion Pipeline DAG for Source: source_erp_db (MySQL ERP Database).

Orchestrates the four discrete stages of data movement:
  Stage 1: Preflight Validation (Metadata catalog & ERP connection health check)
  Stage 2: Bronze Layer (Raw ERP Extraction to Staging Parquet)
  Stage 3: Silver Layer (Parquet Partitioning, Deduplication & History)
  Stage 4: Gold Layer (PostgreSQL Target UPSERT with Change Detection)

Schedule:
  Target Local Time: Daily at 04:30 AM PKT (UTC+5)
  UTC Math: 04:30 PKT - 5 hours = 23:30 UTC (11:30 PM UTC previous calendar day)
  Cron: "30 23 * * *"

Optional run parameters via Airflow UI 'Trigger DAG w/ config':
  {
    "table": "",
    "priority": "",
    "target_tag": ""
  }
"""
from datetime import datetime, timedelta
import logging

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = object
    BashOperator = None

log = logging.getLogger("airflow.task")

SOURCE_TAG = "source_erp_db"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=3),
}

DEFAULT_PARAMS = {
    "table": "",
    "priority": "",
    "target_tag": "",
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="05_medallion_source_erp_db",
        default_args=default_args,
        description=f"Governed Medallion Pipeline for {SOURCE_TAG} (Preflight -> Bronze -> Silver -> Gold UPSERT)",
        schedule_interval=None,  # Triggered exclusively by 00_master_pipeline
        catchup=False,
        max_active_runs=1,
        params=DEFAULT_PARAMS,
        tags=["source_erp_db", "mysql", "medallion", "production"],
    ) as dag:

        # ── Stage 1: Preflight Validation ────────────────────────────────────
        preflight_task = BashOperator(
            task_id="01_preflight_validation",
            bash_command=f"""
                cd /opt/spark/jobs/pyspark_pipeline && \\
                python jobs/app_etl.py \\
                    --source_tag {SOURCE_TAG} \\
                    --validate-only
            """,
        )

        # ── Stage 2: Medallion Pipeline (Bronze -> Silver -> Gold UPSERT) ─────
        pipeline_task = BashOperator(
            task_id="02_medallion_pipeline",
            bash_command=f"""
                cd /opt/spark/jobs/pyspark_pipeline && \\
                python jobs/app_etl.py \\
                    --source_tag {SOURCE_TAG} \\
                    --action run_pipeline \\
                    {{% if params.table %}}--table {{{{ params.table }}}} {{% endif %}} \\
                    {{% if params.priority %}}--priority {{{{ params.priority }}}} {{% endif %}} \\
                    {{% if params.target_tag %}}--target_tag {{{{ params.target_tag }}}} {{% endif %}}
            """,
        )

        # Sequential dependency enforcement:
        preflight_task >> pipeline_task
