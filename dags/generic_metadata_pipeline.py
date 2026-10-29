# dags/generic_metadata_pipeline.py
"""Retired pre-governed Airflow DAG.

Daily Airflow scheduling has not yet been implemented for governed runs.  Keep
this DAG inert so an Airflow deployment cannot query metadata at parse time or
dispatch the retired non-governed worker.  Operators use the governed UI path
until the governed Airflow dispatcher is explicitly introduced.
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

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="generic_metadata_pipeline",
        default_args=default_args,
        description="Inert placeholder until governed Airflow scheduling is implemented",
        schedule_interval="0 0 * * *",
        catchup=False,
        max_active_runs=1,
    ) as dag:

        BashOperator(
            task_id="governed_scheduling_not_implemented",
            bash_command="echo 'GOVERNED_AIRFLOW_SCHEDULING_NOT_IMPLEMENTED'",
        )
