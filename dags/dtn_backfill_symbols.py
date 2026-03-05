import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

from dags.short_circuits.apply import should_continue_data
from src.pipelines.backfil import identify_and_backfill_missing_dates
from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger

load_dotenv(dotenv_path="/opt/airflow/src/.env")

logger = Logger(name="iqfeed_backfill", log_dir="data/logs")

default_args = {
    "owner": "Nick",
    "start_date": datetime(2025, 4, 1),
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "email": "hollywood7366@gmail.com",
    "on_failure_callback": send_dag_failure_email,
    "on_success_callback": send_dag_success_email,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id=f"HIST_BACKFILL_SYMBOLS_{os.getenv('HIST_BACKFILL_SYMBOLS','v1_0')}",
    default_args=default_args,
    description="Backfill missing historical data from IQFeed for symbols by security type",
    schedule_interval="0 12 * * *",
    catchup=False,
    tags=[
        "iqfeed",
        "symbols",
        "backfill",
        "db",
        f"pipeline_version:{os.getenv('PIPELINE_VERSION','v1_2')}",
    ],
)

security_types = ["FUTURE", "FOREX", "EQUITY", "FOPTION", "IEOPTION"]
security_type_tasks = {}

for sec_type in security_types:
    task = PythonOperator(
        task_id=f"backfill_missing_dates_{sec_type.lower()}",
        python_callable=identify_and_backfill_missing_dates,
        op_kwargs={"security_type": sec_type},
        dag=dag,
    )
    security_type_tasks[sec_type] = task

check_complete_task = ShortCircuitOperator(
    task_id="check_if_should_continue",
    python_callable=should_continue_data,
    provide_context=True,
    trigger_rule="all_done",
    dag=dag,
)

trigger_self_task = TriggerDagRunOperator(
    task_id="trigger_self_if_not_complete",
    trigger_dag_id=f"HIST_BACKFILL_SYMBOLS_{os.getenv('HIST_BACKFILL_SYMBOLS','v1_0')}",
    wait_for_completion=False,
    reset_dag_run=False,
    trigger_rule="all_done",
    dag=dag,
)

for sec_type, task in security_type_tasks.items():
    task >> check_complete_task

check_complete_task >> trigger_self_task
