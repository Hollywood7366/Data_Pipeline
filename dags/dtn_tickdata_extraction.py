import os
from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

from dags.short_circuits.apply import should_continue_data
from src.config.config import config as cn
from src.pipelines.iqfeed import get_tick_historical
from utils.CONSTANTS import SYMBOLS_COMPLETE
from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger

load_dotenv(dotenv_path="/opt/airflow/.env")

logger = Logger(name="iqfeed_tick", log_dir="data/logs")

default_args = {
    "owner": "Sarim Sikander",
    "start_date": datetime(2025, 4, 1),
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "email": "sarimsikander24@gmail.com",
    "on_failure_callback": send_dag_failure_email,
    "on_success_callback": send_dag_success_email,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id=f"HIST_TICK_DATA_{os.getenv('HIST_TICK_DATA','v1_0')}",
    default_args=default_args,
    description="Fetch historical tick data from IQFeed and save as parquet files per symbol",
    # schedule_interval="0 1 * * *",
    catchup=False,
    tags=[
        "iqfeed",
        "tick_data",
        "symbols",
        "db",
        f"pipeline_version:{os.getenv('PIPELINE_VERSION','v1_0')}",
    ],
)


def download_tick_data_custom():
    if not os.path.exists(SYMBOLS_COMPLETE):
        raise FileNotFoundError("parquet not found")

    df = pl.read_parquet(SYMBOLS_COMPLETE)
    if df.is_empty():
        return

    symbols = df["symbol"].to_list()

    data = get_tick_historical(
        host=cn.IQFEED_HOST,
        port=int(cn.IQFEED_PORT),
        start_date=Variable.get("TICK_START_DATE"),
        end_date=(
            Variable.get("TICK_END_DATE") if Variable.get("TICK_END_DATE") else None
        ),
        interval=Variable.get("TICK_INTERVAL"),
        symbols=symbols,
        records=df,
    )


download_task = PythonOperator(
    task_id="download_historical_tick_data",
    python_callable=download_tick_data_custom,
    dag=dag,
)

check_complete_task = ShortCircuitOperator(
    task_id="check_if_should_continue",
    python_callable=should_continue_data,
    provide_context=True,
    dag=dag,
)

trigger_self_task = TriggerDagRunOperator(
    task_id="trigger_self_if_not_complete",
    trigger_dag_id=f"HIST_TICK_DATA_{os.getenv('HIST_TICK_DATA','v1_0')}",
    wait_for_completion=False,
    reset_dag_run=False,
    dag=dag,
)

download_task >> check_complete_task >> trigger_self_task
