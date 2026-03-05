import json
import os
from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

from dags.short_circuits.apply import should_continue_data
from src.config.config import config as cn
from src.pipelines.iqfeed import historical
from utils.CONSTANTS import SYMBOLS_COMPLETE
from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger

load_dotenv(dotenv_path="/opt/airflow/src/.env")

logger = Logger(name="iqfeed", log_dir="data/logs")

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
    dag_id=f"HIST_META_SYMBOLS_{os.getenv('HIST_META_SYMBOLS','v1_2')}",
    default_args=default_args,
    description="Fetch historical data from IQFeed and save as parquet files per symbol",
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=[
        "iqfeed",
        "symbols",
        "db",
        f"pipeline_version:{os.getenv('PIPELINE_VERSION','v1_2')}",
    ],
)


def download_historical_data(interval, **kwargs):
    logger.info(f"Downloading historical data with interval {interval}")

    if not os.path.exists(SYMBOLS_COMPLETE):
        raise FileNotFoundError("parquet not found")

    df = pl.read_parquet(SYMBOLS_COMPLETE)
    if df.is_empty():
        return

    symbols = df["symbol"].to_list()

    data, ticker_start_date, ticker_end_date = historical(
        host=cn.IQFEED_HOST,
        port=int(cn.IQFEED_PORT),
        start_date=Variable.get("START_DATE"),
        end_date=(
            Variable.get("END_DATE") if Variable.get("END_DATE") else None
        ),
        interval=interval,
        tickers=symbols,
        records=df,
    )
    return {
        "current_start_date": ticker_start_date,
        "current_end_date": ticker_end_date,
        "interval": interval,
    }


def check_should_continue(interval, ti, **kwargs):
    task_id = f"download_historical_data_{interval}"
    logger.info(f"Checking if should continue for interval {interval}")

    task_result = ti.xcom_pull(task_ids=task_id)

    if task_result:
        task_result["interval"] = interval
        return should_continue_data(ti=ti, **task_result)
    return False


intervals_str = Variable.get("INTERVAL")
intervals = [interval.strip() for interval in intervals_str.split(",")]

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

for interval in intervals:
    download_task = PythonOperator(
        task_id=f"download_historical_data_{interval}",
        python_callable=download_historical_data,
        op_kwargs={"interval": interval},
        dag=dag,
    )

    check_task = ShortCircuitOperator(
        task_id=f"check_if_should_continue_{interval}",
        python_callable=check_should_continue,
        op_kwargs={"interval": interval},
        provide_context=True,
        dag=dag,
    )

    trigger_task = TriggerDagRunOperator(
        task_id=f"trigger_self_if_not_complete_{interval}",
        trigger_dag_id=f"HIST_META_SYMBOLS_{os.getenv('HIST_META_SYMBOLS','v1_2')}",
        wait_for_completion=False,
        reset_dag_run=False,
        dag=dag,
    )

    start >> download_task >> check_task >> trigger_task >> end
