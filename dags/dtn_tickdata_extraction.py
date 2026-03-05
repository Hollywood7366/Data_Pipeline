import os
from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from src.pipelines.tick_data_pipeline import download_ticks
from utils.CONSTANTS import SYMBOLS_COMPLETE
from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger

logger = Logger(name="iqfeed", log_dir="data/logs")

default_args = {
    "owner": "Nick",
    "start_date": datetime(2025, 4, 1),
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "email": "hollywood7366@gmail.com",
    # "on_failure_callback": send_dag_failure_email,
    # "on_success_callback": send_dag_success_email,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id=f"TICK_META_SYMBOLS_{os.getenv('TICK_META_SYMBOLS','v1_2')}",
    default_args=default_args,
    description="Fetch historical data from IQFeed and save as parquet files per symbol",
    # schedule_interval="0 0 * * *",
    catchup=False,
    tags=[
        "iqfeed",
        "symbols",
        "db",
        f"pipeline_version:{os.getenv('PIPELINE_VERSION','v1_2')}",
    ],
)


class Config:
    def __init__(self):
        self.protocol = "5.1"
        self.command = "tick"
        self.start_date = ""
        self.end_date = ""
        self.out_directory = "data"
        self.time_zone = "ET"
        self.interval_type = ""
        self.interval_length = 0
        self.parallelism = 8
        self.detailed_logging = False
        self.end_timestamp = False
        self.use_labels = False


def download_all_symbols():
    if not os.path.exists(SYMBOLS_COMPLETE):
        raise FileNotFoundError("parquet not found")

    df = pl.read_parquet(SYMBOLS_COMPLETE)
    if df.is_empty():
        return

    symbols = df["symbol"].to_list()

    start_date = Variable.get("TICK_START_DATE")
    end_date = Variable.get("TICK_END_DATE")
    out_directory = "data"
    time_zone = "ET"
    detailed_logging = False

    config = Config()
    config.start_date = start_date
    config.end_date = end_date
    config.out_directory = out_directory
    config.time_zone = time_zone
    config.detailed_logging = detailed_logging

    for symbol in symbols:
        download_ticks(symbol, config, records=df)


download_task = PythonOperator(
    task_id="download_tick_data",
    python_callable=download_all_symbols,
    dag=dag,
)

# download_task
