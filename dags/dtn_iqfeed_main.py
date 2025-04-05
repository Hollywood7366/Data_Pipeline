import os
from datetime import datetime

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipelines.iqfeed import historical
from utils.CONSTANTS import SYMBOLS_RAW
from utils.emails import send_dag_failure_email, send_dag_success_email

default_args = {
    "owner": "Sarim Sikander",
    "start_date": datetime(2025, 4, 1),
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "email": "sarimsikander24@gmail.com",
    "on_failure_callback": send_dag_failure_email,
    "on_success_callback": send_dag_success_email,
}

dag = DAG(
    dag_id="HIST_META_SYMBOLS_V1.0.1",
    default_args=default_args,
    description="Fetch historical data from IQFeed and save as parquet files per symbol",
    schedule_interval="@daily",
    catchup=False,
)


def download_all_symbols():
    if not os.path.exists(SYMBOLS_RAW):
        raise FileNotFoundError("CSV not found")

    df = pl.read_csv(SYMBOLS_RAW, has_header=False, new_columns=["symbol"])
    if df.is_empty():
        return

    symbols = df["symbol"].to_list()

    data = historical(
        host="host.docker.internal",
        port=9100,
        start_date="20240101",
        end_date="20240201",
        interval="900",
        tickers=symbols,
    )


download_task = PythonOperator(
    task_id="download_historical_data",
    python_callable=download_all_symbols,
    dag=dag,
)
