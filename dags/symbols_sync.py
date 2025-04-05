import os
from datetime import datetime

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator

CSV_PATH = "data/iqfeed/dropdownoptions.csv"
PARQUET_DIR = "data/historical_symbols"

dag = DAG(
    dag_id="SYMBOLS_SYNC_V1.0.1",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2025, 4, 1),
    },
    description="Sync .parquet files with the current list of symbols in CSV",
    schedule_interval="@hourly",
    catchup=False,
)


def sync_symbols():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError("CSV not found")

    df = pl.read_csv(CSV_PATH, has_header=False, new_columns=["symbol"])
    current_symbols = set(df["symbol"].to_list()) if not df.is_empty() else set()

    if not os.path.exists(PARQUET_DIR):
        os.makedirs(PARQUET_DIR)

    existing_files = set(
        f.replace(".parquet", "")
        for f in os.listdir(PARQUET_DIR)
        if f.endswith(".parquet")
    )

    symbols_to_remove = existing_files - current_symbols
    for symbol in symbols_to_remove:
        file_path = os.path.join(PARQUET_DIR, f"{symbol}.parquet")
        os.remove(file_path)


sync_task = PythonOperator(
    task_id="sync_parquet_files",
    python_callable=sync_symbols,
    dag=dag,
)
