import os
from datetime import datetime, timedelta
from glob import glob

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

from utils.emails import send_dag_failure_email
from utils.logging import Logger
from utils.util import base_path

load_dotenv(dotenv_path="/opt/airflow/.env")
logger = Logger(
    name="dtn_iqfeed_deduplication", log_dir="/opt/airflow/logs"
)

default_args = {
    "owner": "Sarim Sikander",
    "start_date": datetime(2025, 4, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "sarimsikander24@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_dag_failure_email,
}

dag = DAG(
    dag_id=f"DEDUPLICATION_PARQUET_FILES_{os.getenv('DEDUPLICATION_PARQUET_FILES','v1_2')}",
    default_args=default_args,
    description="Scan storage directory for parquet files and remove duplicate rows based on DateTime",
    schedule_interval="0 0,18 * * *",
    catchup=False,
    tags=[
        "parquet",
        "deduplication",
        "data_cleaning",
        f"pipeline_version:{os.getenv('PIPELINE_VERSION','v1_2')}",
    ],
)


def find_parquet_files(storage_dir):
    parquet_files = glob(
        os.path.join(storage_dir, "**", "*.parquet"), recursive=True
    )
    return parquet_files


def deduplicate_parquet_file(file_path):
    try:
        df = pl.read_parquet(file_path)

        if df.is_empty() or "DateTime" not in df.columns:
            logger.info(
                f"Skipping {file_path}: Empty or missing DateTime column"
            )
            return {
                "file": file_path,
                "status": "skipped",
                "reason": "Empty or missing DateTime column",
                "original_rows": df.height if not df.is_empty() else 0,
                "new_rows": df.height if not df.is_empty() else 0,
            }

        original_row_count = df.height
        df_deduplicated = df.unique(subset=["DateTime"])
        new_row_count = df_deduplicated.height

        if new_row_count < original_row_count:
            df_deduplicated.write_parquet(file_path)
            logger.info(
                f"Deduplicated {file_path}: Removed {original_row_count - new_row_count} duplicate rows"
            )
        else:
            logger.info(f"No duplicates found in {file_path}")

        return {
            "file": file_path,
            "status": "processed",
            "original_rows": original_row_count,
            "new_rows": new_row_count,
            "duplicates_removed": original_row_count - new_row_count,
        }
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return {
            "file": file_path,
            "status": "error",
            "error": str(e),
        }


def process_all_parquet_files(**kwargs):
    storage_dir = f"{base_path()}/storage"

    parquet_files = find_parquet_files(storage_dir)
    logger.info(f"Found {len(parquet_files)} parquet files to process")

    results = []
    for file_path in parquet_files:
        result = deduplicate_parquet_file(file_path)
        results.append(result)

    processed = [r for r in results if r["status"] == "processed"]
    skipped = [r for r in results if r["status"] == "skipped"]
    errors = [r for r in results if r["status"] == "error"]

    total_duplicates_removed = sum(
        r.get("duplicates_removed", 0) for r in processed
    )

    logger.info(f"Deduplication Summary:")
    logger.info(f"- Total files: {len(results)}")
    logger.info(f"- Files processed: {len(processed)}")
    logger.info(f"- Files skipped: {len(skipped)}")
    logger.info(f"- Files with errors: {len(errors)}")
    logger.info(
        f"- Total duplicate rows removed: {total_duplicates_removed}"
    )

    return {
        "total_files": len(results),
        "processed_files": len(processed),
        "skipped_files": len(skipped),
        "error_files": len(errors),
        "total_duplicates_removed": total_duplicates_removed,
    }


process_parquet_files_task = PythonOperator(
    task_id="process_parquet_files",
    python_callable=process_all_parquet_files,
    provide_context=True,
    dag=dag,
)
