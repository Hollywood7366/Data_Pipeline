import asyncio
from datetime import datetime

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.models.dtn_iqfeed import IqfeedSymbols
from src.pipelines.extras.base import BaseDB
from src.pipelines.google_sheet_pipeline import GoogleSheetSync
from utils.CONSTANTS import (
    CREDENTIALS_PATH,
    SPREADSHEET_KEY,
    SYMBOLS_COMPLETE,
    SYMBOLS_RAW,
)
from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger

logger = Logger(name="iqfeed", log_dir="data/logs")

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
    dag_id="DTN_SYMBOLS_HOURLY_TRACKER_V1.0.1",
    default_args=default_args,
    description="Fetch IQFeed symbols from Google Sheet and update CSV hourly",
    schedule_interval="@hourly",
    catchup=False,
    tags=["iqfeed", "google_sheets", "csv"],
)


async def fetch_symbols_from_sheet():
    sheet_sync = GoogleSheetSync(
        credentials_path=CREDENTIALS_PATH,
        spreadsheet_key=SPREADSHEET_KEY,
        worksheet_name=1,
        filename="selectedsymbols.csv",
        data_folder="data/GOOGLE_TO_LOCAL",
        auto_save=True,
    )
    sheet_sync.update_dataframe()

    df = pl.read_csv(SYMBOLS_RAW, has_header=False)
    symbols = df["column_1"].to_list()

    if not symbols:
        logger.info("No symbols found in sheet.")

    return symbols


async def fetch_and_save_db_records(symbols):
    db = BaseDB(IqfeedSymbols)
    records = []
    missing_symbols = []

    for symbol in symbols:
        results = await db.get_by_column("symbol", symbol)

        if not results:
            missing_symbols.append(symbol)
            continue

        for r in results:
            records.append(
                {
                    "symbol": r.symbol,
                    "description": r.description,
                    "security_type": r.security_type,
                    "exchange": r.exchange,
                    "listed_market": r.listed_market,
                    "created_at": r.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    if not records:
        logger.info("No matching records found in DB.")
        return

    full_df = pl.DataFrame(records)
    full_df.write_csv(SYMBOLS_COMPLETE)

    logger.info(f"Saved {len(records)} full records to {SYMBOLS_COMPLETE}")


def sync_google_sheet_to_csv():
    async def run():
        symbols = await fetch_symbols_from_sheet()
        if symbols:
            await fetch_and_save_db_records(symbols)

    asyncio.run(run())


update_task = PythonOperator(
    task_id="sync_iqfeed_symbols_to_csv",
    python_callable=sync_google_sheet_to_csv,
    dag=dag,
)
