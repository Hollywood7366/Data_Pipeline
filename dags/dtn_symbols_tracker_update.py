import asyncio
from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from src.models import *
from src.pipelines.extras.base import BaseDB
from src.pipelines.google_sheet_pipeline import GoogleSheetSync
from src.pipelines.transformations.parquet_misc import (
    parquet_columns_naming,
)
from utils.atomic_creator import AtomicFileUpdate
from utils.CONSTANTS import (
    CREDENTIALS_PATH,
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
    "retries": 3,  
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="DTN_SYMBOLS_HOURLY_TRACKER_V1.1.0",
    default_args=default_args,
    description="Fetch IQFeed symbols from Google Sheet and update parquet hourly",
    schedule_interval="0 */3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iqfeed", "google_sheets", "parquet"],
)


async def fetch_symbols_from_sheet():
    sheet_sync = GoogleSheetSync(
        credentials_path=CREDENTIALS_PATH,
        spreadsheet_key=Variable.get('SPREADSHEET_KEY'),
        worksheet_name=1,
        filename="selectedsymbols.parquet",
        data_folder="data/GOOGLE_TO_LOCAL",
        auto_save=True,
    )
    success = sheet_sync.update_dataframe()

    if success:
        df = pl.read_parquet(SYMBOLS_RAW)
        result_df = parquet_columns_naming(df)
        symbols = result_df["column_0"].to_list()

        if not symbols:
            logger.info("No symbols found in sheet.")

        return symbols


async def fetch_and_save_db_records(symbols):
    db = BaseDB(IqfeedSymbolsAll)
    records = []
    missing_symbols = []
    processed_symbols = set()

    models = [
        IqfeedSymbolsFrontMonth,
        IqfeedSymbolsContinuousContracts,
        IqfeedSymbolsEminis,
        IqfeedSymbolsNoOptions,
        IqfeedSymbolsNoSpreads,
        IqfeedSymbolsAll,
    ]

    for model in models:
        db.model = model

        for symbol in symbols:
            if symbol in processed_symbols:
                continue

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
                        "created_at": r.created_at.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                )

            processed_symbols.add(symbol)

    if not records:
        logger.info("No matching records found in DB.")
        return

    full_df = pl.DataFrame(records)
    atomic_update = AtomicFileUpdate(
        SYMBOLS_COMPLETE, f"{SYMBOLS_COMPLETE}.tmp"
    )
    atomic_update.perform_atomic_update(full_df)

    logger.info(f"Saved {len(records)} full records to {SYMBOLS_COMPLETE}")


def sync_google_sheet_to_parquet():
    async def run():
        symbols = await fetch_symbols_from_sheet()
        if symbols:
            await fetch_and_save_db_records(symbols)

    asyncio.run(run())


update_task = PythonOperator(
    task_id="sync_iqfeed_symbols_to_parquet",
    python_callable=sync_google_sheet_to_parquet,
    dag=dag,
)
