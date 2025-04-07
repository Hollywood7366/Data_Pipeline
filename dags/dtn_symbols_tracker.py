import asyncio
from datetime import datetime

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.models import IqfeedSymbols
from src.pipelines.extras.base import BaseDB
from src.pipelines.google_sheet_pipeline import GoogleSheetSync
from utils.CONSTANTS import CREDENTIALS_PATH, SPREADSHEET_KEY
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
    dag_id="DTN_SYMBOLS_TRACKER_V1.0.3",
    default_args=default_args,
    description="Fetch all symbols from multiple tables and append to Google Sheet",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["google_sheets", "symbols", "db"],
)


def update_dropdown_symbols():
    async def run():
        models = [IqfeedSymbols]
        all_symbols = set()

        for model in models:
            db = BaseDB(model)
            results = await db.get_unique_column("symbol")
            all_symbols.update([r for r in results if r])

        if not all_symbols:
            logger.info("No symbols found across tables")
            return

        sheet = GoogleSheetSync(
            credentials_path=CREDENTIALS_PATH,
            spreadsheet_key=SPREADSHEET_KEY,
            worksheet_name=0,
            auto_save=False,
        )

        existing_values = sheet.worksheet.col_values(1)
        existing_symbols = set(existing_values)
        new_symbols = sorted(all_symbols - existing_symbols)

        if not new_symbols:
            logger.info("No new symbols to add.")
            return

        df = pl.DataFrame({"symbol": new_symbols})

        start_row = len(existing_symbols) + 1
        sheet.worksheet.update(f"A{start_row}", df.rows())

        logger.info(f"Added {len(new_symbols)} new symbols to the sheet.")

    asyncio.run(run())


update_symbols_task = PythonOperator(
    task_id="update_dropdown_symbols",
    python_callable=update_dropdown_symbols,
    dag=dag,
)
