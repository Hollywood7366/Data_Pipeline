from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import polars as pl

from src.pipelines.extras.base import BaseDB
from src.models import IqfeedSymbols
from src.pipelines.google_sheet_pipeline import GoogleSheetSync
from utils.CONSTANTS import CREDENTIALS_PATH, SPREADSHEET_KEY
from utils.logging import Logger

logger = Logger(name='iqfeed', log_dir='data/logs')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='update_google_sheet_symbols',
    default_args=default_args,
    description='Fetch all symbols from multiple tables and append to Google Sheet',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['google_sheets', 'symbols', 'db'],
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

        df = pl.DataFrame({"symbol": sorted(all_symbols)})

        sheet = GoogleSheetSync(
            credentials_path=CREDENTIALS_PATH,
            spreadsheet_key=SPREADSHEET_KEY,
            worksheet_name=0,
            auto_save=False
        )

        existing_rows = sheet.worksheet.get_all_values()
        start_row = len(existing_rows) + 1

        sheet.worksheet.update(f"A{start_row}", df.rows())

    asyncio.run(run())

update_symbols_task = PythonOperator(
    task_id='update_dropdown_symbols',
    python_callable=update_dropdown_symbols,
    dag=dag,
)
