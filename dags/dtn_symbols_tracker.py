import asyncio
from datetime import datetime, timedelta
import os

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from src.models import *
from src.pipelines.extras.base import BaseDB
from src.pipelines.google_sheet_pipeline import GoogleSheetSync
from utils.CONSTANTS import CREDENTIALS_PATH
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
    dag_id=f"DTN_SYMBOLS_TRACKER_{os.getenv("DTN_SYMBOLS_TRACKER")}",
    default_args=default_args,
    description="Fetch all symbols from multiple tables and append to Google Sheet",
    schedule_interval="*/30 23 * * *" ,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["google_sheets", "symbols", "db", f"pipeline_version:{os.getenv("PIPELINE_VERSION")}"],
)


def update_dropdown_symbols():
    async def run():
        models = [
            IqfeedSymbolsAll,
            IqfeedSymbolsContinuousContracts,
            IqfeedSymbolsEminis,
            IqfeedSymbolsFrontMonth,
            IqfeedSymbolsNoOptions,
            IqfeedSymbolsNoSpreads,
        ]
        all_symbols = set()

        for model in models:
            db = BaseDB(model)
            results = await db.get_unique_column("symbol")
            all_symbols.update([r for r in results if r])

        if not all_symbols:
            logger.info("No symbols found across tables")
            return
        
        logger.info(f"Found {len(all_symbols)} unique symbols across all tables")

        sheet = GoogleSheetSync(
            credentials_path=CREDENTIALS_PATH,
            spreadsheet_key=Variable.get('SPREADSHEET_KEY'),
            worksheet_name=0,
            auto_save=False,
        )
        
        try:
            worksheets = [ws for ws in sheet.sheet.worksheets() if ws.title.startswith("Symbols_Batch_")]
            
            if not worksheets:
                logger.info("No Symbols_Batch sheets found, creating first batch sheet")
                new_ws = sheet.sheet.add_worksheet(title="Symbols_Batch_0", rows=100010, cols=1)
                worksheets = [new_ws]
            
            symbols_list = sorted(list(all_symbols))
            batch_size = 100000
            symbol_batches = [symbols_list[i:i+batch_size] for i in range(0, len(symbols_list), batch_size)]
            
            total_added = 0
            for i, batch in enumerate(symbol_batches):
                if i < len(worksheets):
                    ws = worksheets[i]
                    ws.clear() 
                    symbols_2d = [[s] for s in batch]
                    ws.update("A1", symbols_2d)
                    total_added += len(batch)
                else:
                    new_ws = sheet.sheet.add_worksheet(title=f"Symbols_Batch_{i}", rows=len(batch)+10, cols=1)
                    symbols_2d = [[s] for s in batch]
                    new_ws.update("A1", symbols_2d)
                    total_added += len(batch)
            
            logger.info(f"Total symbols added: {total_added}")
            
        except Exception as e:
            logger.error(f"Error updating symbols in sheets: {e}")

    asyncio.run(run())


update_symbols_task = PythonOperator(
    task_id="update_dropdown_symbols",
    python_callable=update_dropdown_symbols,
    dag=dag,
)
