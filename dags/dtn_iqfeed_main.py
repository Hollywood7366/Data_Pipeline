import os
from datetime import datetime, timedelta

from src.pipelines.extras.asyncer import run_async_task
from src.pipelines.extras.extraction_manager import ExtractionManager
import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from src.config.config import config as cn
from src.pipelines.iqfeed import historical
from utils.CONSTANTS import SYMBOLS_COMPLETE
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
    dag_id=f"HIST_META_SYMBOLS_{os.getenv('HIST_META_SYMBOLS')}",
    default_args=default_args,
    description="Fetch historical data from IQFeed and save as parquet files per symbol",
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["iqfeed", "symbols", "db", f"pipeline_version:{os.getenv('PIPELINE_VERSION')}"],
)

def should_continue(**context):
    try:
        if not os.path.exists(SYMBOLS_COMPLETE):
            return True

        df = pl.read_parquet(SYMBOLS_COMPLETE)
        if df.is_empty():
            return False 
            
        all_symbols = df["symbol"].to_list()
        
        extraction_manager = ExtractionManager()
        start_date = Variable.get('START_DATE')
        end_date = Variable.get('END_DATE') if Variable.get('END_DATE') else (
            datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        interval = Variable.get('INTERVAL')
        
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        successful_extractions = run_async_task(extraction_manager.get_successful_extractions())
        
        relevant_extractions = [
            e for e in successful_extractions 
            if (e.start_date.date() == start_dt.date() and 
                e.end_date.date() == end_dt.date() and
                e.interval == interval)
        ]
        
        processed_symbols = [e.ticker for e in relevant_extractions]
        pending_symbols = [sym for sym in all_symbols if sym not in processed_symbols]
        should_continue = len(pending_symbols) > 0
        
        if should_continue:
            logger.info(f"DAG will continue: {len(pending_symbols)} symbols still need processing")
        else:
            logger.info("DAG completed: All symbols processed successfully")
            
        return should_continue
    except Exception as e:
        logger.error(f"Error in should_continue: {e}")
        return True 


def download_all_symbols():
    if not os.path.exists(SYMBOLS_COMPLETE):
        raise FileNotFoundError("parquet not found")

    df = pl.read_parquet(SYMBOLS_COMPLETE)
    if df.is_empty():
        return

    symbols = df["symbol"].to_list()

    data = historical(
        host=cn.IQFEED_HOST,
        port=int(cn.IQFEED_PORT),
        start_date=Variable.get('START_DATE'),
        end_date=Variable.get('END_DATE') if Variable.get('END_DATE') else None,
        interval=Variable.get('INTERVAL'),
        tickers=symbols,
        records=df,
    )


download_task = PythonOperator(
    task_id="download_historical_data",
    python_callable=download_all_symbols,
    dag=dag,
)

check_complete_task = ShortCircuitOperator(
    task_id="check_if_should_continue",
    python_callable=should_continue,
    provide_context=True,
    trigger_rule="all_done",
    dag=dag,
)

trigger_self_task = TriggerDagRunOperator(
    task_id="trigger_self_if_not_complete",
    trigger_dag_id="HIST_META_SYMBOLS_V1.1.0", 
    wait_for_completion=False,
    reset_dag_run=False,
    trigger_rule="all_done",
    dag=dag,
)

download_task >> check_complete_task >> trigger_self_task