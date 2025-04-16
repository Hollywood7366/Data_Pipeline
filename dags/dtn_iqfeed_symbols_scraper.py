from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from src.pipelines.iqfeedscrape_refactored import STATE_FILE, inspect_state_file, reset_scraper_state, scrape_dtn_symbols

from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger
from dotenv import load_dotenv
load_dotenv(dotenv_path="/opt/airflow/.env") 

logger = Logger(name="dtn_iqfeed_scraper", log_dir="/opt/airflow/logs")

def should_continue(**kwargs):
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            return not state.get("complete", False)
        return False
    except Exception as e:
        logger.error(f"Error checking state file for loop logic: {e}")
        return False


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

with DAG(
    dag_id=f"DTN_IQFEED_BATCH_SCRAPER_{os.getenv('DTN_IQFEED_BATCH_SCRAPER','v1_2')}",
    default_args=default_args,
    description="Scrape symbols from DTN IQFeed using Selenium with batch processing",
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["scraping", "dtn", "iqfeed", "symbols", "batch",f"pipeline_version:{os.getenv('PIPELINE_VERSION','v1_2')}"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_dtn_symbols",
        python_callable=scrape_dtn_symbols,
        provide_context=True,
    )
    
    reset_state_task = PythonOperator(
        task_id="reset_scraper_state",
        python_callable=reset_scraper_state,
        provide_context=True,
        trigger_rule="all_done", 
    )
    
    inspect_state_task = PythonOperator(
        task_id="inspect_state_file",
        python_callable=inspect_state_file,
        provide_context=True,
    )

    check_complete_task = ShortCircuitOperator(
        task_id="check_if_should_continue",
        python_callable=should_continue,
        provide_context=True,
        trigger_rule="all_done",
    )

    trigger_self_task = TriggerDagRunOperator(
        task_id="trigger_self_if_not_complete",
        trigger_dag_id="DTN_IQFEED_BATCH_SCRAPER_V1.0.0",
        wait_for_completion=False,
        reset_dag_run=False,
        trigger_rule="all_done",
    )
    
    scrape_task >> inspect_state_task >> reset_state_task >> check_complete_task >> trigger_self_task