import os
from datetime import datetime, timedelta, date

import polars as pl
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

from src.pipelines.transformations.misc import ParquetDatabaseHandler
from dags.short_circuits.apply import should_continue_data
from src.config.config import config as cn
from src.pipelines.iqfeed import historical
from utils.CONSTANTS import SYMBOLS_COMPLETE
from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger

# Load environment variables
load_dotenv(dotenv_path="/opt/airflow/.env")

logger = Logger(name="iqfeed_backfill", log_dir="data/logs")

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

# Create DAG
dag = DAG(
    dag_id=f"HIST_BACKFILL_SYMBOLS_{os.getenv('HIST_BACKFILL_SYMBOLS','v1_0')}",
    default_args=default_args,
    description="Backfill missing historical data from IQFeed for symbols by security type",
    schedule_interval="0 12 * * *",  # Run at noon daily (separate from main DAG)
    catchup=False,
    tags=[
        "iqfeed",
        "symbols",
        "backfill",
        "db",
        f"pipeline_version:{os.getenv('PIPELINE_VERSION','v1_2')}",
    ],
)


def should_run_security_type(security_type):
    import os
    from utils.CONSTANTS import GET_THIS_TYPE as CONSTANTS_GET_THIS_TYPE
    
    env_setting = os.getenv(f"GET_THIS_TYPE_{security_type}", "").lower()
    if env_setting in ("true", "1", "yes"):
        return True
    elif env_setting in ("false", "0", "no"):
        return False
    
    try:
        return CONSTANTS_GET_THIS_TYPE.get(security_type, False)
    except (AttributeError, ImportError):
        # Finally fall back to the dict defined in this file
        return CONSTANTS_GET_THIS_TYPE.get(security_type, False)


def identify_and_backfill_missing_dates(security_type):
    """
    Identify symbols with missing dates for a specific security type and backfill them
    """
    # Check if this security type should be processed
    if not should_run_security_type(security_type):
        logger.info(f"Skipping {security_type} as it's not enabled in GET_THIS_TYPE")
        return
    
    logger.info(f"Processing backfill for security_type: {security_type}")
    
    if not os.path.exists(SYMBOLS_COMPLETE):
        raise FileNotFoundError("Symbols parquet file not found")

    # Read symbols from parquet file
    df = pl.read_parquet(SYMBOLS_COMPLETE)
    if df.is_empty():
        logger.warning("Symbols file is empty. No symbols to process.")
        return

    # Filter symbols by security_type
    security_type_df = df.filter(pl.col("security_type") == security_type)
    
    if security_type_df.is_empty():
        logger.info(f"No symbols found for security_type: {security_type}")
        return
        
    logger.info(f"Found {security_type_df.height} symbols for security_type: {security_type}")

    # Initialize ParquetDatabaseHandler with storage path
    storage_path = os.getenv("STORAGE_PATH", "data/market_data")
    db_handler = ParquetDatabaseHandler(storage_path)
    
    # Get interval from airflow variable
    interval = Variable.get("INTERVAL")
    
    # Set the global date range that all symbols should have
    min_date = date(1950, 1, 1)
    max_date = datetime.now().date() - timedelta(days=1)  # Yesterday
    
    # Get batch size from environment or default to processing 10 symbols per DAG run
    batch_size = int(os.getenv(f"BACKFILL_BATCH_SIZE_{security_type}", 
                              os.getenv("BACKFILL_BATCH_SIZE", "10")))
    
    # Initialize list to track symbols that need backfilling
    symbols_to_backfill = []
    missing_date_ranges = []
    
    # Limit processed symbols for this run based on batch size
    processed_symbols = 0
    
    # Check each symbol for missing dates
    for row in security_type_df.iter_rows(named=True):
        if processed_symbols >= batch_size:
            break
            
        symbol = row["symbol"]
        exchange = row.get("exchange", "DEFAULT")
        
        try:
            # Try to load the parquet file for this symbol
            try:
                symbol = symbol.replace('.','_')
                symbol_data = db_handler.load_data(exchange, security_type, interval, symbol)
                
                # Get the date column (might be 'date' or 'datetime')
                date_col = "datetime" if "datetime" in symbol_data.columns else "date"
                
                # Check if data exists and has dates
                if symbol_data.is_empty() or date_col not in symbol_data.columns:
                    # Empty or malformed data, needs complete backfill
                    symbols_to_backfill.append(row)
                    missing_date_ranges.append((min_date, max_date))
                    processed_symbols += 1
                    continue
                
                # Convert dates to python date objects if they're datetime
                if symbol_data[date_col].dtype == pl.Datetime:
                    # Extract dates from datetime column
                    dates = symbol_data.select(
                        pl.col(date_col).dt.date().alias("date_only")
                    )["date_only"].to_list()
                else:
                    dates = symbol_data[date_col].to_list()
                
                # Sort dates
                dates = sorted(dates)
                
                # Check if we have complete data from min_date to max_date
                if not dates or dates[0] > min_date or dates[-1] < max_date:
                    # Find missing ranges
                    if not dates:
                        # No data at all
                        start_date = min_date
                        end_date = max_date
                    elif dates[0] > min_date and dates[-1] < max_date:
                        # Missing data at both ends
                        start_ranges = [(min_date, dates[0] - timedelta(days=1))]
                        end_ranges = [(dates[-1] + timedelta(days=1), max_date)]
                        
                        # Also check for gaps in the middle
                        prev_date = dates[0]
                        middle_ranges = []
                        for current_date in dates[1:]:
                            if (current_date - prev_date).days > 1:
                                # Gap found
                                gap_start = prev_date + timedelta(days=1)
                                gap_end = current_date - timedelta(days=1)
                                middle_ranges.append((gap_start, gap_end))
                            prev_date = current_date
                        
                        # Combine all ranges
                        all_ranges = start_ranges + middle_ranges + end_ranges
                        
                        # Add each range to the backfill list
                        for start_date, end_date in all_ranges:
                            symbols_to_backfill.append(row)
                            missing_date_ranges.append((start_date, end_date))
                        
                        processed_symbols += 1
                        continue
                    elif dates[0] > min_date:
                        # Missing beginning data
                        start_date = min_date
                        end_date = dates[0] - timedelta(days=1)
                    else:
                        # Missing end data
                        start_date = dates[-1] + timedelta(days=1)
                        end_date = max_date
                    
                    # Check for gaps in the existing data
                    prev_date = None
                    for current_date in dates:
                        if prev_date and (current_date - prev_date).days > 1:
                            # Gap found
                            gap_start = prev_date + timedelta(days=1)
                            gap_end = current_date - timedelta(days=1)
                            symbols_to_backfill.append(row)
                            missing_date_ranges.append((gap_start, gap_end))
                        prev_date = current_date
                        
                    # Add the main missing range
                    symbols_to_backfill.append(row)
                    missing_date_ranges.append((start_date, end_date))
                    processed_symbols += 1
                
            except FileNotFoundError:
                # Symbol file doesn't exist, needs complete backfill
                symbols_to_backfill.append(row)
                missing_date_ranges.append((min_date, max_date))
                processed_symbols += 1
                
        except Exception as e:
            logger.error(f"Error processing symbol {symbol}: {e}")
            continue
    
    # If no symbols need backfilling, we're done
    if not symbols_to_backfill:
        logger.info(f"No symbols need backfilling for {security_type} in this batch.")
        return
    
    logger.info(f"Found {len(symbols_to_backfill)} {security_type} symbol ranges to backfill in this batch.")
    
    # Create a records dataframe with the symbols to backfill
    records_df = pl.DataFrame(symbols_to_backfill)
    
    # Process each date range separately
    for i, (start_date, end_date) in enumerate(missing_date_ranges):
        symbol_row = symbols_to_backfill[i]
        symbol = symbol_row["symbol"]
        
        logger.info(f"Backfilling {security_type} symbol {symbol} from {start_date} to {end_date}")
        
        # Convert dates to string format
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        try:
            # Create single-row dataframe for this symbol
            single_symbol_df = pl.DataFrame([symbol_row])
            
            # Fetch historical data for this symbol and date range
            historical(
                host=cn.IQFEED_HOST,
                port=int(cn.IQFEED_PORT),
                start_date=start_date_str,
                end_date=end_date_str,
                interval=interval,
                tickers=[symbol],
                records=single_symbol_df,
            )
            
            logger.info(f"Successfully backfilled {security_type} symbol {symbol} for date range {start_date} to {end_date}")
            
        except Exception as e:
            logger.error(f"Failed to backfill {security_type} symbol {symbol} for date range {start_date} to {end_date}: {e}")
            continue
            
    logger.info(f"Completed backfill batch of {len(symbols_to_backfill)} {security_type} symbol ranges")
    
    # Store batch tracking information 
    Variable.set(
        f"BACKFILL_LAST_PROCESSED_{security_type}_SYMBOL", 
        str(processed_symbols)
    )


# Create tasks for each security type
security_types = ['FUTURE', 'FOREX', 'EQUITY', 'FOPTION', 'IEOPTION']
security_type_tasks = {}

for sec_type in security_types:
    task = PythonOperator(
        task_id=f"backfill_missing_dates_{sec_type.lower()}",
        python_callable=identify_and_backfill_missing_dates,
        op_kwargs={"security_type": sec_type},
        dag=dag,
    )
    security_type_tasks[sec_type] = task

# Create a task that checks if DAG execution should continue
check_complete_task = ShortCircuitOperator(
    task_id="check_if_should_continue",
    python_callable=should_continue_data,
    provide_context=True,
    trigger_rule="all_done",
    dag=dag,
)

trigger_self_task = TriggerDagRunOperator(
    task_id="trigger_self_if_not_complete",
    trigger_dag_id=f"HIST_BACKFILL_SYMBOLS_{os.getenv('HIST_BACKFILL_SYMBOLS','v1_0')}",
    wait_for_completion=False,
    reset_dag_run=False,
    trigger_rule="all_done",
    dag=dag,
)

for sec_type, task in security_type_tasks.items():
    task >> check_complete_task

check_complete_task >> trigger_self_task