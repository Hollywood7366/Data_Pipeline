from datetime import datetime, timedelta
import asyncio
import pandas as pd
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipelines.iqfeed import historical
from src.pipelines.extras.base import QuestDBOperations
from schemas.historical import HISTORICAL_DATA_SCHEMA
from config import config
from utils.logging import Logger

logger = Logger(name='iqfeed', log_dir='data/logs')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 10),
}

dag = DAG(
    'iqfeed_historical_data_pipelinev1.7',
    default_args=default_args,
    description='Pipeline to fetch historical market data from IQFeed and store in QuestDB',
    schedule_interval='0 0 * * *',
    catchup=False,
)

def download_historical_data(**kwargs):
    tickers = config['data']['tickers']
    interval = config['data']['interval']
    
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=config['data']['days_look_back'])).strftime('%Y%m%d')
    
    data = historical(
        host=config['iqfeed']['host'],
        port=config['iqfeed']['port'],
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        tickers=tickers
    )
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'tickers': tickers,
        'interval': interval
    }


def process_and_store_data(**kwargs):
    ti = kwargs['ti']
    download_info = ti.xcom_pull(task_ids='download_historical_data')
    
    tickers = download_info['tickers']
    start_date = download_info['start_date']
    end_date = download_info['end_date']
    interval = download_info['interval']
    
    asyncio.run(store_in_questdb(tickers, start_date, end_date, interval))
    
    return f"Successfully processed and stored data for {len(tickers)} tickers"

async def store_in_questdb(tickers: List[str], start_date: str, end_date: str, interval: str):
    table_name = 'historical_market_data_v1'
    
    db_ops = QuestDBOperations(
        table_name=table_name,
        schema=HISTORICAL_DATA_SCHEMA,
        create_if_not_exists=True,
        **config['questdb']
    )
    
    await db_ops.initialize()
    await db_ops.truncate_table()
    
    try:
        for ticker in tickers:
            file_path = f"data/{ticker}_{start_date}_{end_date}_{interval}.csv"
            
            try:
                df = pd.read_csv(file_path)
                df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
                valid_rows = df[~df['DateTime'].isna()]
                
                if len(valid_rows) < len(df):
                    logger.info(f"Filtered out {len(df) - len(valid_rows)} rows with invalid datetime values from {ticker}")
                
                records = []
                for _, row in valid_rows.iterrows():
                    record = {
                        'symbol': ticker,
                        'datetime': row['DateTime'],
                        'high': float(row['High']),
                        'low': float(row['Low']),
                        'open': float(row['Open']),
                        'close': float(row['Close']),
                        'total_volume': int(row['TotalVolume']),
                        'period_volume': int(row['PeriodVolume']),
                        'created_at': datetime.now()
                    }
                    for key, value in record.items():
                        if pd.isna(value): 
                            record[key] = None 
                    records.append(record)
                
                if records:
                    batch_size = 500
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i+batch_size]
                        try:
                            await db_ops.bulk_create(batch)
                            logger.info(f"Inserted batch {i//batch_size + 1} with {len(batch)} records for {ticker}")
                        except Exception as batch_error:
                            logger.error(f"Error inserting batch {i//batch_size + 1} for {ticker}: {batch_error}")
                            success_count = 0
                            for record in batch:
                                try:
                                    await db_ops.bulk_create([record])
                                    success_count += 1
                                except Exception as single_error:
                                    logger.error(f"Error inserting single record for {ticker}: {single_error}")
                            logger.info(f"Inserted {success_count} out of {len(batch)} records individually for {ticker}")
                else:
                    logger.info(f"No valid records to insert for {ticker}")
                    
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue
    finally:
        await db_ops.cleanup()

download_task = PythonOperator(
    task_id='download_historical_data',
    python_callable=download_historical_data,
    provide_context=True,
    dag=dag,
)

store_task = PythonOperator(
    task_id='process_and_store_data',
    python_callable=process_and_store_data,
    provide_context=True,
    dag=dag,
)

download_task >> store_task