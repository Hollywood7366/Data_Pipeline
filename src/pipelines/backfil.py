import os
from datetime import datetime, timedelta, date

import polars as pl
from airflow.models import Variable
from dotenv import load_dotenv

from src.pipelines.transformations.misc import ParquetDatabaseHandler
from src.config.config import config as cn
from src.pipelines.iqfeed import historical
from utils.CONSTANTS import SYMBOLS_COMPLETE
from utils.logging import Logger

load_dotenv(dotenv_path="/opt/airflow/.env")

logger = Logger(name="iqfeed_backfill", log_dir="data/logs")

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
        return CONSTANTS_GET_THIS_TYPE.get(security_type, False)
        
def is_symbol_already_processed(symbol, start_date, end_date):
    metadata_file = os.path.join(os.getenv("STORAGE_PATH", "data/market_data"), "backfill_metadata.txt")
    
    try:
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                content = f.read()
                entry = f"{symbol}:{start_date.strftime('%Y-%m-%d')}:{end_date.strftime('%Y-%m-%d')}"
                if entry in content:
                    return True
    except Exception as e:
        logger.error(f"Error checking if symbol {symbol} was processed: {e}")
    
    return False

def mark_symbol_as_processed(symbol, start_date, end_date):
    metadata_file = os.path.join(os.getenv("STORAGE_PATH", "data/market_data"), "backfill_metadata.txt")
    
    try:
        entry = f"{symbol}:{start_date.strftime('%Y-%m-%d')}:{end_date.strftime('%Y-%m-%d')}\n"
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
        
        with open(metadata_file, 'a') as f:
            f.write(entry)
    except Exception as e:
        logger.error(f"Error marking symbol {symbol} as processed: {e}")
        return False
    
    return True


def identify_and_backfill_missing_dates(security_type):
    if not should_run_security_type(security_type):
        logger.info(f"Skipping {security_type} as it's not enabled in GET_THIS_TYPE")
        return
    
    logger.info(f"Processing backfill for security_type: {security_type}")
    
    if not os.path.exists(SYMBOLS_COMPLETE):
        raise FileNotFoundError("Symbols parquet file not found")

    df = pl.read_parquet(SYMBOLS_COMPLETE)
    if df.is_empty():
        logger.warning("Symbols file is empty. No symbols to process.")
        return

    security_type_df = df.filter(pl.col("security_type") == security_type)
    
    if security_type_df.is_empty():
        logger.info(f"No symbols found for security_type: {security_type}")
        return
        
    logger.info(f"Found {security_type_df.height} symbols for security_type: {security_type}")

    storage_path = os.getenv("STORAGE_PATH", "data/market_data")
    db_handler = ParquetDatabaseHandler(storage_path)
    
    interval = Variable.get("INTERVAL")
    
    min_date = date(1950, 1, 1)
    max_date = datetime.now().date() - timedelta(days=1) 
    
    symbols_to_backfill = []
    missing_date_ranges = []
    
    for row in security_type_df.iter_rows(named=True):
        symbol = row["symbol"]
        exchange = row.get("exchange", "DEFAULT")
        
        try:
            try:
                symbol = symbol.replace('.','_')
                symbol_data = db_handler.load_data(exchange, security_type, interval, symbol)
                
                if "DateTime" in symbol_data.columns:
                    date_col = "DateTime"
                elif "datetime" in symbol_data.columns:
                    date_col = "datetime"
                else:
                    date_col = "date"
                
                if symbol_data.is_empty() or date_col not in symbol_data.columns:
                    symbols_to_backfill.append(row)
                    missing_date_ranges.append((min_date, max_date))
                    continue
                
                if symbol_data[date_col].dtype == pl.Datetime:
                    dates = symbol_data.select(
                        pl.col(date_col).dt.date().alias("date_only")
                    )["date_only"].to_list()
                else:
                    dates = symbol_data[date_col].to_list()
                
                dates = sorted(dates)
                
                if not dates or dates[0] > min_date or dates[-1] < max_date:
                    if not dates:
                        start_date = min_date
                        end_date = max_date
                    elif dates[0] > min_date and dates[-1] < max_date:
                        start_ranges = [(min_date, dates[0] - timedelta(days=1))]
                        end_ranges = [(dates[-1] + timedelta(days=1), max_date)]
                        
                        prev_date = dates[0]
                        middle_ranges = []
                        for current_date in dates[1:]:
                            if (current_date - prev_date).days > 1:
                                gap_start = prev_date + timedelta(days=1)
                                gap_end = current_date - timedelta(days=1)
                                middle_ranges.append((gap_start, gap_end))
                            prev_date = current_date
                        
                        all_ranges = start_ranges + middle_ranges + end_ranges
                        
                        for start_date, end_date in all_ranges:
                            symbols_to_backfill.append(row)
                            missing_date_ranges.append((start_date, end_date))
                        
                        continue
                    elif dates[0] > min_date:
                        start_date = min_date
                        end_date = dates[0] - timedelta(days=1)
                    else:
                        start_date = dates[-1] + timedelta(days=1)
                        end_date = max_date
                    
                    prev_date = None
                    for current_date in dates:
                        if prev_date and (current_date - prev_date).days > 1:
                            gap_start = prev_date + timedelta(days=1)
                            gap_end = current_date - timedelta(days=1)
                            symbols_to_backfill.append(row)
                            missing_date_ranges.append((gap_start, gap_end))
                        prev_date = current_date
                        
                    symbols_to_backfill.append(row)
                    missing_date_ranges.append((start_date, end_date))
                
            except FileNotFoundError:
                symbols_to_backfill.append(row)
                missing_date_ranges.append((min_date, max_date))
                
        except Exception as e:
            logger.error(f"Error processing symbol {symbol}: {e}")
            continue
    
    if not symbols_to_backfill:
        logger.info(f"No symbols need backfilling for {security_type}.")
        return
    
    logger.info(f"Found {len(symbols_to_backfill)} {security_type} symbol ranges to backfill.")
    records_df = pl.DataFrame(symbols_to_backfill)
    
    for i, (start_date, end_date) in enumerate(missing_date_ranges):
        symbol_row = symbols_to_backfill[i]
        symbol = symbol_row["symbol"]
        
        logger.info(f"Backfilling {security_type} symbol {symbol} from {start_date} to {end_date}")
        
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        try:
            single_symbol_df = pl.DataFrame([symbol_row])
            if is_symbol_already_processed(symbol, start_date, end_date):
                logger.info(f"Symbol {symbol} already processed for date range {start_date} to {end_date}, skipping...")
                continue
                
            historical(
                host=cn.IQFEED_HOST,
                port=int(cn.IQFEED_PORT),
                start_date=start_date_str,
                end_date=end_date_str,
                interval=interval,
                tickers=[symbol],
                records=single_symbol_df,
                backfill=True
            )
            
            logger.info(f"Successfully backfilled {security_type} symbol {symbol} for date range {start_date} to {end_date}")
            mark_symbol_as_processed(symbol, start_date, end_date)
            
        except Exception as e:
            logger.error(f"Failed to backfill {security_type} symbol {symbol} for date range {start_date} to {end_date}: {e}")
            continue
            
    logger.info(f"Completed backfill of {len(symbols_to_backfill)} {security_type} symbol ranges")
