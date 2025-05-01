import json
import os
from datetime import datetime, timedelta

import polars as pl
from airflow.models import Variable

from src.pipelines.extras.asyncer import run_async_task
from src.pipelines.extras.extraction_manager import ExtractionManager
from src.pipelines.iqfeedscrape_refactored import STATE_FILE
from utils.CONSTANTS import SYMBOLS_COMPLETE
from utils.logging import Logger

logger = Logger(name="iqfeed", log_dir="data/logs")


def should_continue_data(**context):
    try:
        if not os.path.exists(SYMBOLS_COMPLETE):
            logger.info("SYMBOLS_COMPLETE file not found, returning True to continue DAG")
            return True

        df = pl.read_parquet(SYMBOLS_COMPLETE)
        if df.is_empty():
            logger.info("SYMBOLS_COMPLETE file is empty, returning False to stop DAG")
            return False

        all_symbols = df["symbol"].to_list()

        extraction_manager = ExtractionManager()
        start_date = (
            (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")
            if Variable.get("START_DATE") == "CURRENT"
            else Variable.get("START_DATE")
        )
        end_date = (
            Variable.get("END_DATE")
            if Variable.get("END_DATE")
            else (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        )
        interval = Variable.get("INTERVAL")

        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")

        successful_extractions = run_async_task(
            extraction_manager.get_successful_extractions()
        )

        relevant_extractions = [
            e
            for e in successful_extractions
            if (
                e.start_date.date() == start_dt.date()
                and e.end_date.date() == end_dt.date()
                and e.interval == interval
            )
        ]

        processed_symbols = [e.ticker for e in relevant_extractions]
        pending_symbols = [
            sym for sym in all_symbols if sym not in processed_symbols
        ]
        should_continue = len(pending_symbols) > 0

        logger.info(f"Total symbols: {len(all_symbols)}")
        logger.info(f"Processed symbols: {len(processed_symbols)}")
        logger.info(f"Pending symbols: {len(pending_symbols)}")
        
        if should_continue:
            logger.info(
                f"DAG will continue: {len(pending_symbols)} symbols still need processing"
            )
            logger.info(f"First 5 pending symbols: {pending_symbols[:5] if len(pending_symbols) >= 5 else pending_symbols}")
        else:
            logger.info(
                "DAG completed: All symbols processed successfully - STOPPING DAG"
            )

        return should_continue
    except Exception as e:
        logger.error(f"Error in should_continue_data: {e}")
        return False


def should_continue_scraper(**kwargs):
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            return not state.get("complete", False)
        return False
    except Exception as e:
        logger.error(f"Error checking state file for loop logic: {e}")
        return False
