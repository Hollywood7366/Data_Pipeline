from datetime import datetime, timedelta
from typing import List

from src.pipelines.extras.asyncer import (
    get_async_logs_script,
    run_async_task,
)
from src.pipelines.extras.extraction_manager import ExtractionManager
from utils.iqfeed_utils import (
    clean_data,
    close_socket,
    connect_to_socket,
    data_to_parquet,
    establish_live_feed,
    receive_data,
    send_message_to_socket,
)
from utils.logging import Logger
from utils.util import _parse_raw_data

logger = Logger(name="iqfeed", log_dir="data/logs")


def historical(
    host: str,
    port: int,
    start_date: str,
    interval: str,
    tickers: List[str],
    records,
    end_date: str = None,
    backfill = False
):
    all_data = {}
    successful_tickers = []

    extraction_manager = ExtractionManager()

    if start_date == "CURRENT":
        day_before_yesterday = datetime.now() - timedelta(days=2)
        start_date = day_before_yesterday.strftime("%Y%m%d")
    if not end_date:
        yesterday = datetime.now() - timedelta(days=1)
        end_date = yesterday.strftime("%Y%m%d")

    try:
        sock = connect_to_socket(host, port)
        send_message_to_socket(sock, "S,SET PROTOCOL,6.2\n")

        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")

        yesterday_dt = datetime.now() - timedelta(days=1)
        if end_dt > yesterday_dt:
            end_dt = yesterday_dt
            end_date = yesterday_dt.strftime("%Y%m%d")
            logger.info(f"End date adjusted to previous day: {end_date}")

        for sym in tickers:
            if backfill == False:
                if not extraction_manager.should_process_ticker(
                    sym, start_dt, end_dt, interval
                ):
                    logger.info(
                        f"Skipping {sym} - already processed for this date range"
                    )
                    # successful_tickers.append(sym)
                    continue

            logger.info(f"Downloading data for: {sym}")
            if interval.upper() == "TICK":
                message = (
                    f"HTT,{sym},{start_date} 093000,{end_date} 160000\n"
                )
            else:
                message = f"HIT,{sym},{interval},{start_date} 093000,{end_date} 160000\n"

            send_message_to_socket(sock, message)
            data = receive_data(sock)
            data = clean_data(data=data)
            if data and not data.isspace():
                data_to_parquet(
                    data=data, sym=sym, interval=interval, records=records
                )
                formatted_rows = _parse_raw_data(data)
                record_count = len(formatted_rows)
                run_async_task(
                    extraction_manager.record_extraction(
                        ticker=sym,
                        start_date=start_dt,
                        end_date=end_dt,
                        interval=interval,
                        successful=True,
                        record_count=record_count,
                    )
                )
                all_data[sym] = data
                successful_tickers.append(sym)
            else:
                logger.warning(f"No valid data received for {sym}")
                run_async_task(
                    extraction_manager.record_extraction(
                        ticker=sym,
                        start_date=start_dt,
                        end_date=end_dt,
                        interval=interval,
                        successful=False,
                        record_count=0,
                    )
                )

        run_async_task(get_async_logs_script(successful_tickers))
        close_socket(sock)
        return all_data
    except Exception as e:
        logger.error(f"Error in historical data download: {e}")
        run_async_task(get_async_logs_script(successful_tickers))


def live(host: str, port: int, ticker: str):
    try:
        sock = connect_to_socket(host, port)
        establish_live_feed(sock, ticker_name=ticker)
    except Exception as e:
        logger.error(f"Error in live feed: {e}")
        close_socket(sock)
