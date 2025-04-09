from typing import List

from src.pipelines.extras.asyncer import (
    get_async_logs_script,
    run_async_task,
)
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

logger = Logger(name="iqfeed", log_dir="data/logs")


def historical(
    host: str,
    port: int,
    start_date: str,
    end_date: str,
    interval: str,
    tickers: List[str],
    records,
):
    all_data = {}
    successful_tickers = []
    try:
        sock = connect_to_socket(host, port)
        send_message_to_socket(sock, "S,SET PROTOCOL,6.2\n")

        for sym in tickers:
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
                all_data[sym] = data
                successful_tickers.append(sym)
            else:
                logger.warning(f"No valid data received for {sym}")

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
