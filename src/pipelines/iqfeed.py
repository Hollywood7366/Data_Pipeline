from typing import List
from utils.iqfeed_utils import (
    connect_to_socket, send_message_to_socket, receive_data, clean_data, data_to_csv,
    close_socket, establish_live_feed
)
from utils.logging import Logger

logger = Logger(name='iqfeed', log_dir='data/logs')

def historical(
    host: str,
    port: int,
    start_date: str,
    end_date: str,
    interval: str,
    tickers: List[str],
):
    try:
        sock = connect_to_socket(host, port)
        send_message_to_socket(sock, "S,SET PROTOCOL,6.2\n")

        for sym in tickers:
            logger.info(f"Downloading data for: {sym}")

            if interval.upper() == "TICK":
                message = f"HTT,{sym},{start_date} 093000,{end_date} 160000\n"
            else:
                message = f"HIT,{sym},{interval},{start_date} 093000,{end_date} 160000\n"

            send_message_to_socket(sock, message)
            data = receive_data(sock)
            data = clean_data(data)
            data_to_csv(data, sym, start_date, end_date, interval)

        close_socket(sock)
    except Exception as e:
        logger.error(f"Error in historical data download: {e}")

def live(host: str, port: int, ticker: str):
    try:
        sock = connect_to_socket(host, port)
        establish_live_feed(sock, ticker_name=ticker)
    except Exception as e:
        logger.error(f"Error in live feed: {e}")
        close_socket(sock)
