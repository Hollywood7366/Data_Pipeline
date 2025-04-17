import socket
from pathlib import Path

import polars as pl

from src.pipelines.extras.asyncer import (
    data_to_parquet_async,
    run_async_task,
)
from src.pipelines.transformations.misc import ParquetDatabaseHandler
from utils.CONSTANTS import STORAGE_DIR
from utils.logging import Logger
from utils.util import clean_data

logger = Logger(name="iqfeed", log_dir="data/logs")


def connect_to_socket(host: str, port: int) -> socket.socket:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        logger.info("Connection established.")
        return sock
    except socket.error as e:
        logger.error(f"Socket connection failed: {e}")
        raise


def close_socket(sock: socket.socket) -> None:
    try:
        sock.close()
        logger.info("Connection closed.")
    except socket.error as e:
        logger.error(f"Failed to close socket: {e}")


def send_message_to_socket(sock: socket.socket, message: str) -> None:
    try:
        sock.sendall(message.encode("utf-8"))
        logger.info(f"Message sent: {message.strip()}")
    except socket.error as e:
        logger.error(f"Failed to send message: {e}")
        raise


def receive_data(sock: socket.socket, recv_buffer=4096) -> str:
    buffer = ""
    try:
        while True:
            data = sock.recv(recv_buffer).decode("utf-8")
            buffer += data
            if "!ENDMSG!" in buffer:
                break
        buffer = buffer.replace("!ENDMSG!", "").strip()
        logger.info("Data received successfully.")
    except socket.error as e:
        logger.error(f"Error receiving data: {e}")
        return ""

    return buffer


def data_to_parquet(data: str, sym: str, interval: str, records) -> None:
    filtered_records = records.filter(pl.col("symbol") == sym)
    if filtered_records.height == 0:
        logger.warning(
            f"No records found for symbol {sym}, skipping Parquet creation."
        )
        return

    exchange = filtered_records.select("exchange").row(0)[0]
    security_type = filtered_records.select("security_type").row(0)[0]

    formatted_rows = _parse_raw_data(data)
    if not formatted_rows:
        logger.warning(
            f"No valid data for {sym}, skipping Parquet creation."
        )
        return

    try:
        df = _create_dataframe(formatted_rows)
        parquet_handler = ParquetDatabaseHandler(base_path=STORAGE_DIR)

        file_path = (
            Path(STORAGE_DIR)
            / exchange
            / security_type
            / interval
            / f"{sym}.parquet"
        )

        if file_path.exists():
            file_path = parquet_handler.append_data(
                exchange=exchange,
                security_type=security_type,
                timeframe=interval,
                symbol=sym,
                new_data=df,
            )
            logger.info(f"Appended data to existing file: {file_path}")
        else:
            file_path = parquet_handler.save_data(
                exchange=exchange,
                security_type=security_type,
                timeframe=interval,
                symbol=sym,
                data=df,
            )
            logger.info(f"Created new file: {file_path}")

        metadata_record = _create_metadata_record(df, sym)
        run_async_task(data_to_parquet_async(sym, metadata_record))
    except Exception as e:
        logger.error(f"Failed to save {sym} data to Parquet: {e}")


def establish_live_feed(sock: socket.socket, ticker_name: str) -> None:
    try:
        send_message_to_socket(sock, "S,TIMESTAMPSOFF\n")
        send_message_to_socket(sock, f"w{ticker_name}\n")
        logger.info(f"Live feed started for {ticker_name}")

        while True:
            data = receive_data(sock)
            if data:
                return clean_data(data)
    except KeyboardInterrupt:
        logger.info("Live feed stopped by user.")
        close_socket(sock)
    except Exception as e:
        logger.error(f"Error in live feed: {e}")
        close_socket(sock)


def _parse_raw_data(data: str) -> list:
    lines = [
        line
        for line in data.split("\n")
        if not line.startswith("S,") and line.strip()
    ]

    if not lines:
        return []

    formatted_rows = []
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 8 and parts[0] in ["LH", "DT", "T"]:
            row = parts[1:]
        else:
            row = parts
        if len(row) == 8:
            formatted_rows.append(row)

    return formatted_rows


def _create_dataframe(formatted_rows: list) -> pl.DataFrame:
    headers = [
        "DateTime",
        "High",
        "Low",
        "Open",
        "Close",
        "TotalVolume",
        "PeriodVolume",
        "Unknown",
    ]

    df = pl.DataFrame(formatted_rows, schema=headers)
    df = df.drop("Unknown")

    df = df.with_columns(
        [
            pl.col("High").cast(pl.Float64),
            pl.col("Low").cast(pl.Float64),
            pl.col("Open").cast(pl.Float64),
            pl.col("Close").cast(pl.Float64),
            pl.col("TotalVolume").cast(pl.Int64),
            pl.col("PeriodVolume").cast(pl.Int64),
        ]
    )

    return df


def _save_data_and_metadata(
    df: pl.DataFrame,
    sym: str,
    exchange: str,
    security_type: str,
    interval: str,
) -> None:
    parquet_handler = ParquetDatabaseHandler(base_path=STORAGE_DIR)

    file_path = parquet_handler.save_data(
        exchange=exchange,
        security_type=security_type,
        timeframe=interval,
        symbol=sym,
        data=df,
    )
    logger.info(f"Data saved to {file_path}")

    metadata_record = _create_metadata_record(df, sym)
    run_async_task(data_to_parquet_async(sym, metadata_record))


def _create_metadata_record(df: pl.DataFrame, sym: str) -> dict:
    max_high = df.select(pl.col("High").max())[0, 0]
    min_low = df.select(pl.col("Low").min())[0, 0]
    total_volume = df.select(pl.col("TotalVolume").sum())[0, 0]
    
    if total_volume > 9223372036854775807:
        logger.warning(f"Total volume for {sym} exceeds database limits, capping value")
        total_volume = 9223372036854775807
    if total_volume > 2147483647:
        logger.warning(f"Total volume for {sym} exceeds INT limit, capping value")
        total_volume = 2147483647
    
    avg_close = df.select(pl.col("Close").mean())[0, 0]
    last_datetime = df.select(pl.col("DateTime")).row(-1)[0]
    count = df.height

    return {
        "symbol": sym,
        "count": count,
        "last_record_datetime": last_datetime,
        "max_high": max_high,
        "min_low": min_low,
        "total_volume": total_volume,
        "average_close": avg_close,
        "period": "daily",
    }