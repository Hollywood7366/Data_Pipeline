import os
import socket

import polars as pl

from utils.atomic_creator import AtomicFileUpdate
from utils.logging import Logger

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


def data_to_parquet(
    data: str, sym: str, start_date: str, end_date: str, interval: str
) -> None:
    os.makedirs("data", exist_ok=True)

    filename = f"{sym}_{start_date}_{end_date}_{interval}.parquet"
    filepath = os.path.join("data", filename)

    lines = [
        line for line in data.split("\n") if not line.startswith("S,") and line.strip()
    ]

    if not lines:
        logger.warning(f"No valid data for {sym}, skipping Parquet creation.")
        return

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
    formatted_rows = []

    for line in lines:
        parts = line.split(",")
        if len(parts) >= 8 and parts[0] in ["LH", "DT", "T"]:
            row = parts[1:]
        else:
            row = parts
        if len(row) == 8:
            formatted_rows.append(row)

    if not formatted_rows:
        logger.warning(f"No properly formatted rows for {sym}.")
        return

    try:
        df = pl.DataFrame(formatted_rows, schema=headers)
        atomic_update = AtomicFileUpdate(filepath, f"{filepath}.tmp")
        atomic_update.perform_atomic_update(df)
        logger.info(f"Data saved to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save {sym} data to Parquet: {e}")


def clean_data(data: str) -> str:
    return data.replace("\r", "").replace(",\n", "\n").strip()


def establish_live_feed(sock: socket.socket, ticker_name: str) -> None:
    try:
        send_message_to_socket(sock, "S,TIMESTAMPSOFF\n")
        send_message_to_socket(sock, f"w{ticker_name}\n")
        logger.info(f"Live feed started for {ticker_name}")

        while True:
            data = receive_data(sock)
            if data:
                print(clean_data(data))
    except KeyboardInterrupt:
        logger.info("Live feed stopped by user.")
        close_socket(sock)
    except Exception as e:
        logger.error(f"Error in live feed: {e}")
        close_socket(sock)
