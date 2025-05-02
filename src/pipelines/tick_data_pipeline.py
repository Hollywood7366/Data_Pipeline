import socket
import time
from datetime import datetime, timedelta

import polars as pl
import pytz

from src.config.config import config as cn
from src.pipelines.extras.asyncer import run_async_task
from src.pipelines.extras.extraction_manager import ExtractionManager
from utils.iqfeed_utils import data_to_parquet


def get_target_location(time_zone):
    abbreviations = {
        "UTC": "UTC",
        "": "America/New_York",
        "ET": "America/New_York",
        "EST": "America/New_York",
        "CT": "America/Chicago",
        "CST": "America/Chicago",
        "PT": "America/Los_Angeles",
        "PST": "America/Los_Angeles",
    }

    if time_zone.upper() in abbreviations:
        time_zone = abbreviations[time_zone.upper()]

    return pytz.timezone(time_zone)


def milliseconds_timestamp():
    return int(time.time() * 1000)


def create_tick_request(symbol, request_id, config):
    return f"HTT,{symbol.upper()},{config.start_date},{config.end_date},,,,1,{request_id}"


def download_ticks(symbol, config, records):
    successful = False
    interval = "TICK"
    extraction_manager = ExtractionManager()

    if config.start_date == "CURRENT":
        day_before_yesterday = datetime.now() - timedelta(days=2)
        config.start_date = day_before_yesterday.strftime("%Y%m%d")

    if config.end_date:
        end_dt = datetime.strptime(config.end_date, "%Y%m%d")
        yesterday_dt = datetime.now() - timedelta(days=1)
        if end_dt > yesterday_dt:
            config.end_date = yesterday_dt.strftime("%Y%m%d")
            print(f"End date adjusted to previous day: {config.end_date}")

    start_dt = ""
    end_dt = ""

    # Check if we should process this ticker
    current_start_dt = start_dt
    current_end_dt = end_dt

    should_process, adjusted_start_dt, adjusted_end_dt = (
        extraction_manager.should_process_ticker(
            symbol, start_dt, end_dt, interval
        )
    )

    if not should_process:
        print(f"Skipping {symbol} - already processed for this date range")
        return

    current_start_dt = adjusted_start_dt
    current_end_dt = adjusted_end_dt
    config.start_date = (
        current_start_dt.strftime("%Y%m%d") if current_start_dt else ""
    )
    config.end_date = (
        current_end_dt.strftime("%Y%m%d") if current_end_dt else ""
    )

    print(
        f"Processing {symbol} from {config.start_date} to {config.end_date}"
    )

    try:
        target_location = get_target_location(config.time_zone)
        source_location = pytz.timezone("America/New_York")
    except Exception as e:
        print(f"Error: Could not load time zone: {e}")
        return

    started = milliseconds_timestamp()
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((cn.IQFEED_HOST, int(cn.IQFEED_PORT)))
        print("Connection successful")
    except Exception as e:
        print(f"Error: Could not connect to IQFeed at port 9100: {e}")
        return

    try:
        # Send protocol message
        conn.sendall(f"S,SET PROTOCOL,{config.protocol}\r\n".encode())

        # Send tick data request
        request_id = str(int(time.time()))
        request = create_tick_request(symbol, request_id, config)
        if config.detailed_logging:
            print(f"Debug: {request}")
        conn.sendall(f"{request}\r\n".encode())

        print(f"Info: Downloading {symbol}")

        row_count = 0
        buffer = ""

        datetime_col = []
        last_col = []
        lastsize_col = []
        totalsize_col = []
        bid_col = []
        ask_col = []
        tickid_col = []
        basis_col = []
        market_col = []
        cond_col = []

        while True:
            data = conn.recv(4 * 1024 * 1024).decode("utf-8")
            if not data:
                break

            buffer += data
            lines = buffer.split("\r\n")
            buffer = lines.pop()

            for line in lines:
                if not line:
                    continue

                iqfeed_row = line.split(",")

                if len(iqfeed_row) == 0:
                    continue
                if iqfeed_row[0] == "S":
                    continue
                if iqfeed_row[0] != request_id:
                    continue
                if iqfeed_row[1] == "!ENDMSG!":
                    break
                if iqfeed_row[1] == "E" and len(iqfeed_row) >= 3:
                    print(f"Error: IQFeed error: {iqfeed_row[2]}")
                    return

                try:
                    if config.detailed_logging:
                        print(f"Debug: {','.join(iqfeed_row)}")

                    if len(iqfeed_row) < 11:
                        continue

                    timestamp_str = iqfeed_row[1]
                    try:
                        timestamp = datetime.strptime(
                            timestamp_str, "%Y-%m-%d %H:%M:%S.%f"
                        )
                        timestamp = source_location.localize(timestamp)

                        timestamp = timestamp.astimezone(target_location)

                        datetime_col.append(timestamp)
                        last_col.append(
                            float(iqfeed_row[2]) if iqfeed_row[2] else None
                        )
                        lastsize_col.append(
                            int(iqfeed_row[3]) if iqfeed_row[3] else None
                        )
                        totalsize_col.append(
                            int(iqfeed_row[4]) if iqfeed_row[4] else None
                        )
                        bid_col.append(
                            float(iqfeed_row[5]) if iqfeed_row[5] else None
                        )
                        ask_col.append(
                            float(iqfeed_row[6]) if iqfeed_row[6] else None
                        )
                        tickid_col.append(iqfeed_row[7])
                        basis_col.append(iqfeed_row[8])
                        market_col.append(iqfeed_row[9])
                        cond_col.append(iqfeed_row[10])

                        row_count += 1
                    except ValueError as e:
                        print(
                            f"Warning: Could not parse timestamp: {timestamp_str} - {e}"
                        )
                        continue

                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue

        if row_count > 0:
            df = pl.DataFrame(
                {
                    "datetime": datetime_col,
                    "last": last_col,
                    "lastsize": lastsize_col,
                    "totalsize": totalsize_col,
                    "bid": bid_col,
                    "ask": ask_col,
                    "tickid": tickid_col,
                    "basis": basis_col,
                    "market": market_col,
                    "cond": cond_col,
                }
            )

            data_to_parquet(
                data=df,
                sym=symbol,
                interval=interval,
                records=records,
                ticks=True,
            )

            successful = True
            duration = milliseconds_timestamp() - started

            print(
                f"Info: Completed {symbol} in {duration}ms with {row_count} rows, saved as Parquet"
            )

            # Record successful extraction
            if extraction_manager:
                run_async_task(
                    extraction_manager.record_extraction(
                        ticker=symbol,
                        start_date=current_start_dt,
                        end_date=current_end_dt,
                        interval=interval,
                        successful=True,
                        record_count=row_count,
                    )
                )
        else:
            print(f"Warning: No data received for {symbol}")
            # Record failed extraction
            if extraction_manager:
                run_async_task(
                    extraction_manager.record_extraction(
                        ticker=symbol,
                        start_date=current_start_dt,
                        end_date=current_end_dt,
                        interval=interval,
                        successful=False,
                        record_count=0,
                    )
                )

    except Exception as e:
        print(f"Error: {e}")
        # Record failed extraction
        if extraction_manager:
            run_async_task(
                extraction_manager.record_extraction(
                    ticker=symbol,
                    start_date=current_start_dt,
                    end_date=current_end_dt,
                    interval=interval,
                    successful=False,
                    record_count=0,
                )
            )
    finally:
        try:
            conn.close()
        except:
            pass
    return successful
