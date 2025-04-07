import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import polars as pl

from utils.atomic_creator import AtomicFileUpdate


class ParquetDatabaseHandler:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.exchanges = set()
        self.security_types = {}
        self.timeframes = {}
        self.symbols = {}
        self.validate_and_create_structure()

    def validate_and_create_structure(self) -> bool:
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True)
            print(f"Created base directory: {self.base_path}")
            return True

        self._scan_directory_structure()
        return True

    def _scan_directory_structure(self) -> None:
        if not self.base_path.exists():
            return

        for exchange_dir in self.base_path.iterdir():
            if exchange_dir.is_dir():
                exchange = exchange_dir.name
                self.exchanges.add(exchange)
                self.security_types[exchange] = set()

                for security_type_dir in exchange_dir.iterdir():
                    if security_type_dir.is_dir():
                        security_type = security_type_dir.name
                        self.security_types[exchange].add(security_type)
                        security_key = (exchange, security_type)
                        self.timeframes[security_key] = set()

                        for timeframe_dir in security_type_dir.iterdir():
                            if timeframe_dir.is_dir():
                                timeframe = timeframe_dir.name
                                self.timeframes[security_key].add(timeframe)
                                timeframe_key = (exchange, security_type, timeframe)
                                self.symbols[timeframe_key] = set()

                                for file_path in timeframe_dir.glob("*.parquet"):
                                    symbol = file_path.stem
                                    self.symbols[timeframe_key].add(symbol)

    def create_exchange_directory(self, exchange: str) -> Path:
        exchange = self._sanitize_name(exchange)
        exchange_dir = self.base_path / exchange

        if not exchange_dir.exists():
            exchange_dir.mkdir(parents=True)
            print(f"Created exchange directory: {exchange_dir}")

        self.exchanges.add(exchange)
        if exchange not in self.security_types:
            self.security_types[exchange] = set()

        return exchange_dir

    def create_security_type_directory(self, exchange: str, security_type: str) -> Path:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)

        exchange_dir = self.create_exchange_directory(exchange)
        security_type_dir = exchange_dir / security_type

        if not security_type_dir.exists():
            security_type_dir.mkdir(parents=True)
            print(f"Created security type directory: {security_type_dir}")

        self.security_types[exchange].add(security_type)
        security_key = (exchange, security_type)
        if security_key not in self.timeframes:
            self.timeframes[security_key] = set()

        return security_type_dir

    def create_timeframe_directory(
        self, exchange: str, security_type: str, timeframe: str
    ) -> Path:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)

        security_type_dir = self.create_security_type_directory(exchange, security_type)
        timeframe_dir = security_type_dir / timeframe

        if not timeframe_dir.exists():
            timeframe_dir.mkdir(parents=True)
            print(f"Created timeframe directory: {timeframe_dir}")

        security_key = (exchange, security_type)
        self.timeframes[security_key].add(timeframe)
        timeframe_key = (exchange, security_type, timeframe)
        if timeframe_key not in self.symbols:
            self.symbols[timeframe_key] = set()

        return timeframe_dir

    def save_data(
        self,
        exchange: str,
        security_type: str,
        timeframe: str,
        symbol: str,
        data: pl.DataFrame,
    ) -> Path:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)
        symbol = self._sanitize_name(symbol)

        timeframe_dir = self.create_timeframe_directory(
            exchange, security_type, timeframe
        )
        file_path = timeframe_dir / f"{symbol}.parquet"

        # data.write_parquet(file_path)
        atomic_update = AtomicFileUpdate(file_path, f"{file_path}.tmp")
        atomic_update.perform_atomic_update(data)
        print(f"Saved data to: {file_path}")

        timeframe_key = (exchange, security_type, timeframe)
        self.symbols[timeframe_key].add(symbol)

        return file_path

    def load_data(
        self, exchange: str, security_type: str, timeframe: str, symbol: str
    ) -> pl.DataFrame:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)
        symbol = self._sanitize_name(symbol)

        file_path = (
            self.base_path / exchange / security_type / timeframe / f"{symbol}.parquet"
        )

        if not file_path.exists():
            raise FileNotFoundError(
                f"No data found for {symbol} ({timeframe}/{security_type}) on {exchange}"
            )

        return pl.read_parquet(file_path)

    def append_data(
        self,
        exchange: str,
        security_type: str,
        timeframe: str,
        symbol: str,
        new_data: pl.DataFrame,
    ) -> Path:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)
        symbol = self._sanitize_name(symbol)

        file_path = (
            self.base_path / exchange / security_type / timeframe / f"{symbol}.parquet"
        )

        if file_path.exists():
            try:
                existing_data = pl.read_parquet(file_path)
                combined_data = pl.concat([existing_data, new_data])

                # Handle sorting and removing duplicates
                if "datetime" in combined_data.columns:
                    combined_data = combined_data.sort("datetime").unique(
                        subset=["datetime"]
                    )
                elif "date" in combined_data.columns:
                    combined_data = combined_data.sort("date").unique(subset=["date"])
                else:
                    combined_data = combined_data.unique()

                combined_data.write_parquet(file_path)
                print(f"Appended data to: {file_path}")

            except Exception as e:
                print(f"Error appending data: {e}")
                return self.save_data(
                    exchange, security_type, timeframe, symbol, new_data
                )
        else:
            return self.save_data(exchange, security_type, timeframe, symbol, new_data)

        timeframe_key = (exchange, security_type, timeframe)
        self.symbols[timeframe_key].add(symbol)

        return file_path

    def list_exchanges(self) -> List[str]:
        return sorted(list(self.exchanges))

    def list_security_types(self, exchange: str) -> List[str]:
        exchange = self._sanitize_name(exchange)

        if exchange not in self.security_types:
            return []

        return sorted(list(self.security_types[exchange]))

    def list_timeframes(self, exchange: str, security_type: str) -> List[str]:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)

        security_key = (exchange, security_type)
        if security_key not in self.timeframes:
            return []

        return sorted(list(self.timeframes[security_key]))

    def list_symbols(
        self, exchange: str, security_type: str, timeframe: str
    ) -> List[str]:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)

        timeframe_key = (exchange, security_type, timeframe)
        if timeframe_key not in self.symbols:
            return []

        return sorted(list(self.symbols[timeframe_key]))

    def delete_symbol_data(
        self, exchange: str, security_type: str, timeframe: str, symbol: str
    ) -> bool:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)
        symbol = self._sanitize_name(symbol)

        file_path = (
            self.base_path / exchange / security_type / timeframe / f"{symbol}.parquet"
        )

        if file_path.exists():
            file_path.unlink()
            timeframe_key = (exchange, security_type, timeframe)
            if timeframe_key in self.symbols and symbol in self.symbols[timeframe_key]:
                self.symbols[timeframe_key].remove(symbol)
            return True

        return False

    def delete_timeframe(
        self, exchange: str, security_type: str, timeframe: str
    ) -> bool:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)

        timeframe_dir = self.base_path / exchange / security_type / timeframe

        if timeframe_dir.exists():
            shutil.rmtree(timeframe_dir)
            security_key = (exchange, security_type)
            if (
                security_key in self.timeframes
                and timeframe in self.timeframes[security_key]
            ):
                self.timeframes[security_key].remove(timeframe)
            timeframe_key = (exchange, security_type, timeframe)
            if timeframe_key in self.symbols:
                del self.symbols[timeframe_key]
            return True

        return False

    def _sanitize_name(self, name: str) -> str:
        sanitized = re.sub(r"[^\w\-]", "_", name)
        return sanitized

    def get_database_info(self) -> Dict[str, Any]:
        exchange_count = len(self.exchanges)
        security_type_count = sum(
            len(sec_types) for sec_types in self.security_types.values()
        )
        timeframe_count = sum(
            len(timeframes) for timeframes in self.timeframes.values()
        )
        symbol_count = sum(len(symbols) for symbols in self.symbols.values())

        file_count = 0
        total_size_bytes = 0

        for exchange in self.exchanges:
            exchange_dir = self.base_path / exchange
            for security_type in self.security_types.get(exchange, []):
                security_type_dir = exchange_dir / security_type
                security_key = (exchange, security_type)
                for timeframe in self.timeframes.get(security_key, []):
                    timeframe_dir = security_type_dir / timeframe
                    for file_path in timeframe_dir.glob("*.parquet"):
                        file_count += 1
                        total_size_bytes += file_path.stat().st_size

        if total_size_bytes < 1024:
            size_str = f"{total_size_bytes} bytes"
        elif total_size_bytes < 1024 * 1024:
            size_str = f"{total_size_bytes / 1024:.2f} KB"
        elif total_size_bytes < 1024 * 1024 * 1024:
            size_str = f"{total_size_bytes / (1024 * 1024):.2f} MB"
        else:
            size_str = f"{total_size_bytes / (1024 * 1024 * 1024):.2f} GB"

        return {
            "base_path": str(self.base_path),
            "exchanges": exchange_count,
            "security_types": security_type_count,
            "timeframes": timeframe_count,
            "symbols": symbol_count,
            "files": file_count,
            "total_size": size_str,
            "last_updated": datetime.now().isoformat(),
        }

    def search_symbols(
        self,
        pattern: str,
        exchange: Optional[str] = None,
        security_type: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> List[Tuple[str, str, str, str]]:
        results = []

        try:
            regex = re.compile(pattern, re.IGNORECASE)
        except re.error:
            pattern = re.escape(pattern)
            regex = re.compile(pattern, re.IGNORECASE)

        exchanges_to_search = [exchange] if exchange else self.exchanges

        for exch in exchanges_to_search:
            if exch not in self.exchanges:
                continue

            security_types_to_search = (
                [security_type] if security_type else self.security_types.get(exch, [])
            )

            for sec_type in security_types_to_search:
                security_key = (exch, sec_type)
                if security_key not in self.timeframes:
                    continue

                timeframes_to_search = (
                    [timeframe] if timeframe else self.timeframes.get(security_key, [])
                )

                for tf in timeframes_to_search:
                    timeframe_key = (exch, sec_type, tf)
                    if timeframe_key not in self.symbols:
                        continue

                    for symbol in self.symbols[timeframe_key]:
                        if regex.search(symbol):
                            results.append((exch, sec_type, tf, symbol))

        return sorted(results)
