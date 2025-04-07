import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd


class ParquetDatabaseHandler:
    """
    Handles a local database structure using Parquet files for storing IQFeed stock data.

    Directory structure:
    base_path/
    ├── exchange_1/
    │   ├── security_type_1/
    │   │   ├── timeframe_1/ (e.g., 1min, 5min, 15min)
    │   │   │   ├── symbol1.parquet
    │   │   │   ├── symbol2.parquet
    │   │   │   └── ...
    │   │   └── timeframe_2/
    │   │       └── ...
    │   └── security_type_2/
    │       └── ...
    └── exchange_2/
        └── ...
    """

    def __init__(self, base_path: str):
        """
        Initialize the ParquetDatabaseHandler with a base path.

        Args:
            base_path: Base directory path where the database will be stored
        """
        self.base_path = Path(base_path)
        self.exchanges = set()
        self.security_types = (
            {}
        )  # Dictionary mapping exchanges to sets of security types
        self.timeframes = (
            {}
        )  # Dictionary mapping (exchange, security_type) tuples to sets of timeframes
        self.symbols = (
            {}
        )  # Dictionary mapping (exchange, security_type, timeframe) tuples to sets of symbols
        self.validate_and_create_structure()

    def validate_and_create_structure(self) -> bool:
        """
        Validates the database structure and creates it if it doesn't exist.

        Returns:
            bool: True if structure is valid or was successfully created
        """
        # Create base directory if it doesn't exist
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True)
            print(f"Created base directory: {self.base_path}")
            return True

        # If base directory exists, scan it to populate exchanges, security types, timeframes, and symbols
        self._scan_directory_structure()
        return True

    def _scan_directory_structure(self) -> None:
        """Scans the existing directory structure and populates exchanges, security types, timeframes, and symbols."""
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
        """
        Creates a directory for the specified exchange if it doesn't exist.

        Args:
            exchange: The exchange name

        Returns:
            Path: Path to the exchange directory
        """
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
        """
        Creates a directory for the specified security type under the given exchange.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)

        Returns:
            Path: Path to the security type directory
        """
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
        """
        Creates a directory for the specified timeframe under the given exchange and security type.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)
            timeframe: The timeframe (e.g., 1min, 5min, 15min, 1h, 1d)

        Returns:
            Path: Path to the timeframe directory
        """
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
        data: pd.DataFrame,
    ) -> Path:
        """
        Saves data to a parquet file for the specified symbol, timeframe, security type, and exchange.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)
            timeframe: The timeframe (e.g., 1min, 5min, 15min, 1h, 1d)
            symbol: The symbol (e.g., AAPL, MSFT)
            data: The pandas DataFrame to save

        Returns:
            Path: Path to the saved parquet file
        """
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)
        symbol = self._sanitize_name(symbol)

        timeframe_dir = self.create_timeframe_directory(
            exchange, security_type, timeframe
        )
        file_path = timeframe_dir / f"{symbol}.parquet"

        data.to_parquet(file_path)
        print(f"Saved data to: {file_path}")

        timeframe_key = (exchange, security_type, timeframe)
        self.symbols[timeframe_key].add(symbol)

        return file_path

    def load_data(
        self, exchange: str, security_type: str, timeframe: str, symbol: str
    ) -> pd.DataFrame:
        """
        Loads data from a parquet file for the specified symbol, timeframe, security type, and exchange.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)
            timeframe: The timeframe (e.g., 1min, 5min, 15min, 1h, 1d)
            symbol: The symbol (e.g., AAPL, MSFT)

        Returns:
            pd.DataFrame: The loaded data
        """
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

        return pd.read_parquet(file_path)

    def append_data(
        self,
        exchange: str,
        security_type: str,
        timeframe: str,
        symbol: str,
        new_data: pd.DataFrame,
    ) -> Path:
        """
        Appends data to an existing parquet file. If the file doesn't exist, creates a new one.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)
            timeframe: The timeframe (e.g., 1min, 5min, 15min, 1h, 1d)
            symbol: The symbol (e.g., AAPL, MSFT)
            new_data: The pandas DataFrame to append

        Returns:
            Path: Path to the saved parquet file
        """
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        timeframe = self._sanitize_name(timeframe)
        symbol = self._sanitize_name(symbol)

        file_path = (
            self.base_path / exchange / security_type / timeframe / f"{symbol}.parquet"
        )

        if file_path.exists():
            try:
                # Load existing data
                existing_data = pd.read_parquet(file_path)

                # Concatenate and drop duplicates
                combined_data = pd.concat([existing_data, new_data])

                # Check if there's a datetime or date column to sort by
                if "datetime" in combined_data.columns:
                    combined_data = combined_data.sort_values(
                        "datetime"
                    ).drop_duplicates()
                elif "date" in combined_data.columns:
                    combined_data = combined_data.sort_values("date").drop_duplicates()
                else:
                    # If no date column, just drop duplicates with all columns
                    combined_data = combined_data.drop_duplicates()

                # Save back to the file
                combined_data.to_parquet(file_path)
                print(f"Appended data to: {file_path}")

            except Exception as e:
                print(f"Error appending data: {e}")
                # If there's an error, save the new data as is
                return self.save_data(
                    exchange, security_type, timeframe, symbol, new_data
                )
        else:
            # If file doesn't exist, just save the new data
            return self.save_data(exchange, security_type, timeframe, symbol, new_data)

        timeframe_key = (exchange, security_type, timeframe)
        self.symbols[timeframe_key].add(symbol)

        return file_path

    def list_exchanges(self) -> List[str]:
        """
        Lists all available exchanges.

        Returns:
            List[str]: List of exchange names
        """
        return sorted(list(self.exchanges))

    def list_security_types(self, exchange: str) -> List[str]:
        """
        Lists all available security types for a specific exchange.

        Args:
            exchange: The exchange name

        Returns:
            List[str]: List of security type names
        """
        exchange = self._sanitize_name(exchange)

        if exchange not in self.security_types:
            return []

        return sorted(list(self.security_types[exchange]))

    def list_timeframes(self, exchange: str, security_type: str) -> List[str]:
        """
        Lists all available timeframes for a specific exchange and security type.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)

        Returns:
            List[str]: List of timeframe names
        """
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)

        security_key = (exchange, security_type)
        if security_key not in self.timeframes:
            return []

        return sorted(list(self.timeframes[security_key]))

    def list_symbols(
        self, exchange: str, security_type: str, timeframe: str
    ) -> List[str]:
        """
        Lists all available symbols for a specific exchange, security type, and timeframe.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)
            timeframe: The timeframe (e.g., 1min, 5min, 15min, 1h, 1d)

        Returns:
            List[str]: List of symbol names
        """
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
        """
        Deletes data for the specified symbol, timeframe, security type, and exchange.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)
            timeframe: The timeframe (e.g., 1min, 5min, 15min, 1h, 1d)
            symbol: The symbol (e.g., AAPL, MSFT)

        Returns:
            bool: True if deletion was successful
        """
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
        """
        Deletes all data for the specified timeframe, security type, and exchange.

        Args:
            exchange: The exchange name
            security_type: The security type (e.g., STOCK, OPTION, FUTURE)
            timeframe: The timeframe (e.g., 1min, 5min, 15min, 1h, 1d)

        Returns:
            bool: True if deletion was successful
        """
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
        """
        Sanitizes a name for use as a directory or file name.

        Args:
            name: The name to sanitize

        Returns:
            str: The sanitized name
        """
        # Replace any characters that aren't alphanumerics, dash, or underscore
        sanitized = re.sub(r"[^\w\-]", "_", name)
        return sanitized

    def get_database_info(self) -> Dict[str, Any]:
        """
        Gets information about the database.

        Returns:
            Dict: Dictionary containing database information
        """
        exchange_count = len(self.exchanges)
        security_type_count = sum(
            len(sec_types) for sec_types in self.security_types.values()
        )
        timeframe_count = sum(
            len(timeframes) for timeframes in self.timeframes.values()
        )
        symbol_count = sum(len(symbols) for symbols in self.symbols.values())

        # Calculate total file count and size
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

        # Convert bytes to human-readable format
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
        """
        Searches for symbols matching a pattern across the database.

        Args:
            pattern: Regex pattern to match symbols
            exchange: Optional filter for exchange
            security_type: Optional filter for security type
            timeframe: Optional filter for timeframe

        Returns:
            List[Tuple[str, str, str, str]]: List of tuples (exchange, security_type, timeframe, symbol)
        """
        results = []

        try:
            regex = re.compile(pattern, re.IGNORECASE)
        except re.error:
            # If the pattern is not a valid regex, treat it as a substring search
            pattern = re.escape(pattern)
            regex = re.compile(pattern, re.IGNORECASE)

        # Determine which exchanges to search
        exchanges_to_search = [exchange] if exchange else self.exchanges

        for exch in exchanges_to_search:
            if exch not in self.exchanges:
                continue

            # Determine which security types to search
            security_types_to_search = (
                [security_type] if security_type else self.security_types.get(exch, [])
            )

            for sec_type in security_types_to_search:
                security_key = (exch, sec_type)
                if security_key not in self.timeframes:
                    continue

                # Determine which timeframes to search
                timeframes_to_search = (
                    [timeframe] if timeframe else self.timeframes.get(security_key, [])
                )

                for tf in timeframes_to_search:
                    timeframe_key = (exch, sec_type, tf)
                    if timeframe_key not in self.symbols:
                        continue

                    # Search symbols
                    for symbol in self.symbols[timeframe_key]:
                        if regex.search(symbol):
                            results.append((exch, sec_type, tf, symbol))

        return sorted(results)


# Example usage
if __name__ == "__main__":
    db = ParquetDatabaseHandler("storage")

    daily_data = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=252, freq="B"),
            "open": [100.0 + i * 0.1 for i in range(252)],
            "high": [105.0 + i * 0.1 for i in range(252)],
            "low": [99.0 + i * 0.1 for i in range(252)],
            "close": [104.0 + i * 0.1 for i in range(252)],
            "volume": [1000 + i * 10 for i in range(252)],
        }
    )

    minute_data = pd.DataFrame(
        {
            "datetime": pd.date_range("2025-01-01 09:30:00", periods=390, freq="T"),
            "open": [100.0 + i * 0.01 for i in range(390)],
            "high": [101.0 + i * 0.01 for i in range(390)],
            "low": [99.5 + i * 0.01 for i in range(390)],
            "close": [100.5 + i * 0.01 for i in range(390)],
            "volume": [100 + i for i in range(390)],
        }
    )

    db.save_data("NYSE", "STOCK", "1D", "MSFT", daily_data)
    db.save_data("NYSE", "STOCK", "1min", "MSFT", minute_data)

    print("Available exchanges:", db.list_exchanges())
    print("NYSE security types:", db.list_security_types("NYSE"))
    print("NYSE/STOCK timeframes:", db.list_timeframes("NYSE", "STOCK"))
    print("NYSE/STOCK/1D symbols:", db.list_symbols("NYSE", "STOCK", "1D"))

    loaded_daily_data = db.load_data("NYSE", "STOCK", "1D", "AAPL")
    print("\nLoaded daily data sample:")
    print(loaded_daily_data.head())

    loaded_minute_data = db.load_data("NYSE", "STOCK", "1min", "AAPL")
    print("\nLoaded minute data sample:")
    print(loaded_minute_data.head())

    search_results = db.search_symbols("AA")
    print("\nSymbols matching 'AA':")
    for result in search_results:
        print(
            f"Exchange: {result[0]}, Security Type: {result[1]}, Timeframe: {result[2]}, Symbol: {result[3]}"
        )

    db_info = db.get_database_info()
    print("\nDatabase info:")
    for key, value in db_info.items():
        print(f"{key}: {value}")
