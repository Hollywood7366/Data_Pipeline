import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from utils.util import base_path


class ParquetDatabaseHandler:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.exchanges = set()
        self.security_types = {}
        self.equities = {}
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
                        key = (exchange, security_type)
                        self.equities[key] = set()

                        for equity_dir in security_type_dir.iterdir():
                            if equity_dir.is_dir():
                                equity = equity_dir.name
                                self.equities[key].add(equity)

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
        key = (exchange, security_type)
        if key not in self.equities:
            self.equities[key] = set()

        return security_type_dir

    def create_equity_directory(
        self, exchange: str, security_type: str, equity: str
    ) -> Path:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        equity = self._sanitize_name(equity)

        security_type_dir = self.create_security_type_directory(exchange, security_type)
        equity_dir = security_type_dir / equity

        if not equity_dir.exists():
            equity_dir.mkdir(parents=True)
            print(f"Created equity directory: {equity_dir}")

        key = (exchange, security_type)
        self.equities[key].add(equity)
        return equity_dir

    def save_data(
        self,
        exchange: str,
        security_type: str,
        equity: str,
        data: pd.DataFrame,
        year: Optional[Union[int, str]] = None,
    ) -> Path:
        if year is None:
            year = datetime.now().year

        year_str = str(year)

        equity_dir = self.create_equity_directory(exchange, security_type, equity)
        file_path = equity_dir / f"{year_str}.parquet"

        data.to_parquet(file_path)
        print(f"Saved data to: {file_path}")

        return file_path

    def load_data(
        self,
        exchange: str,
        security_type: str,
        equity: str,
        year: Optional[Union[str, int]] = None,
    ) -> pd.DataFrame:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        equity = self._sanitize_name(equity)

        equity_dir = self.base_path / exchange / security_type / equity

        if not equity_dir.exists():
            raise FileNotFoundError(
                f"No data found for {equity} ({security_type}) on {exchange}"
            )

        if year is None:
            parquet_files = list(equity_dir.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(
                    f"No data files found for {equity} ({security_type}) on {exchange}"
                )

            parquet_files.sort(key=lambda x: x.stem, reverse=True)
            file_path = parquet_files[0]
        else:
            year_str = str(year)
            file_path = equity_dir / f"{year_str}.parquet"

            if not file_path.exists():
                raise FileNotFoundError(
                    f"No data found for {equity} ({security_type}) on {exchange} for year {year_str}"
                )

        return pd.read_parquet(file_path)

    def get_available_years(
        self, exchange: str, security_type: str, equity: str
    ) -> List[str]:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        equity = self._sanitize_name(equity)

        equity_dir = self.base_path / exchange / security_type / equity

        if not equity_dir.exists():
            return []

        years = []
        for file_path in equity_dir.glob("*.parquet"):
            year_str = file_path.stem
            if self._is_valid_year_format(year_str):
                years.append(year_str)

        return sorted(years)

    def list_exchanges(self) -> List[str]:
        return sorted(list(self.exchanges))

    def list_security_types(self, exchange: str) -> List[str]:
        exchange = self._sanitize_name(exchange)

        if exchange not in self.security_types:
            return []

        return sorted(list(self.security_types[exchange]))

    def list_equities(self, exchange: str, security_type: str) -> List[str]:

        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)

        key = (exchange, security_type)
        if key not in self.equities:
            return []

        return sorted(list(self.equities[key]))

    def delete_data(
        self,
        exchange: str,
        security_type: str,
        equity: str,
        year: Optional[Union[str, int]] = None,
    ) -> bool:
        exchange = self._sanitize_name(exchange)
        security_type = self._sanitize_name(security_type)
        equity = self._sanitize_name(equity)

        equity_dir = self.base_path / exchange / security_type / equity

        if not equity_dir.exists():
            return False

        if year is None:
            shutil.rmtree(equity_dir)
            key = (exchange, security_type)
            if key in self.equities and equity in self.equities[key]:
                self.equities[key].remove(equity)
            return True
        else:
            year_str = str(year)
            file_path = equity_dir / f"{year_str}.parquet"

            if file_path.exists():
                file_path.unlink()
                return True

        return False

    def _sanitize_name(self, name: str) -> str:
        sanitized = re.sub(r"[^\w\-]", "_", name)
        return sanitized

    def _is_valid_year_format(self, year_str: str) -> bool:
        pattern = r"^\d{4}$"
        return bool(re.match(pattern, year_str))

    def get_database_info(self) -> Dict[str, Any]:
        exchange_count = len(self.exchanges)
        security_type_count = sum(
            len(sec_types) for sec_types in self.security_types.values()
        )
        equity_count = sum(len(equities) for equities in self.equities.values())

        file_count = 0
        total_size_bytes = 0

        for exchange in self.exchanges:
            exchange_dir = self.base_path / exchange
            for security_type in self.security_types.get(exchange, []):
                security_type_dir = exchange_dir / security_type
                key = (exchange, security_type)
                for equity in self.equities.get(key, []):
                    equity_dir = security_type_dir / equity
                    for file_path in equity_dir.glob("*.parquet"):
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
            "equities": equity_count,
            "files": file_count,
            "total_size": size_str,
            "last_updated": datetime.now().isoformat(),
        }


# # Example usage
# if __name__ == "__main__":
#     db = ParquetDatabaseHandler(f"{base_path()}/storage")

#     test_data = pd.DataFrame(
#         {
#             "date": pd.date_range("2025-01-01", periods=252, freq="B"),
#             "open": [100.0 + i * 0.1 for i in range(252)],
#             "high": [105.0 + i * 0.1 for i in range(252)],
#             "low": [99.0 + i * 0.1 for i in range(252)],
#             "close": [104.0 + i * 0.1 for i in range(252)],
#             "volume": [1000 + i * 10 for i in range(252)],
#         }
#     )

#     db.save_data("NYSE", "STOCK", "AAPL", test_data, 2025)
#     db.save_data("NYSE", "FUTURES", "AAPL", test_data, 2024)

#     print("Available exchanges:", db.list_exchanges())

#     print("NYSE security types:", db.list_security_types("NYSE"))

#     print("NYSE/STOCK equities:", db.list_equities("NYSE", "STOCK"))

#     loaded_data = db.load_data("NYSE", "STOCK", "AAPL", 2025)
#     print("\nLoaded data sample:")
#     print(loaded_data.head())

#     years = db.get_available_years("NYSE", "STOCK", "AAPL")
#     print("\nAvailable years for NYSE/STOCK/AAPL:", years)

#     db_info = db.get_database_info()
#     print("\nDatabase info:")
#     for key, value in db_info.items():
#         print(f"{key}: {value}")
