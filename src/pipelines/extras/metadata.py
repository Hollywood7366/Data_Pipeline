from sqlalchemy import func

from src.models import SymbolMetadata
from src.pipelines.extras.base import BaseDB
from utils.CONSTANTS import SYMBOLS_METADATA_STATUS
from utils.logging import Logger

logger = Logger(name="iqfeed-symbols", log_dir="data/sheet_logs")


class MetadataManager:
    def __init__(self, model=SymbolMetadata):
        self.db = BaseDB(model)

    async def save_symbols_metadata(self, records):
        try:
            for record in records:
                symbol = record["symbol"]
                metadata = {
                    "symbol": symbol,
                    "count": 1,
                    "status": SYMBOLS_METADATA_STATUS[0],
                }

                existing_metadata = await self.db.get_by_column(
                    "symbol", symbol, unique=True
                )

                if existing_metadata:
                    existing_metadata.count += 1
                    existing_metadata.status = SYMBOLS_METADATA_STATUS[
                        1
                    ]
                    await self.db.update(existing_metadata)
                else:
                    await self.db.create(metadata)

            logger.info(f"Updated/Inserted metadata for symbols")
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")

    async def save_iqfeed_processed_metadata(
        self, symbol, records, period="daily"
    ):
        try:
            if not records:
                logger.warning(f"No records provided for symbol {symbol}")
                return

            record = records[0]

            metadata = {
                "symbol": symbol,
                "count": record["count"],
                "last_record_datetime": record["last_record_datetime"],
                "status": SYMBOLS_METADATA_STATUS[1],
                "max_high": record["max_high"],
                "min_low": record["min_low"],
                "total_volume": record["total_volume"],
                "average_close": record["average_close"],
                "last_processed_period": period,
            }

            existing_obj = await self.db.get_by_column(
                column="symbol", value=symbol, unique=True
            )

            if existing_obj:

                existing_obj.count += 1
                existing_obj.status = SYMBOLS_METADATA_STATUS[1]
                existing_obj.max_high = record["max_high"]
                existing_obj.min_low = record["min_low"]
                existing_obj.total_volume = record["total_volume"]
                existing_obj.average_close = record["average_close"]
                if (
                    isinstance(records, list)
                    and records
                    and isinstance(records[-1], dict)
                    and "DateTime" in records[-1]
                ):
                    existing_obj.last_record_datetime = records[-1][
                        "DateTime"
                    ]
                else:
                    existing_obj.last_record_datetime = record[
                        "last_record_datetime"
                    ]

                await self.db.update(existing_obj)
                logger.info(f"Updated metadata for symbol {symbol}")

            else:
                await self.db.create(metadata)
                logger.info(f"Inserted new metadata for symbol {symbol}")

        except Exception as e:
            logger.error(f"Error saving metadata for {symbol}: {e}")

    async def log_script_run(self, symbol_count, run_date=None):
        try:
            if not run_date:
                run_date = func.now()

            existing_obj = await self.db.get_by_column(
                "run_date", run_date, unique=True
            )

            if existing_obj:
                existing_obj.symbol_count += symbol_count
                existing_obj.run_count += 1
                await self.db.update(existing_obj)
                logger.info(
                    f"Updated metadata for script run on {run_date}, successful symbols: {symbol_count}"
                )
            else:
                new_metadata = {
                    "run_date": run_date,
                    "symbol_count": symbol_count,
                    "run_count": 1,
                }
                await self.db.create(new_metadata)
                logger.info(
                    f"Inserted metadata for script run on {run_date}, successful symbols: {symbol_count}"
                )

        except Exception as e:
            logger.error(f"Error logging script run metadata: {e}")
