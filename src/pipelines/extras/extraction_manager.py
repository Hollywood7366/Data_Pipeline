from datetime import datetime
from typing import Any, List

from src.models import TickerExtraction
from src.pipelines.extras.asyncer import run_async_task
from src.pipelines.extras.base import BaseDB
from utils.logging import Logger

logger = Logger(name="extraction-tracker", log_dir="data/logs")


class ExtractionManager:
    def __init__(self):
        self.db = BaseDB(TickerExtraction)

    async def record_extraction(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        interval: str,
        successful: bool,
        record_count: int = 0,
    ) -> None:
        try:
            if successful:
                extraction_data = {
                    "ticker": ticker,
                    "start_date": start_date,
                    "end_date": end_date,
                    "interval": interval,
                    "extraction_date": datetime.now().date(),
                    "successful": successful,
                    "record_count": record_count,
                }

                await self.db.create(extraction_data)
                logger.info(
                    f"Recorded successful extraction for {ticker} with {record_count} records"
                )
            else:
                logger.info(
                    f"Skipped recording failed extraction for {ticker}"
                )

        except Exception as e:
            logger.error(f"Error recording extraction for {ticker}: {e}")

    async def get_ticker_extractions(self, ticker: str) -> List[Any]:
        try:
            return await self.db.get_by_column("ticker", ticker)
        except Exception as e:
            logger.error(
                f"Error retrieving extraction records for {ticker}: {e}"
            )
            return []

    async def get_successful_extractions(self) -> List[Any]:
        try:
            return await self.db.get_by_column("successful", True)
        except Exception as e:
            logger.error(
                f"Error retrieving successful extraction records: {e}"
            )
            return []

    async def get_latest_extraction(self, ticker: str) -> Any:
        try:
            records = await self.db.get_by_column("ticker", ticker)
            if records:
                return sorted(
                    records, key=lambda x: x.extraction_date, reverse=True
                )[0]
            return None
        except Exception as e:
            logger.error(
                f"Error retrieving latest extraction for {ticker}: {e}"
            )
            return None

    def should_process_ticker(
        self, sym, start_dt, end_dt, interval
    ) -> bool:
        latest_extraction = run_async_task(self.get_latest_extraction(sym))

        if latest_extraction:
            same_dates = (
                latest_extraction.start_date.date() == start_dt.date()
                and latest_extraction.end_date.date() == end_dt.date()
            )
            same_interval = latest_extraction.interval == interval

            if (
                same_dates
                and same_interval
                and latest_extraction.successful
            ):
                logger.info(
                    f"Skipping {sym}: already successfully processed for the requested date range"
                )
                return False

        return True
