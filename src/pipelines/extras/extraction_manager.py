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
        
    async def get_extraction_date_range(self, ticker: str, interval: str) -> tuple:
        try:
            records = await self.db.get_by_column("ticker", ticker)
            
            matching_records = [r for r in records if r.successful and r.interval == interval]
            
            if not matching_records:
                return None, None
                
            oldest_start_date = min(matching_records, key=lambda x: x.start_date).start_date
            newest_end_date = max(matching_records, key=lambda x: x.end_date).end_date
            
            logger.info(f"Found date range for {ticker}: {oldest_start_date.date()} to {newest_end_date.date()}")
            return oldest_start_date, newest_end_date
            
        except Exception as e:
            logger.error(
                f"Error retrieving extraction date range for {ticker}: {e}"
            )
            return None, None

    def should_process_ticker(
        self, sym, start_dt, end_dt, interval
    ) -> bool:
        oldest_start, newest_end = run_async_task(
            self.get_extraction_date_range(sym, interval)
        )
        
        if oldest_start is None or newest_end is None:
            logger.info(f"Processing {sym}: no previous extractions found with matching interval")
            return True
            
        requested_start_date = start_dt.date()
        requested_end_date = end_dt.date()
        
        if requested_start_date >= oldest_start.date() and requested_end_date <= newest_end.date():
            logger.info(
                f"Skipping {sym}: already processed for the requested date range (within {oldest_start.date()} to {newest_end.date()})"
            )
            return False
        
        logger.info(
            f"Processing {sym}: requested date range not fully covered by existing extractions"
        )
        return True