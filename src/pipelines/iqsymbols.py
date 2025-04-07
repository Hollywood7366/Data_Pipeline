import time
from datetime import datetime

import chromedriver_autoinstaller
import polars as pl
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.models import (
    IqfeedSymbolsAll,
    IqfeedSymbolsContinuousContracts,
    IqfeedSymbolsEminis,
    IqfeedSymbolsFrontMonth,
    IqfeedSymbolsNoOptions,
    IqfeedSymbolsNoSpreads,
)
from src.pipelines.extras.base import BaseDB
from utils.logging import Logger
from utils.util import base_path

logger = Logger(name="iqfeed", log_dir="data/logs")


class DTNIQFeed:
    def __init__(self, output_file="dtn_iqfeed_symbols.parquet"):
        chromedriver_autoinstaller.install()
        self.url = "https://ws1.dtn.com/IQ/Search/"
        self.output_file = output_file
        self.symbols_data = []
        self.current_page, self.total_records, self.total_records_extracted = 1, 0, 0
        self.records_per_page = 250
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.driver = None
        self.db = None
        self.model_name = "IqfeedSymbolsAll"

    def start_browser(self):
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.get(self.url)
        time.sleep(15)
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.ID, "htmlTable"))
        )
        time.sleep(2)

    def select_model_and_filename(
        self,
        show_front_month=False,
        show_continuous=False,
        show_eminis=False,
        no_options=False,
        no_spreads=False,
    ):
        if show_front_month:
            self.db = BaseDB(IqfeedSymbolsFrontMonth, local=True)
            self.model_name = "IqfeedSymbolsFrontMonth"
        elif show_continuous:
            self.db = BaseDB(IqfeedSymbolsContinuousContracts, local=True)
            self.model_name = "IqfeedSymbolsContinuousContracts"
        elif show_eminis:
            self.db = BaseDB(IqfeedSymbolsEminis, local=True)
            self.model_name = "IqfeedSymbolsEminis"
        elif no_options:
            self.db = BaseDB(IqfeedSymbolsNoOptions, local=True)
            self.model_name = "IqfeedSymbolsNoOptions"
        elif no_spreads:
            self.db = BaseDB(IqfeedSymbolsNoSpreads, local=True)
            self.model_name = "IqfeedSymbolsNoSpreads"
        else:
            self.db = BaseDB(IqfeedSymbolsAll, local=True)
            self.model_name = "IqfeedSymbolsAll"

        self.output_file = (
            f"{base_path()}/data/DTN_SYMBOLS/dtn_{self.model_name.lower()}.parquet"
        )

    def perform_search(
        self,
        exchange=None,
        security_type=None,
        show_front_month=False,
        show_continuous=False,
        show_eminis=False,
        no_options=False,
        no_spreads=False,
    ):
        try:
            if exchange and exchange != "ALL":
                self.driver.find_element(By.ID, "exchangeSelect").click()
                time.sleep(0.5)
                self.driver.find_element(
                    By.XPATH, f"//option[contains(text(), '{exchange}')]"
                ).click()
                time.sleep(0.5)

            if security_type and security_type != "ALL":
                self.driver.find_element(By.ID, "securityTypeSelect").click()
                time.sleep(0.5)
                self.driver.find_element(
                    By.XPATH, f"//option[contains(text(), '{security_type}')]"
                ).click()
                time.sleep(0.5)

            for option, checkbox_id in [
                (show_front_month, "frontMonthOnly"),
                (show_continuous, "continuousOnly"),
                (show_eminis, "miniOnly"),
                (no_options, "noOptions"),
                (no_spreads, "noSpreads"),
            ]:
                if option:
                    checkbox = self.driver.find_element(By.ID, checkbox_id)
                    if not checkbox.is_selected():
                        checkbox.click()

            try:
                html_table_radio = self.driver.find_element(
                    By.XPATH, "//input[@type='radio' and @value='htmlTable']"
                )
                if not html_table_radio.is_selected():
                    html_table_radio.click()
            except Exception as e:
                logger.error(f"Could not select HTML Table option: {e}")

            WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, "searchButton"))
            ).click()
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "symbolTable"))
            )
            time.sleep(3)
            records_text = self.driver.find_element(By.ID, "quantityHeader").text
            self.total_records = (
                int(records_text.split("of")[1].strip().replace(",", ""))
                if "of" in records_text
                else 0
            )
            logger.info(f"Found {self.total_records} total records.")
            return True
        except Exception as e:
            logger.error(f"Error performing search: {e}")
            return False

    def extract_current_page(self):
        try:
            time.sleep(3)
            rows = self.driver.find_element(By.ID, "symbolTable").find_elements(
                By.CSS_SELECTOR, "tbody tr"
            )
            if not rows:
                return False
            page_data = [
                {
                    "symbol": row.find_elements(By.TAG_NAME, "td")[0].text.strip(),
                    "description": row.find_elements(By.TAG_NAME, "td")[1].text.strip(),
                    "security_type": row.find_elements(By.TAG_NAME, "td")[
                        2
                    ].text.strip(),
                    "exchange": row.find_elements(By.TAG_NAME, "td")[3].text.strip(),
                    "listed_market": row.find_elements(By.TAG_NAME, "td")[
                        4
                    ].text.strip(),
                    "created_at": datetime.now(),
                }
                for row in rows
            ]
            self.symbols_data.extend(page_data)
            self.total_records_extracted += len(page_data)
            logger.info(
                f"Extracted {len(page_data)} valid records from page {self.current_page}"
            )
            return True
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            return False

    def go_to_next_page(self):
        try:
            if self.total_records_extracted >= self.total_records:
                logger.info("Reached the last page or extracted all records.")
                return False
            next_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, "nextButtonTop"))
            )
            if next_button.is_enabled() and next_button.is_displayed():
                next_button.click()
                logger.info("Next button clicked")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "symbolTable"))
            )
            time.sleep(3)
            self.current_page += 1
            logger.info(f"Navigated to page {self.current_page}")
            return True
        except Exception as e:
            logger.error(f"Error navigating to next page: {e}")
            return False

    def save_to_parquet(self):
        if not self.symbols_data:
            logger.info("No data to save")
            return
        try:
            pl.DataFrame(self.symbols_data).write_parquet(self.output_file)
            logger.info(
                f"Successfully saved {len(self.symbols_data)} records to {self.output_file}"
            )
        except Exception as e:
            logger.error(f"Error saving to Parquet: {e}")

    def close(self):
        if self.driver:
            self.driver.quit()
            logger.info("Browser closed")

    async def save_to_db(self):
        if not self.symbols_data:
            logger.info("No data to save to database")
            return

        try:
            existing_symbols_raw = await self.db.get_unique_column("symbol")
            existing_symbols = set(
                s.symbol if hasattr(s, "symbol") else s for s in existing_symbols_raw
            )

            new_records = []
            for record in self.symbols_data:
                symbol = record["symbol"]
                if symbol not in existing_symbols:
                    record["symbol"] = symbol
                    new_records.append(record)

            if not new_records:
                logger.info("No new symbols to insert.")
                return

            await self.db.bulk_insert(new_records)
            logger.info(f"Inserted {len(new_records)} new records into the database.")

            pl.DataFrame(new_records).write_parquet(self.output_file)
            logger.info(
                f"Saved {len(new_records)} new records to Parquet at {self.output_file}"
            )

        except Exception as e:
            logger.error(f"Error saving to database or parquet: {e}")

    async def run_complete_extraction(
        self,
        show_front_month=False,
        show_continuous=False,
        show_eminis=False,
        no_options=False,
        no_spreads=False,
    ):
        try:
            await self.extract_all_data(
                show_front_month, show_continuous, show_eminis, no_options, no_spreads
            )
            await self.save_to_db()
            self.save_to_parquet()
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
        finally:
            self.close()

    async def extract_all_data(
        self,
        show_front_month=False,
        show_continuous=False,
        show_eminis=False,
        no_options=False,
        no_spreads=False,
    ):
        self.select_model_and_filename(
            show_front_month, show_continuous, show_eminis, no_options, no_spreads
        )
        if not self.perform_search():
            logger.error("Initial search failed")
            return
        if not self.extract_current_page():
            logger.error("Failed to extract first page")
            return
        page_count = 1
        while self.go_to_next_page():
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.ID, "symbolTable"))
            )
            time.sleep(3)
            if not self.extract_current_page():
                logger.error(f"Failed to extract data from page {self.current_page}")
                break
            page_count += 1
        logger.info(
            f"Extraction complete. Processed {page_count} pages. Total records extracted: {len(self.symbols_data)}"
        )
