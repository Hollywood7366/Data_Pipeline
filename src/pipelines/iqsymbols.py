import time
import pandas as pd
import asyncio
from datetime import datetime
from schemas.historical import SYMBOLS_SCHEMA
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.logging import Logger
from src.pipelines.extras.base import QuestDBOperations

logger = Logger(name='iqfeed', log_dir='data/logs')

class DTNIQFeed:
    def __init__(self, headless=False, output_file="dtn_iqfeed_symbols.csv"):
        self.url = "https://ws1.dtn.com/IQ/Search/"
        self.output_file = output_file
        self.symbols_data = []
        self.current_page, self.total_records, self.total_records_extracted = 1, 0, 0
        self.records_per_page = 250
        self.options = Options()
        if headless: self.options.add_argument("--headless")
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument("--disable-notifications")
        self.driver = None
        self.db_operations = None

    def start_browser(self):
        self.driver = webdriver.Chrome(options=self.options)
        self.driver.get(self.url)
        WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.ID, "htmlTable")))
        time.sleep(2)

    async def set_db_operations(self, security_type):
        table_name = f"{security_type}_data" if security_type else "Default_data"
        schema = SYMBOLS_SCHEMA.get(security_type, SYMBOLS_SCHEMA['Default'])
        self.db_operations = QuestDBOperations(table_name, schema, create_if_not_exists=True)
        # await self.db_operations.truncate_table()
        await self.db_operations.initialize()
        time.sleep(2)
        logger.info(f"QuestDB table set: {table_name}")

    def perform_search(self, exchange=None, security_type=None, show_front_month=False, show_continuous=False, show_eminis=False, no_options=False, no_spreads=False):
        try:
            if exchange and exchange != "ALL": 
                self.driver.find_element(By.ID, "exchangeSelect").click()
                time.sleep(0.5)
                self.driver.find_element(By.XPATH, f"//option[contains(text(), '{exchange}')]").click()
                time.sleep(0.5)

            if security_type and security_type != "ALL": 
                self.driver.find_element(By.ID, "securityTypeSelect").click()
                time.sleep(0.5)
                self.driver.find_element(By.XPATH, f"//option[contains(text(), '{security_type}')]").click()
                time.sleep(0.5)

            for option, checkbox_id in [(show_front_month, "frontMonthOnly"), (show_continuous, "continuousOnly"), (show_eminis, "miniOnly"), (no_options, "noOptions"), (no_spreads, "noSpreads")]:
                if option:
                    checkbox = self.driver.find_element(By.ID, checkbox_id)
                    if not checkbox.is_selected(): checkbox.click()

            try:
                html_table_radio = self.driver.find_element(By.XPATH, "//input[@type='radio' and @value='htmlTable']")
                if not html_table_radio.is_selected(): html_table_radio.click()
            except Exception as e:
                logger.error(f"Could not select HTML Table option: {e}")

            WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.ID, "searchButton"))).click()
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "symbolTable")))
            time.sleep(3)
            records_text = self.driver.find_element(By.ID, "quantityHeader").text
            self.total_records = int(records_text.split("of")[1].strip().replace(',', '')) if "of" in records_text else 0
            logger.info(f"Found {self.total_records} total records.")
            return True
        except Exception as e:
            logger.error(f"Error performing search: {e}")
            return False

    def extract_current_page(self):
        try:
            time.sleep(3)
            rows = self.driver.find_element(By.ID, "symbolTable").find_elements(By.CSS_SELECTOR, "tbody tr")
            if not rows: return False
            page_data = [{"symbol": row.find_elements(By.TAG_NAME, "td")[0].text.strip(),
                        "description": row.find_elements(By.TAG_NAME, "td")[1].text.strip(),
                        "security_type": row.find_elements(By.TAG_NAME, "td")[2].text.strip(),
                        "exchange": row.find_elements(By.TAG_NAME, "td")[3].text.strip(),
                        "listed_market": row.find_elements(By.TAG_NAME, "td")[4].text.strip(),
                        "created_at": datetime.now()} for row in rows]
            logger.info(f"Extracted page data: {page_data}") 
            self.symbols_data.extend(page_data)
            self.total_records_extracted += len(page_data)
            logger.info(f"Extracted {len(page_data)} valid records from page {self.current_page}")
            return True
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            return False

    def go_to_next_page(self):
        try:
            if self.total_records_extracted >= self.total_records:
                logger.info("Reached the last page or extracted all records.")
                return False
            next_button = WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.ID, "nextButtonTop")))
            if next_button.is_enabled() and next_button.is_displayed():
                next_button.click()
                logger.info("Next button clicked")
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "symbolTable")))
            time.sleep(3)
            self.current_page += 1
            logger.info(f"Navigated to page {self.current_page}")
            return True
        except Exception as e:
            logger.error(f"Error navigating to next page: {e}")
            return False

    def save_to_csv(self):
        if not self.symbols_data: 
            logger.info("No data to save")
            return
        try:
            pd.DataFrame(self.symbols_data).to_csv(self.output_file, index=False)
            logger.info(f"Successfully saved {len(self.symbols_data)} records to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

    def sanitize_symbol(self, symbol:str):
        return symbol.replace('@', 'AT')

    async def save_to_db(self):
        if self.db_operations:
            for record in self.symbols_data:
                record['symbol'] = self.sanitize_symbol(record['symbol'])
                if isinstance(record['created_at'], str):
                    try:
                        record['created_at'] = datetime.strptime(record['created_at'], '%Y-%m-%d %H:%M:%S')
                    except Exception as e:
                        logger.error(f"Error parsing 'created_at': {e}")
                        continue
                elif not isinstance(record['created_at'], datetime):
                    logger.error(f"Invalid type for 'created_at': {type(record['created_at'])}")
                    continue

                await self.db_operations.insert_data(record)
            logger.info(f"Saved {len(self.symbols_data)} records to QuestDB")


    def close(self):
        if self.driver: 
            self.driver.quit()
            logger.info("Browser closed")

    async def run_complete_extraction(self, security_type):
        await self.set_db_operations(security_type)
        try:
            await self.extract_all_data()
            # await self.save_to_db()
            self.save_to_csv()
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
        finally:
            self.close()

    async def extract_all_data(self):
        if not self.perform_search(): 
            logger.error("Initial search failed")
            return
        if not self.extract_current_page():
            logger.error("Failed to extract first page")
            return
        page_count = 1
        while self.go_to_next_page():
            WebDriverWait(self.driver, 8).until(EC.presence_of_element_located((By.ID, "symbolTable")))
            time.sleep(3)
            if not self.extract_current_page():
                logger.error(f"Failed to extract data from page {self.current_page}")
                break
            page_count += 1
        logger.info(f"Extraction complete. Processed {page_count} pages. Total records extracted: {len(self.symbols_data)}")

# if __name__ == "__main__":
#     scraper = DTNIQFeed(headless=False, output_file="data/dtn_iqfeed_symbols_all.csv")
#     scraper.start_browser()
#     scraper.perform_search(show_front_month=True)
#     asyncio.run(scraper.run_complete_extraction(security_type="Futures"))
