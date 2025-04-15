from datetime import datetime, timedelta
import os
import subprocess
import sys
import time
import traceback
import asyncio
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable, XCom
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException

from utils.atomic_creator import AtomicFileUpdate
from utils.emails import send_dag_failure_email, send_dag_success_email
from utils.logging import Logger
import polars as pl
from dotenv import load_dotenv
load_dotenv(dotenv_path="/opt/airflow/.env") 

logger = Logger(name="dtn_iqfeed_scraper", log_dir="/opt/airflow/logs")

BATCH_SIZE = int(Variable.get('BATCH_SIZE_FOR_SCRAPER'))
STATE_FILE = "/opt/airflow/data/DTN_SYMBOLS/scraper_state.json"


def setup_display():
    """Set up a virtual display for headless browser operation"""
    subprocess.Popen(["Xvfb", ":99", "-screen", "0", "1920x1080x24"])
    os.environ["DISPLAY"] = ":99"
    return True


def setup_chrome_driver():
    """Configure and initialize Chrome WebDriver"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.binary_location = "/usr/bin/google-chrome"
    
    service = Service()
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    
    return driver


def enhanced_perform_search(
    driver,
    exchange=None,
    security_type=None,
    show_front_month=False,
    show_continuous=False,
    show_eminis=False,
    no_options=False,
    no_spreads=False,
):
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "htmlTable"))
        )

        if exchange and exchange != "ALL":
            driver.find_element(By.ID, "exchangeSelect").click()
            time.sleep(1)
            driver.find_element(
                By.XPATH, f"//option[contains(text(), '{exchange}')]"
            ).click()
            time.sleep(1)

        if security_type and security_type != "ALL":
            driver.find_element(By.ID, "securityTypeSelect").click()
            time.sleep(1)
            driver.find_element(
                By.XPATH, f"//option[contains(text(), '{security_type}')]"
            ).click()
            time.sleep(1)

        handle_checkbox_options(
            driver, 
            show_front_month, 
            show_continuous, 
            show_eminis, 
            no_options, 
            no_spreads
        )
        select_html_table_format(driver)

        search_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "searchButton"))
        )
        search_button.click()

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "symbolTable"))
        )
        time.sleep(5)

        total_records = extract_record_count(driver)
        logger.info(f"Found {total_records} total records.")
        
        return total_records, True

    except Exception as e:
        logger.error(f"Error performing search: {e}")
        log_exception_details()
        
        screenshot_path = "/opt/airflow/logs/search_error.png"
        try:
            driver.save_screenshot(screenshot_path)
            logger.info(f"Saved error screenshot to {screenshot_path}")
        except:
            pass
            
        return 0, False


def handle_checkbox_options(
    driver, 
    show_front_month, 
    show_continuous, 
    show_eminis, 
    no_options, 
    no_spreads
):
    for option, checkbox_id in [
        (show_front_month, "frontMonthOnly"),
        (show_continuous, "continuousOnly"),
        (show_eminis, "miniOnly"),
        (no_options, "noOptions"),
        (no_spreads, "noSpreads"),
    ]:
        if option:
            try:
                checkbox = driver.find_element(By.ID, checkbox_id)
                if not checkbox.is_selected():
                    checkbox.click()
                    time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Could not interact with checkbox {checkbox_id}: {e}")


def select_html_table_format(driver):
    """Select HTML table format for results"""
    try:
        time.sleep(15)
        try:
            html_table_radio = driver.find_element(
                By.XPATH, "//input[@type='radio' and @value='htmlTable']"
            )
            if not html_table_radio.is_selected():
                html_table_radio.click()
                logger.info("Selected HTML Table using standard XPath")
        except NoSuchElementException:
            try:
                html_table_radio = driver.find_element(By.ID, "htmlTable")
                if not html_table_radio.is_selected():
                    html_table_radio.click()
                    logger.info("Selected HTML Table using ID")
            except NoSuchElementException:
                try:
                    driver.execute_script(
                        """
                        var radios = document.querySelectorAll('input[type="radio"]');
                        for(var i=0; i<radios.length; i++) {
                            if(radios[i].value == 'htmlTable') {
                                radios[i].click();
                                break;
                            }
                        }
                        """
                    )
                    logger.info("Selected HTML Table using JavaScript")
                except Exception as js_error:
                    logger.warning(f"JavaScript selection failed: {js_error}")
                    
                    try:
                        label = driver.find_element(
                            By.XPATH, "//label[contains(text(), 'HTML Table')]"
                        )
                        label.click()
                        logger.info("Selected HTML Table via label")
                    except Exception as label_error:
                        logger.warning(f"Label selection failed: {label_error}")
                        logger.info("Continuing without explicit HTML Table selection")
    except Exception as radio_error:
        logger.warning(f"All HTML Table selection approaches failed: {radio_error}")


def extract_record_count(driver):
    try:
        records_text = driver.find_element(By.ID, "quantityHeader").text
        total_records = (
            int(records_text.split("of")[1].strip().replace(",", ""))
            if "of" in records_text
            else 0
        )
        return total_records
    except Exception as qty_error:
        logger.warning(f"Could not extract record count: {qty_error}")
        return 1000


def log_exception_details():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    logger.error("Exception traceback:")
    for line in traceback.format_exception(exc_type, exc_value, exc_traceback):
        logger.error(line.rstrip())


def get_variable_boolean(var_name, default=False):
    try:
        value = Variable.get(var_name)
        return value.lower() == "true"
    except:
        return default


def get_variable_string(var_name, default=None):
    try:
        return Variable.get(var_name)
    except:
        return default


def save_state(state_data):
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        
        with open(STATE_FILE, 'w') as f:
            json.dump(state_data, f)
        logger.info(f"State saved to {STATE_FILE}")
        return True
    except Exception as e:
        logger.error(f"Failed to save state: {e}")
        return False


def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            logger.info(f"Loaded state: {state}")
            return state
        else:
            logger.info("No previous state found, starting fresh")
            return {
                "current_page": 1,
                "total_records": 0,
                "processed_records": 0,
                "batch_number": 1,
                "complete": False
            }
    except Exception as e:
        logger.error(f"Failed to load state: {e}")
        return {
            "current_page": 1,
            "total_records": 0,
            "processed_records": 0,
            "batch_number": 1,
            "complete": False
        }


def go_to_specific_page(driver, page_number):
    try:
        if page_number == 1:
            return True
        
        logger.info(f"Attempting to navigate to page {page_number}")
        
        try:
            page_input = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "pageNumberInput"))
            )
            page_input.clear()
            page_input.send_keys(str(page_number))
            
            go_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "goButton"))
            )
            go_button.click()
            
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "symbolTable"))
            )
            time.sleep(3)
            
            logger.info(f"Successfully navigated to page {page_number}")
            return True
        except Exception as e:
            logger.warning(f"Direct page navigation failed: {e}")
            
            current_page = 1
            while current_page < page_number:
                next_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "nextButtonTop"))
                )
                if next_button.is_enabled() and next_button.is_displayed():
                    next_button.click()
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "symbolTable"))
                    )
                    time.sleep(2)
                    current_page += 1
                    logger.info(f"Sequential navigation: Now on page {current_page}")
                else:
                    logger.error("Next button not available")
                    return False
            
            return current_page == page_number
            
    except Exception as e:
        logger.error(f"Failed to navigate to page {page_number}: {e}")
        return False


def extract_page_data(driver):
    try:
        time.sleep(3)
        rows = driver.find_element(By.ID, "symbolTable").find_elements(By.CSS_SELECTOR, "tbody tr")
        if not rows:
            logger.warning("No rows found on current page")
            return []
            
        page_data = []
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 5:
                record = {
                    "symbol": cells[0].text.strip(),
                    "description": cells[1].text.strip(),
                    "security_type": cells[2].text.strip(),
                    "exchange": cells[3].text.strip(),
                    "listed_market": cells[4].text.strip(),
                    "created_at": datetime.now(),
                }
                page_data.append(record)
        
        logger.info(f"Extracted {len(page_data)} records from current page")
        return page_data
    except Exception as e:
        logger.error(f"Failed to extract page data: {e}")
        return []


def save_batch_to_parquet(data, output_file, batch_num, first_batch=False):
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        df = pl.DataFrame(data)
        
        if first_batch or not os.path.exists(output_file):
            atomic_update = AtomicFileUpdate(
                output_file, f"{output_file}.tmp"
            )
            atomic_update.perform_atomic_update(df)
            logger.info(f"Created new parquet file with {len(data)} records")
        else:
            existing_df = pl.read_parquet(output_file)
            combined_df = pl.concat([existing_df, df])
            atomic_update = AtomicFileUpdate(
                output_file, f"{output_file}.tmp"
            )
            atomic_update.perform_atomic_update(combined_df)
            logger.info(f"Appended {len(data)} records to existing parquet file")
            
        backup_file = output_file.replace(".parquet", f"_backup.parquet")
        
        if first_batch or not os.path.exists(backup_file):
            df.write_parquet(backup_file)
        else:
            backup_df = pl.read_parquet(backup_file)
            combined_backup_df = pl.concat([backup_df, df])
            combined_backup_df.write_parquet(backup_file)
            
        logger.info(f"Updated backup file with batch {batch_num} data")
        
        return True
    except Exception as e:
        logger.error(f"Failed to save batch to parquet: {e}")
        return False


async def save_batch_to_db(data, db_manager):
    try:
        if not data:
            logger.info("No data to save to database")
            return True
            
        if not hasattr(db_manager, 'metadata_manager'):
            from src.pipelines.extras.metadata import MetadataManager
            db_manager.metadata_manager = MetadataManager()
            logger.info("Initialized MetadataManager")

        existing_symbols_raw = await db_manager.db.get_unique_column("symbol")
        existing_symbols = set(
            s.symbol if hasattr(s, "symbol") else s for s in existing_symbols_raw
        )
        
        new_records = []
        for record in data:
            symbol = record["symbol"]
            if symbol not in existing_symbols:
                new_records.append(record)
                existing_symbols.add(symbol) 
        
        if not new_records:
            logger.info("No new symbols to insert in this batch")
            return True
            
        await db_manager.db.bulk_insert(new_records)
        logger.info(f"Inserted {len(new_records)} new records into the database")
        
        await db_manager.metadata_manager.save_symbols_metadata(new_records)
        logger.info(f"Updated metadata for {len(new_records)} symbols")
        
        return True
    except Exception as e:
        logger.error(f"Failed to save batch to database: {e}")
        return False


def navigate_to_next_page(driver):
    try:
        next_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "nextButtonTop"))
        )
        
        if next_button.is_enabled() and next_button.is_displayed():
            next_button.click()
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "symbolTable"))
            )
            time.sleep(3)
            logger.info("Successfully navigated to next page")
            return True
        else:
            logger.info("Next button not enabled, likely reached the end of results")
            return False
    except Exception as e:
        logger.error(f"Failed to navigate to next page: {e}")
        return False


async def process_batch(
    state,
    driver,
    db_manager,
    output_file,
    model_file_pattern,
    search_params
):
    try:
        batch_data = []
        current_page = state["current_page"]
        first_page_in_batch = current_page
        records_in_batch = 0
        batch_size = min(BATCH_SIZE, state["total_records"] - state["processed_records"])
        
        logger.info(f"Starting batch {state['batch_number']} from page {current_page}")
        logger.info(f"Target batch size: {batch_size} records")
        
        if current_page > 1:
            success = go_to_specific_page(driver, current_page)
            if not success:
                logger.error(f"Failed to navigate to starting page {current_page}")
                return False
        
        while records_in_batch < batch_size:
            page_data = extract_page_data(driver)
            if not page_data:
                logger.warning(f"No data found on page {current_page}, may have reached the end")
                if state["processed_records"] >= state["total_records"]:
                    state["complete"] = True
                    logger.info(f"All records processed ({state['processed_records']}/{state['total_records']}). Marking as complete.")
                else:
                    logger.error(f"No data found but only processed {state['processed_records']}/{state['total_records']} records.")
                    state["complete"] = False
                break
                
            batch_data.extend(page_data)
            records_in_batch += len(page_data)
            state["processed_records"] += len(page_data)
            
            logger.info(f"Added {len(page_data)} records from page {current_page}")
            logger.info(f"Batch progress: {records_in_batch}/{batch_size} records")
            logger.info(f"Total progress: {state['processed_records']}/{state['total_records']} records")
            
            state["current_page"] = current_page
            save_state(state)
            
            if state["processed_records"] >= state["total_records"]:
                logger.info("Reached total record count, marking as complete")
                state["complete"] = True
                break
                
            if records_in_batch < batch_size:
                success = navigate_to_next_page(driver)
                if not success:
                    logger.warning("Could not navigate to next page, may have reached the end")
                    if state["processed_records"] >= state["total_records"]:
                        state["complete"] = True
                        logger.info(f"Navigation ended after processing all {state['processed_records']} records. Marking as complete.")
                    else:
                        logger.error(f"Navigation ended but only processed {state['processed_records']}/{state['total_records']} records.")
                        state["complete"] = False
                    break
                current_page += 1
                state["current_page"] = current_page
        
        first_batch = state["batch_number"] == 1 and first_page_in_batch == 1
        if batch_data:
            actual_output_file = determine_output_file(model_file_pattern, search_params)
            
            save_batch_to_parquet(batch_data, actual_output_file, state["batch_number"], first_batch)
            await save_batch_to_db(batch_data, db_manager)
            
            state["batch_number"] += 1
            state["current_page"] = current_page
            save_state(state)
            
            logger.info(f"Completed batch {state['batch_number']-1} with {len(batch_data)} records")
            return True
        else:
            logger.warning("No data collected in this batch")
            if state["processed_records"] >= state["total_records"]:
                state["complete"] = True
                logger.info("No data in this batch, but all records have been processed. Marking as complete.")
            else:
                state["complete"] = False
                logger.warning(f"No data in this batch and only processed {state['processed_records']}/{state['total_records']} records.")
            save_state(state)
            return False
            
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        log_exception_details()
        return False

def determine_output_file(base_pattern, search_params):
    output_file = base_pattern
    
    if search_params.get("show_front_month"):
        output_file = output_file.replace(".parquet", "_frontmonth.parquet")
    elif search_params.get("show_continuous"):
        output_file = output_file.replace(".parquet", "_continuous.parquet")
    elif search_params.get("show_eminis"):
        output_file = output_file.replace(".parquet", "_eminis.parquet")
    elif search_params.get("no_options"):
        output_file = output_file.replace(".parquet", "_nooptions.parquet")
    elif search_params.get("no_spreads"):
        output_file = output_file.replace(".parquet", "_nospreads.parquet")
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    return output_file


async def run_batch_extraction(
    scraper,
    driver,
    search_params,
    output_file
):
    scraper.select_model_and_filename(
        search_params.get("show_front_month", False),
        search_params.get("show_continuous", False),
        search_params.get("show_eminis", False),
        search_params.get("no_options", False),
        search_params.get("no_spreads", False),
    )
    
    state = load_state()
    
    if state["current_page"] == 1 and state["total_records"] == 0:
        total_records, success = enhanced_perform_search(
            driver,
            exchange=search_params.get("exchange"),
            security_type=search_params.get("security_type"),
            show_front_month=search_params.get("show_front_month", False),
            show_continuous=search_params.get("show_continuous", False),
            show_eminis=search_params.get("show_eminis", False),
            no_options=search_params.get("no_options", False),
            no_spreads=search_params.get("no_spreads", False),
        )
        
        if not success:
            raise Exception("Initial search failed")
            
        state["total_records"] = total_records
        save_state(state)
    else:
        logger.info(f"Resuming from previous state: page {state['current_page']}, {state['processed_records']}/{state['total_records']} records processed")
        
        _, success = enhanced_perform_search(
            driver,
            exchange=search_params.get("exchange"),
            security_type=search_params.get("security_type"),
            show_front_month=search_params.get("show_front_month", False),
            show_continuous=search_params.get("show_continuous", False),
            show_eminis=search_params.get("show_eminis", False),
            no_options=search_params.get("no_options", False),
            no_spreads=search_params.get("no_spreads", False),
        )
        
        if not success:
            raise Exception("Failed to resume search")
    
    while not state.get("complete", False):
        success = await process_batch(
            state,
            driver,
            scraper,
            output_file,
            output_file,
            search_params
        )
        
        if not success:
            logger.warning("Batch processing failed or complete")
            break
    
    if state["processed_records"] >= state["total_records"]:
        state["complete"] = True
        logger.info(f"Extraction truly complete. Processed all {state['total_records']} records.")
    else:
        state["complete"] = False
        logger.warning(f"Extraction incomplete. Only processed {state['processed_records']} out of {state['total_records']} records.")
    
    save_state(state)
    
    return state["processed_records"]

def run_scraper(**kwargs):
    from src.pipelines.iqsymbols import DTNIQFeed
    
    setup_display()
    
    output_file = "/opt/airflow/data/DTN_SYMBOLS/dtn_symbols.parquet"
    scraper = DTNIQFeed(output_file=output_file)
    
    try:
        driver = setup_chrome_driver()
        scraper.driver = driver
        scraper.driver.get(scraper.url)
        time.sleep(20)
        
        search_params = {
            "exchange": get_variable_string('EXCHANGE'),
            "security_type": get_variable_string('SECURITY_TYPE'),
            "show_front_month": get_variable_boolean('SHOW_FRONT_MONTH'),
            "show_continuous": get_variable_boolean('SHOW_CONTINUOUS'),
            "show_eminis": get_variable_boolean('SHOW_EMINIS'),
            "no_options": get_variable_boolean('NO_OPTIONS'),
            "no_spreads": get_variable_boolean('NO_SPREADS'),
        }
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            total_processed = loop.run_until_complete(
                run_batch_extraction(
                    scraper,
                    driver,
                    search_params,
                    output_file
                )
            )
            
            completion_status = load_state()
            total_records = completion_status.get("total_records", 0)
            
            if completion_status.get("complete", False) and total_processed >= total_records:
                return f"DTN symbol scraping completed successfully. Processed {total_processed} out of {total_records} records."
            else:
                percentage = (total_processed / total_records * 100) if total_records > 0 else 0
                return f"DTN symbol scraping incomplete. Processed {total_processed} out of {total_records} records ({percentage:.2f}%)."
        finally:
            loop.close()
    
    except Exception as e:
        logger.error(f"DTN symbol scraping failed: {str(e)}")
        
        if hasattr(scraper, "driver") and scraper.driver:
            screenshot_path = "/opt/airflow/logs/final_error.png"
            try:
                scraper.driver.save_screenshot(screenshot_path)
                logger.info(f"Saved final error screenshot to {screenshot_path}")
            except:
                pass
        
        raise Exception(f"DTN symbol scraping failed: {str(e)}")
    finally:
        if hasattr(scraper, "driver") and scraper.driver:
            try:
                scraper.driver.quit()
                logger.info("Browser closed")
            except:
                pass


def scrape_dtn_symbols(**kwargs):
    try:
        return run_scraper(**kwargs)
    except Exception as outer_e:
        logger.error(f"Scraper setup failed: {str(outer_e)}")
        log_exception_details()
        raise Exception(f"Setup failed: {str(outer_e)}")

def reset_scraper_state(**kwargs):
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            
            total_records = state.get("total_records", 0)
            processed_records = state.get("processed_records", 0)
            
            if state.get("complete", False) and processed_records >= total_records:
                os.remove(STATE_FILE)
                logger.info(f"Scraping completed successfully. All {processed_records} records processed. State file removed.")
                return "Scraper state file removed - all records processed successfully"
            else:
                remaining = total_records - processed_records
                progress_pct = (processed_records / total_records * 100) if total_records > 0 else 0
                
                logger.info(f"Scraping incomplete. State file preserved for resumption. "
                          f"Progress: {processed_records}/{total_records} records ({progress_pct:.2f}%). "
                          f"Remaining: {remaining} records.")
                return f"Scraping incomplete ({progress_pct:.2f}%). State file preserved for resumption."
        else:
            return "No state file found. Nothing to remove."
    except Exception as e:
        logger.error(f"Error checking state file: {e}")
        log_exception_details()
        return f"Failed to process state file: {e}"

def inspect_state_file(**kwargs):
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            
            total_records = state.get("total_records", 0)
            processed_records = state.get("processed_records", 0)
            current_page = state.get("current_page", 1)
            complete_status = state.get("complete", False)
            batch_num = state.get("batch_number", 1)
            
            progress_pct = (processed_records / total_records * 100) if total_records > 0 else 0
            remaining = total_records - processed_records
            
            report = (f"STATE FILE DIAGNOSTIC REPORT:\n"
                     f"Complete flag: {complete_status}\n"
                     f"Total records: {total_records}\n"
                     f"Processed records: {processed_records}\n"
                     f"Remaining records: {remaining}\n"
                     f"Progress: {progress_pct:.2f}%\n"
                     f"Current page: {current_page}\n"
                     f"Batch number: {batch_num}\n"
                     f"Full state: {state}")
            
            logger.info(report)
            return report
        else:
            return "No state file found to inspect."
    except Exception as e:
        logger.error(f"Failed to inspect state file: {e}")
        return f"Error inspecting state file: {e}"

def should_continue(**kwargs):
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
            return not state.get("complete", False)
        return False
    except Exception as e:
        logger.error(f"Error checking state file for loop logic: {e}")
        return False


default_args = {
    "owner": "Sarim Sikander",
    "start_date": datetime(2025, 4, 1),
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "email": "sarimsikander24@gmail.com",
    "on_failure_callback": send_dag_failure_email,
    "on_success_callback": send_dag_success_email,
    "retries": 3,  
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=f"DTN_IQFEED_BATCH_SCRAPER_{os.getenv('DTN_IQFEED_BATCH_SCRAPER')}",
    default_args=default_args,
    description="Scrape symbols from DTN IQFeed using Selenium with batch processing",
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["scraping", "dtn", "iqfeed", "symbols", "batch",f"pipeline_version:{os.getenv('PIPELINE_VERSION')}"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_dtn_symbols",
        python_callable=scrape_dtn_symbols,
        provide_context=True,
    )
    
    reset_state_task = PythonOperator(
        task_id="reset_scraper_state",
        python_callable=reset_scraper_state,
        provide_context=True,
        trigger_rule="all_done", 
    )
    
    inspect_state_task = PythonOperator(
        task_id="inspect_state_file",
        python_callable=inspect_state_file,
        provide_context=True,
    )

    check_complete_task = ShortCircuitOperator(
        task_id="check_if_should_continue",
        python_callable=should_continue,
        provide_context=True,
        trigger_rule="all_done",
    )

    trigger_self_task = TriggerDagRunOperator(
        task_id="trigger_self_if_not_complete",
        trigger_dag_id="DTN_IQFEED_BATCH_SCRAPER_V1.0.0",
        wait_for_completion=False,
        reset_dag_run=False,
        trigger_rule="all_done",
    )
    
    scrape_task >> inspect_state_task >> reset_state_task >> check_complete_task >> trigger_self_task