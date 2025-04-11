from datetime import datetime
import os
import subprocess
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from utils.emails import send_dag_failure_email, send_dag_success_email

def setup_display():
    subprocess.Popen(["Xvfb", ":99", "-screen", "0", "1920x1080x24"])
    os.environ["DISPLAY"] = ":99"
    return True


def scrape_dtn_symbols(**kwargs):
    import sys
    import traceback
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    setup_display()

    try:
        from src.pipelines.iqsymbols import DTNIQFeed

        def enhanced_perform_search(
            self,
            exchange=None,
            security_type=None,
            show_front_month=False,
            show_continuous=False,
            show_eminis=False,
            no_options=False,
            no_spreads=False,
        ):
            from utils.logging import Logger

            logger = Logger(name="iqfeed", log_dir="data/logs")
            from selenium.common.exceptions import (
                TimeoutException,
                NoSuchElementException,
            )

            try:
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.ID, "htmlTable"))
                )

                if exchange and exchange != "ALL":
                    self.driver.find_element(
                        By.ID, "exchangeSelect"
                    ).click()
                    time.sleep(1)
                    self.driver.find_element(
                        By.XPATH,
                        f"//option[contains(text(), '{exchange}')]",
                    ).click()
                    time.sleep(1)

                if security_type and security_type != "ALL":
                    self.driver.find_element(
                        By.ID, "securityTypeSelect"
                    ).click()
                    time.sleep(1)
                    self.driver.find_element(
                        By.XPATH,
                        f"//option[contains(text(), '{security_type}')]",
                    ).click()
                    time.sleep(1)

                for option, checkbox_id in [
                    (show_front_month, "frontMonthOnly"),
                    (show_continuous, "continuousOnly"),
                    (show_eminis, "miniOnly"),
                    (no_options, "noOptions"),
                    (no_spreads, "noSpreads"),
                ]:
                    if option:
                        try:
                            checkbox = self.driver.find_element(
                                By.ID, checkbox_id
                            )
                            if not checkbox.is_selected():
                                checkbox.click()
                                time.sleep(0.5)
                        except Exception as e:
                            logger.warning(
                                f"Could not interact with checkbox {checkbox_id}: {e}"
                            )

                try:
                    time.sleep(15)
                    try:
                        html_table_radio = self.driver.find_element(
                            By.XPATH,
                            "//input[@type='radio' and @value='htmlTable']",
                        )
                        if not html_table_radio.is_selected():
                            html_table_radio.click()
                            logger.info(
                                "Selected HTML Table using standard XPath"
                            )
                    except NoSuchElementException:
                        try:
                            html_table_radio = self.driver.find_element(
                                By.ID, "htmlTable"
                            )
                            if not html_table_radio.is_selected():
                                html_table_radio.click()
                                logger.info("Selected HTML Table using ID")
                        except NoSuchElementException:
                            try:
                                self.driver.execute_script(
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
                                logger.info(
                                    "Selected HTML Table using JavaScript"
                                )
                            except Exception as js_error:
                                logger.warning(
                                    f"JavaScript selection failed: {js_error}"
                                )

                                try:
                                    label = self.driver.find_element(
                                        By.XPATH,
                                        "//label[contains(text(), 'HTML Table')]",
                                    )
                                    label.click()
                                    logger.info(
                                        "Selected HTML Table via label"
                                    )
                                except Exception as label_error:
                                    logger.warning(
                                        f"Label selection failed: {label_error}"
                                    )
                                    logger.info(
                                        "Continuing without explicit HTML Table selection"
                                    )
                except Exception as radio_error:
                    logger.warning(
                        f"All HTML Table selection approaches failed: {radio_error}"
                    )

                search_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "searchButton"))
                )
                search_button.click()

                try:
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located(
                            (By.ID, "symbolTable")
                        )
                    )
                    time.sleep(5)

                    try:
                        records_text = self.driver.find_element(
                            By.ID, "quantityHeader"
                        ).text
                        self.total_records = (
                            int(
                                records_text.split("of")[1]
                                .strip()
                                .replace(",", "")
                            )
                            if "of" in records_text
                            else 0
                        )
                        logger.info(
                            f"Found {self.total_records} total records."
                        )
                    except Exception as qty_error:
                        logger.warning(
                            f"Could not extract record count: {qty_error}"
                        )
                        self.total_records = 1000
                    return True
                except TimeoutException:
                    logger.error("Timed out waiting for search results")
                    screenshot_path = (
                        "/opt/airflow/logs/search_timeout.png"
                    )
                    self.driver.save_screenshot(screenshot_path)
                    logger.info(f"Saved screenshot to {screenshot_path}")
                    return False

            except Exception as e:
                logger.error(f"Error performing search: {e}")
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logger.error("Exception traceback:")
                for line in traceback.format_exception(
                    exc_type, exc_value, exc_traceback
                ):
                    logger.error(line.rstrip())

                screenshot_path = "/opt/airflow/logs/search_error.png"
                try:
                    self.driver.save_screenshot(screenshot_path)
                    logger.info(
                        f"Saved error screenshot to {screenshot_path}"
                    )
                except:
                    pass
                return False

        scraper = DTNIQFeed(
            output_file="/opt/airflow/data/DTN_SYMBOLS/dtn_no_options.parquet"
        )

        import types

        scraper.perform_search = types.MethodType(
            enhanced_perform_search, scraper
        )

        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.binary_location = "/usr/bin/google-chrome"

            service = Service()
            scraper.driver = webdriver.Chrome(
                service=service, options=options
            )
            scraper.driver.set_page_load_timeout(60)
            scraper.driver.get(scraper.url)

            time.sleep(20)
            success = scraper.perform_search(
                exchange=Variable.get('EXCHANGE') if Variable.get('EXCHANGE') else None,
                security_type=Variable.get('SECURITY_TYPE') if Variable.get('SECURITY_TYPE') else None,
                show_front_month=(
                    True if Variable.get('SHOW_FRONT_MONTH') == "True" else False
                ),
                show_continuous=(
                    True if Variable.get('SHOW_CONTINUOUS') == "True" else False
                ),
                show_eminis=True if Variable.get('SHOW_EMINIS') == "True" else False,
                no_options=True if Variable.get('NO_OPTIONS') == "True" else False,
                no_spreads=True if Variable.get('NO_SPREADS') == "True" else False,
            )

            if not success:
                raise Exception("Search operation failed")

            import asyncio

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    scraper.run_complete_extraction(show_front_month=True)
                )
            finally:
                loop.close()

            return "DTN symbol scraping completed successfully"
        except Exception as e:
            from utils.logging import Logger

            logger = Logger(
                name="airflow_task", log_dir="/opt/airflow/logs"
            )
            logger.error(f"DTN symbol scraping failed: {str(e)}")

            if hasattr(scraper, "driver") and scraper.driver:
                screenshot_path = "/opt/airflow/logs/final_error.png"
                try:
                    scraper.driver.save_screenshot(screenshot_path)
                    logger.info(
                        f"Saved final error screenshot to {screenshot_path}"
                    )
                except:
                    pass

            raise Exception(f"DTN symbol scraping failed: {str(e)}")
    except Exception as outer_e:
        raise Exception(f"Setup failed: {str(outer_e)}")


default_args = {
    "owner": "Sarim Sikander",
    "start_date": datetime(2025, 4, 1),
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "email": "sarimsikander24@gmail.com",
    "on_failure_callback": send_dag_failure_email,
    "on_success_callback": send_dag_success_email,
}

with DAG(
    dag_id="DTN_IQFEED_SCRAPER_V1.0.1",
    default_args=default_args,
    description="Scrape symbols from DTN IQFeed using Selenium",
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["scraping", "dtn", "iqfeed", "symbols"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_dtn_symbols",
        python_callable=scrape_dtn_symbols,
        provide_context=True,
    )

    scrape_task
