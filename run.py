import asyncio
from src.pipelines.iqsymbols import DTNIQFeed


if __name__ == "__main__":
    scraper = DTNIQFeed(headless=False, output_file="data/dtn_no_options.csv")
    scraper.start_browser()
    scraper.perform_search(no_options=True)
    asyncio.run(scraper.run_complete_extraction(security_type="Futures"))
