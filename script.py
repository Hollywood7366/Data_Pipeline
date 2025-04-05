import asyncio

from src.pipelines.iqsymbols import DTNIQFeed

if __name__ == "__main__":
    scraper = DTNIQFeed(
        headless=False, output_file="data/DTN_SYMBOLS/dtn_no_options.parquet"
    )
    scraper.start_browser()
    scraper.perform_search(show_front_month=True)
    asyncio.run(scraper.run_complete_extraction())
