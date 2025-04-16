import asyncio

from src.config.config import config as cn
from src.pipelines.iqsymbols import DTNIQFeed

if __name__ == "__main__":
    scraper = DTNIQFeed(
        output_file="data/DTN_SYMBOLS/dtn_no_options.parquet"
    )
    scraper.start_browser()
    scraper.perform_search(
        exchange=cn.EXCHANGE,
        security_type=cn.SECURITY_TYPE,
        show_front_month=True if cn.SHOW_FRONT_MONTH == "True" else False,
        show_continuous=True if cn.SHOW_CONTINUOUS == "True" else False,
        show_eminis=True if cn.SHOW_EMINIS == "True" else False,
        no_options=True if cn.NO_OPTIONS == "True" else False,
        no_spreads=True if cn.NO_SPREADS == "True" else False,
    )
    asyncio.run(scraper.run_complete_extraction(show_front_month=True))
