import asyncio

from src.models.metadata import History, IQFeedDataMeta
from src.pipelines.extras.metadata import MetadataManager


def run_async_task(coroutine):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        return future.result()
    else:
        return loop.run_until_complete(coroutine)


async def get_async_logs_script(successful_symbols=None):
    metadata_manager = MetadataManager(model=History)
    success_count = len(successful_symbols) if successful_symbols else 0

    await metadata_manager.log_script_run(success_count)


async def data_to_parquet_async(sym, metadata_record):
    metadata_manager = MetadataManager(model=IQFeedDataMeta)
    await metadata_manager.save_iqfeed_processed_metadata(
        symbol=sym, records=[metadata_record], period="daily"
    )
