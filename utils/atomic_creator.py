import polars as pl
import os
import time
from utils.logging import Logger

logger = Logger(name="iqfeed", log_dir="data/logs")

class AtomicFileUpdate:
    def __init__(self, final_file_path: str, temp_file_path: str):
        self.final_file_path = final_file_path
        self.temp_file_path = temp_file_path

    def write_data(self, data: pl.DataFrame):
        try:
            data.write_parquet(self.temp_file_path)
        except Exception as e:
            logger.error(f"Error writing to temporary file: {e}")
            return False
        return True

    def finalize_update(self):
        try:
            if os.path.exists(self.final_file_path):
                os.remove(self.final_file_path)
            
            os.rename(self.temp_file_path, self.final_file_path)
        except Exception as e:
            logger.error(f"Error finalizing update: {e}")
            return False
        return True

    def perform_atomic_update(self, data: pl.DataFrame):
        if self.write_data(data):
            time.sleep(1)
            return self.finalize_update()
        return False
