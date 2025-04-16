import logging
import os
import traceback
from logging.handlers import RotatingFileHandler


class Logger:
    def __init__(
        self,
        name="iqfeed",
        log_dir="data/logs",
        console_level=logging.INFO,
        file_level=logging.DEBUG,
        max_file_size=10 * 1024 * 1024,
        backup_count=5,
    ):
        self.logger = logging.getLogger(name)

        if self.logger.handlers:
            return

        self.logger.setLevel(logging.DEBUG)
        self.log_dir = log_dir
        self.max_file_size = max_file_size
        self.backup_count = backup_count

        os.makedirs(self.log_dir, exist_ok=True)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        self._add_file_handler("debug", logging.DEBUG, file_level)
        self._add_file_handler("info", logging.INFO, file_level)
        self._add_file_handler("warning", logging.WARNING, file_level)
        self._add_file_handler("error", logging.ERROR, file_level)

        self.logger.propagate = False

    def _add_file_handler(self, level_name, level, min_level):
        if level < min_level:
            return

        file_path = os.path.join(self.log_dir, f"{level_name}.txt")

        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
        )

        file_handler.setLevel(level)
        file_handler.addFilter(lambda record: record.levelno == level)

        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message, exc_info=None):
        if exc_info:
            self.logger.error(f"{message}\n{traceback.format_exc()}")
        else:
            self.logger.error(message)

    def critical(self, message, exc_info=None):
        if exc_info:
            self.logger.critical(f"{message}\n{traceback.format_exc()}")
        else:
            self.logger.critical(message)

    def exception(self, message):
        self.logger.exception(message)
