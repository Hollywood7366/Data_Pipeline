from __future__ import annotations

import os
from typing import Any


class Config:
    ENVIRONMENT: str
    DATABASE_URL: str
    LOCAL_DATABASE_URL: str

    IQFEED_HOST: str
    IQFEED_PORT: int

    @staticmethod
    def load_environment_variables(*env_files: str) -> None:
        possible_paths = [
            "src/.env",
            "/opt/airflow/src/.env",
            os.path.join(os.environ.get("AIRFLOW_HOME", ""), "src/.env"),
        ]

        found = False
        for env_file in possible_paths + list(env_files):
            if os.path.exists(env_file):
                found = True
                with open(env_file) as file:
                    for line in file:
                        if line.strip() and not line.startswith("#"):
                            key, value = map(str.strip, line.split("=", 1))
                            os.environ[key] = value

        if not found:
            pass

    @staticmethod
    def get_env_variable(key: str, default=None) -> str | Any | None:
        return os.getenv(key, default)

    def __init__(self) -> None:
        for attr, default_value in self.__annotations__.items():
            setattr(
                self,
                attr,
                self.get_env_variable(attr, default=default_value),
            )


Config.load_environment_variables(".env")

config = Config()
