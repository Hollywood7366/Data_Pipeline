from __future__ import annotations

import os
from typing import Any


class Config:
    ENVIRONMENT: str = 'development'
    DATABASE_URL: str = 'mysql+aiomysql://root:1234@host.docker.internal:3306/probabilitiesunlimited'
    LOCAL_DATABASE_URL: str = 'mysql+aiomysql://root:1234@localhost:3306/probabilitiesunlimited'
    SPREADSHEET_KEY: str = '1w07aJMtZx_f77zef_vIER-_xR1ygj7Kyqr76d6AjwuI'

    @staticmethod
    def load_environment_variables(*env_files: str) -> None:
        for env_file in env_files:
            if os.path.exists(env_file):
                with open(env_file) as file:
                    for line in file:
                        if line.strip() and not line.startswith("#"):
                            key, value = map(str.strip, line.split("=", 1))
                            os.environ[key] = value

    @staticmethod
    def get_env_variable(key: str, default=None) -> str | Any | None:
        return os.getenv(key, default)

    def __init__(self) -> None:
        for attr, default_value in self.__annotations__.items():
            setattr(self, attr, self.get_env_variable(attr, default=default_value))


Config.load_environment_variables(".env")

config = Config()
