import os


def base_path() -> str:
    """
    Returns the base path of the project.
    """
    return os.getcwd()


def clean_data(data: str) -> str:
    return data.replace("\r", "").replace(",\n", "\n").strip()
