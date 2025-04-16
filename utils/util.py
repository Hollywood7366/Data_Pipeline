import os


def base_path() -> str:
    """
    Returns the base path of the project.
    """
    return os.getcwd()


def clean_data(data: str) -> str:
    return data.replace("\r", "").replace(",\n", "\n").strip()


def _parse_raw_data(data: str) -> list:
    lines = [
        line
        for line in data.split("\n")
        if not line.startswith("S,") and line.strip()
    ]

    if not lines:
        return []

    formatted_rows = []
    for line in lines:
        parts = line.split(",")
        if len(parts) >= 8 and parts[0] in ["LH", "DT", "T"]:
            row = parts[1:]
        else:
            row = parts
        if len(row) == 8:
            formatted_rows.append(row)

    return formatted_rows
