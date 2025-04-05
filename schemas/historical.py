HISTORICAL_DATA_SCHEMA = {
    "symbol": "SYMBOL",
    "datetime": "TIMESTAMP",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "open": "DOUBLE",
    "close": "DOUBLE",
    "total_volume": "LONG",
    "period_volume": "LONG",
}

SYMBOLS_SCHEMA = {
    "Futures": {
        "symbol": "SYMBOL",
        "description": "STRING",
        "security_type": "STRING",
        "exchange": "STRING",
        "listed_market": "STRING",
    },
    "Equity": {
        "symbol": "SYMBOL",
        "description": "STRING",
        "security_type": "STRING",
        "exchange": "STRING",
        "listed_market": "STRING",
    },
    "Default": {
        "symbol": "SYMBOL",
        "description": "STRING",
        "security_type": "STRING",
        "exchange": "STRING",
        "listed_market": "STRING",
    },
}
