from src.models import (
    IqfeedSymbolsContinuousContracts,
    IqfeedSymbolsEminis,
    IqfeedSymbolsFrontMonth,
    IqfeedSymbolsNoOptions,
    IqfeedSymbolsNoSpreads,
)
from utils.util import base_path

CREDENTIALS_PATH = f"{base_path()}/src/api.json"

STORAGE_DIR = f"{base_path()}/storage"
SYMBOLS_RAW = f"{base_path()}/data/GOOGLE_TO_LOCAL/selectedsymbols.parquet"
SYMBOLS_COMPLETE = (
    f"{base_path()}/data/GOOGLE_TO_LOCAL/selectedsymbols_renewed.parquet"
)

SYMBOLS_MODEL_SELECTION = {
    "IqfeedSymbolsNoSpreads": IqfeedSymbolsNoSpreads,
    "IqfeedSymbolsContinuousContracts": IqfeedSymbolsContinuousContracts,
    "IqfeedSymbolsEminis": IqfeedSymbolsEminis,
    "IqfeedSymbolsFrontMonth": IqfeedSymbolsFrontMonth,
    "IqfeedSymbolsNoOptions": IqfeedSymbolsNoOptions,
}

SYMBOLS_METADATA_STATUS = ["ADDED", "UPDATED"]

GET_THIS_TYPE = {'FUTURE': False,
'FOREX': False,
'EQUITY': False,
'FOPTION': False,
'IEOPTION':False}