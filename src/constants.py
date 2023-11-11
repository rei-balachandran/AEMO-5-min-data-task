from enum import Enum


class Region(Enum):
    NSW = "NSW1"
    QLD = "QLD1"
    SA = "SA1"
    TAS = "TAS1"
    VIC = "VIC1"


RAW_DATA_DAY_ZIP_PATH = "resources/raw_data/day_zip"
RAW_DATA_INTERVAL_ZIP_PATH = "resources/raw_data/interval_zip"
RAW_DATA_INTERVAL_CSV_PATH = "resources/raw_data/interval_csv"
CLEAN_DATA_INTERVAL_CSV_PATH = "resources/clean_data/interval_price"
