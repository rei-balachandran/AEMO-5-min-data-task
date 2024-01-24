import unittest
import interval_price_extraction as ipe
from constants import RAW_DATA_INTERVAL_CSV_PATH, CLEAN_DATA_INTERVAL_CSV_PATH
import pandas as pd
from datetime import datetime
import helpers


class TestExtractIntervalDataIntoCSV(unittest.TestCase):

    def test_extract_interval_data_into_csv(self):
        actual_data_non_midnight = pd.read_csv(f"{CLEAN_DATA_INTERVAL_CSV_PATH}/expected_data_202212212355.csv")
        file_name_non_midnight = "PUBLIC_DISPATCHIS_202212212355_0000000377348000.CSV"
        ipe.extract_interval_data_into_csv(f"{RAW_DATA_INTERVAL_CSV_PATH}/{file_name_non_midnight}")
        expected_data_non_midnight = pd.read_csv(f"{CLEAN_DATA_INTERVAL_CSV_PATH}/interval_data_202212212355.csv")

        pd.testing.assert_frame_equal(expected_data_non_midnight, actual_data_non_midnight)

        actual_data_midnight = pd.read_csv(f"{CLEAN_DATA_INTERVAL_CSV_PATH}/expected_data_202212220000.csv")
        file_name_midnight = "PUBLIC_DISPATCHIS_202212220000_0000000377348186.CSV"
        ipe.extract_interval_data_into_csv(f"{RAW_DATA_INTERVAL_CSV_PATH}/{file_name_midnight}")
        expected_data_midnight = pd.read_csv(f"{CLEAN_DATA_INTERVAL_CSV_PATH}/interval_data_202212220000.csv")

        pd.testing.assert_frame_equal(expected_data_midnight, actual_data_midnight)


if __name__ == "__main__":
    unittest.main()

