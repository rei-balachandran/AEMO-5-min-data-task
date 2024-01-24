import unittest
from constants import CLEAN_DATA_INTERVAL_CSV_PATH, Region
from unzip_data import unzip_data
from constants import (
    RAW_DATA_DAY_ZIP_PATH,
    RAW_DATA_INTERVAL_ZIP_PATH,
    RAW_DATA_INTERVAL_CSV_PATH,
)
import os
import glob
from . import helpers

class TestUnzipData(unittest.TestCase):
    source_day_zip_files_pattern = "resources/raw_data/day_zip/*.zip"
    target_zip_dir = "resources/raw_data/interval_zip"
    target_csv_dir = "resources/raw_data/interval_csv"

    def setUp(self) -> None:
        helpers.delete_files(self.target_zip_dir)
        helpers.delete_files(self.target_csv_dir)

    def test_extraction_successful(self):
        source_day_zip_files = glob.glob(self.source_day_zip_files_pattern)
        for source_day_zip_file in source_day_zip_files:
            unzip_data(source_day_zip_file, self.target_zip_dir)
        extracted_zip_files_present = os.path.exists(self.target_zip_dir)
        self.assertTrue(extracted_zip_files_present, "Extraction of interval zip files failed.")

        expected_interval_zip_files_pattern = "resources/raw_data/interval_zip/PUBLIC*/*.zip"
        interval_zip_files = glob.glob(expected_interval_zip_files_pattern)
        for file in interval_zip_files:
            unzip_data(file, self.target_csv_dir)
        extracted_csv_files_present = os.path.exists(self.target_csv_dir)
        self.assertTrue(extracted_csv_files_present, "Extraction of interval csv files failed.")

        missing_zip_files = [file for file in interval_zip_files if not os.path.exists(file)]
        self.assertEqual(len(missing_zip_files), 0,
                         f"The following zip files are missing from the target directory: {missing_zip_files}")

        expected_interval_csv_files_pattern = "resources/raw_data/interval_csv/*.CSV"
        interval_csv_files = glob.glob(expected_interval_csv_files_pattern)
        missing_csv_files = [file for file in interval_csv_files if not os.path.exists(file)]
        self.assertEqual(len(missing_csv_files), 0,
                         f"The following csv files are missing from the target directory: {missing_csv_files}")

if __name__ == "__main__":
    unittest.main()



