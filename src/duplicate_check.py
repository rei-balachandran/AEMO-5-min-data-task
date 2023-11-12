import glob
import os
import csv
from typing import List
import concurrent.futures
from constants import CLEAN_DATA_INTERVAL_CSV_PATH

region_ids = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}


def check_regions_in_csv(
    file_path, column_name, matched_list
):  # TODO: Make sure to do sanity check of duplicates
    # Initialize a set to store unique values from the column
    found_regions = set()

    # Open the CSV file and read it
    with open(file_path, mode="r", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Add the value from the column to the set
            found_regions.add(row[column_name])

            # Early exit if all regions are found
        if found_regions != region_ids:
            matched_list.append(f"{file_path}-Failed")


def check_regions_in_csv_multithreaded(
    file_path: str, matched_list: List, column_name: str, num_threads: int = 10
):
    csv_files_pattern = f"{file_path}/*.csv"
    file_paths = glob.glob(csv_files_pattern)
    csv_file_names = [
        f"{CLEAN_DATA_INTERVAL_CSV_PATH}/{os.path.basename(csv_file)}"
        for csv_file in file_paths
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks to the thread pool
        futures = [
            executor.submit(check_regions_in_csv, file, column_name, matched_list)
            for file in csv_file_names
        ]

        # Wait for all threads to complete
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    check_list = []
    check_regions_in_csv_multithreaded(
        CLEAN_DATA_INTERVAL_CSV_PATH, check_list, "region", 30
    )
    print(check_list)
