import csv
import concurrent.futures
import glob
import os
from typing import List
from constants import CLEAN_DATA_INTERVAL_CSV_PATH
from helpers import convert_midnight_to_previous_day

REGION_IDS = ("NSW1", "QLD1", "SA1", "TAS1", "VIC1")  # TODO: replace with enum
(
    D_COL_INDEX,
    DISPATCH_COL_INDEX,
    PRICE_COL_INDEX,
    REGION_ID_COL_INDEX,
    SETTLEMENT_DATE_COL_INDEX,
    RRP_COL_INDEX,
) = (0, 1, 2, 6, 4, 9)
HEADER = ["settlement_date", "region", "price"]


def extract_interval_data_into_csv(file_path: str):
    interval_data = [HEADER]

    with open(file_path, newline="") as csvfile:
        # Create a CSV reader object
        csvreader = csv.reader(csvfile)
        desired_rows_count = 0

        for row in csvreader:
            # Extract the value using the column index
            D_value = row[D_COL_INDEX]
            dispatch_value = row[DISPATCH_COL_INDEX]
            price_value = row[PRICE_COL_INDEX]
            region_id = row[REGION_ID_COL_INDEX]

            if (
                D_value == "D"
                and dispatch_value == "DISPATCH"
                and price_value == "PRICE"
                and region_id in REGION_IDS
            ):
                desired_rows_count += 1
                settlement_date = convert_midnight_to_previous_day(
                    row[SETTLEMENT_DATE_COL_INDEX]
                )
                rrp = row[RRP_COL_INDEX]
                interval_data.append([settlement_date, region_id, rrp])

            if desired_rows_count > 4:
                break

    file_name = file_path.split("/")[-1]
    settlement_date_interval = file_name.split("_")[2]
    new_file_path = (
        f"{CLEAN_DATA_INTERVAL_CSV_PATH}/interval_data_{settlement_date_interval}.csv"
    )
    with open(new_file_path, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(interval_data)
        print(
            f"Extraction of settlement date, region and price values from {file_path} completed"
        )


def extract_interval_data_into_csv_multithreaded(
    source_path: str, num_threads: int = 10
):
    csv_files_pattern = f"{source_path}/*.CSV"
    csv_file_paths = glob.glob(csv_files_pattern)
    csv_file_names = [
        f"{source_path}/{os.path.basename(csv_file)}" for csv_file in csv_file_paths
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks to the thread pool
        futures = [
            executor.submit(extract_interval_data_into_csv, file)
            for file in csv_file_names
        ]

        # Wait for all threads to complete
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    extract_interval_data_into_csv_multithreaded(CLEAN_DATA_INTERVAL_CSV_PATH)
