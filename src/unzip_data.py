import glob
import os
import concurrent.futures
from zipfile import ZipFile
from constants import (
    RAW_DATA_DAY_ZIP_PATH,
    RAW_DATA_INTERVAL_ZIP_PATH,
    RAW_DATA_INTERVAL_CSV_PATH,
)


def unzip_data(source_file: str, target_dir: str):
    print(f"Extracting {source_file}")

    with ZipFile(source_file, "r") as zip_ref:
        zip_ref.extractall(target_dir)
        print(f"Extraction of {source_file} to {target_dir} completed.")


def unzip_data_multithreaded(
    source_dir: str, destination_path: str, num_threads: int = 10
):
    zip_files_pattern = f"{source_dir}/*.zip"
    file_paths = glob.glob(zip_files_pattern)
    zip_file_names = [
        f"{source_dir}/{os.path.basename(zip_file)}" for zip_file in file_paths
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks to the thread pool
        futures = [
            executor.submit(unzip_data, file, destination_path)
            for file in zip_file_names
        ]

        # Wait for all threads to complete
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    unzip_data_multithreaded(RAW_DATA_DAY_ZIP_PATH, RAW_DATA_INTERVAL_ZIP_PATH, 30)
    unzip_data_multithreaded(RAW_DATA_INTERVAL_ZIP_PATH, RAW_DATA_INTERVAL_CSV_PATH, 30)
