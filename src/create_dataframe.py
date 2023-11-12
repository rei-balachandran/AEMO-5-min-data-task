import pandas as pd
from typing import List
import glob
import os
import concurrent.futures
from constants import CLEAN_DATA_INTERVAL_CSV_PATH


def read_data(file_name, data_list: List):
    interval_data = pd.read_csv(file_name, header=1, index_col=False)
    data_list.append(interval_data)


def read_data_multithreaded(files: List, data_list: List, num_threads: int = 10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks to the thread pool
        futures = [executor.submit(read_data, file, data_list) for file in files]

        # Wait for all threads to complete
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    csv_files_pattern = f"{CLEAN_DATA_INTERVAL_CSV_PATH}/*.csv"
    csv_file_paths = glob.glob(csv_files_pattern)
    csv_file_names = [
        f"{CLEAN_DATA_INTERVAL_CSV_PATH}/{os.path.basename(csv_file)}"
        for csv_file in csv_file_paths
    ]
    data = []
    read_data_multithreaded(csv_file_names, data, 20)
    df = pd.concat(data)
    print(df.head())
