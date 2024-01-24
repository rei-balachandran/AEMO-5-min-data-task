import concurrent.futures
import time
import requests
from constants import RAW_DATA_DAY_ZIP_PATH
from helpers import get_dates_between


DATA_URL = "https://nemweb.com.au/Reports/Archive/DispatchIS_Reports/PUBLIC_DISPATCHIS_"


def download_interval_data(date: str):
    url = f"{DATA_URL}{date}.zip"
    file_name = f"interval_data_{date}.zip"
    file_location = f"{RAW_DATA_DAY_ZIP_PATH}/{file_name}"
    retry_count = 0

    while retry_count <= 3:
        # Check if the request was successful
        try:
            response = requests.get(url)
            response.raise_for_status()
            if response.status_code == 200:
                # Open the file in binary write mode and save the content of the request
                with open(file_location, "wb") as file:
                    file.write(response.content)
                    print(f"Saved {file_name} into {file_location}.")
                break
        except Exception as e:
            print(f"HTTP error occurred when trying to download {url}: {e}")
            retry_count += 1
            time.sleep(0.2)



def download_interval_data_multithreaded(
    start_date: str, end_date: str, num_threads: int = 10
):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks to the thread pool
        futures = [
            executor.submit(download_interval_data, date)
            for date in get_dates_between(start_date, end_date)
        ]

        # Wait for all threads to complete
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    download_interval_data_multithreaded("20221201", "20221231", 20)
