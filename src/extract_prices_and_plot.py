import argparse
from constants import (
    RAW_DATA_DAY_ZIP_PATH,
    RAW_DATA_INTERVAL_ZIP_PATH,
    RAW_DATA_INTERVAL_CSV_PATH,
    CLEAN_DATA_INTERVAL_CSV_PATH,
    Region
)
from download_data import download_interval_data_multithreaded
from unzip_data import unzip_data_multithreaded
from interval_price_extraction import extract_interval_data_into_csv_multithreaded
from analytics import *


def extract_and_plot(start_date: str, end_date: str, num_threads: int):
    download_interval_data_multithreaded(start_date, end_date, num_threads)

    unzip_data_multithreaded(
        RAW_DATA_DAY_ZIP_PATH, RAW_DATA_INTERVAL_ZIP_PATH, num_threads
    )
    unzip_data_multithreaded(
        RAW_DATA_INTERVAL_ZIP_PATH, RAW_DATA_INTERVAL_CSV_PATH, num_threads
    )

    extract_interval_data_into_csv_multithreaded(RAW_DATA_INTERVAL_CSV_PATH, num_threads)

    for region in Region:
        df_5_min_interval = get_5_min_interval(
            CLEAN_DATA_INTERVAL_CSV_PATH, region.value
        )

        df_30_min_interval = get_time_weighted_avg_30_min_res(df_5_min_interval)

        df_percentile = create_percentile_df(df_30_min_interval)

        # convert data to long format for visualisation
        df_long_format = long_format_conversion(df_percentile)

        create_visualisation(df_long_format, f'1st, 10th, 50th, 90th, 99th Percentile of $/MWh in {region.name} from {start_date} to {end_date}', f'{region.name}_plot.png')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Script to extract and plot interval energy price.')
    parser.add_argument('--start_date', type=str, default='20221201', help='Start date must be in YYYYmmdd format')
    parser.add_argument('--end_date', type=str, default='20221205',
                        help='End date must be in YYYYmmdd format')
    parser.add_argument('--num_threads', type=int, default=10, help="Number of threads required to extract the data")
    args = parser.parse_args()
    extract_and_plot(args.start_date, args.end_date, args.num_threads)
