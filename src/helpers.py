from datetime import datetime, timedelta
from typing import List

DATE_FORMAT = "%Y%m%d"


def get_dates_between(from_date: str, to_date: str):
    start_date = datetime.strptime(from_date, DATE_FORMAT)
    end_date = datetime.strptime(to_date, DATE_FORMAT)

    current_date = start_date

    while current_date <= end_date:
        yield current_date.strftime(DATE_FORMAT)
        current_date += timedelta(days=1)


def convert_midnight_to_previous_day(date: str) -> str:
    settlement_date = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")

    # Check if the time is midnight (00:00:00)
    if settlement_date.time() == datetime.min.time():
        # Subtract one minute to get 23:59:59 of the previous day
        settlement_date = settlement_date - timedelta(seconds=1)
        return settlement_date.strftime("%Y/%m/%d %H:%M:%S")
    return date


def get_30_min_intervals() -> List:
    time = datetime(2021, 1, 1, 0, 30)
    time_intervals = []

    for _ in range(48):
        time_intervals.append(time.strftime("%H:%M"))
        time += timedelta(minutes=30)

    return time_intervals


if __name__ == "__main__":
    date = convert_midnight_to_previous_day("19/12/2022 0:10")
    print("")
