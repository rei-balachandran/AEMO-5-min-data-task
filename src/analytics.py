import dask.dataframe as dd
import pandas as pd
import numpy as np
import swifter
from plotnine import *
from constants import CLEAN_DATA_INTERVAL_CSV_PATH, Region
from helpers import get_30_min_intervals

PERCENTILE_COL_NAMES = [
    "1st_percentile",
    "10th_percentile",
    "50th_percentile",
    "90th_percentile",
    "99th_percentile",
]


def get_5_min_interval(file_path: str, region_id: str = None) -> pd.DataFrame:
    dask_df = dd.read_csv(f"{file_path}/*.csv")

    if region_id:
        dask_df = dask_df[dask_df["region"] == region_id]

    df = dask_df.compute()
    df["settlement_date"] = pd.to_datetime(df["settlement_date"])
    df_sorted = df.sort_values(by="settlement_date")
    grouped_df = (
        df_sorted.groupby([pd.Grouper(key="settlement_date", freq="D"), "region"])
        .agg({"price": list})
        .reset_index()
    )
    grouped_df["5_min_intervals_len"] = grouped_df["price"].swifter.apply(
        lambda x: len(x)
    )
    return grouped_df


def get_time_weighted_avg_30_min_res(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe["30_min_avg"] = dataframe["price"].resample('30T').mean()
    return dataframe


def create_percentile_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    percentiles_to_calculate = [1, 10, 50, 90, 99]
    time_intervals = get_30_min_intervals()

    lst_30_min_avg = dataframe["30_min_avg"].to_list()
    lst_30_min_avg_np_array = np.array(lst_30_min_avg)
    percentiles_array = np.percentile(
        lst_30_min_avg_np_array, percentiles_to_calculate, axis=0
    )
    percentiles_array_transposed = percentiles_array.T
    percentile_df = pd.DataFrame(
        percentiles_array_transposed, columns=PERCENTILE_COL_NAMES
    )
    percentile_df.insert(0, "30_min_interval", time_intervals)
    return percentile_df


def long_format_conversion(dataframe: pd.DataFrame) -> pd.DataFrame:
    long_format_df = dataframe.melt(
        id_vars="30_min_interval",
        value_vars=PERCENTILE_COL_NAMES,
        var_name="percentiles",
        value_name="aud_per_MWh",
    )
    return long_format_df


def create_visualisation(
    dataframe: pd.DataFrame, title: str, output_file_name: str
) -> None:
    """file_name: must include extension, eg abc.png"""
    dataframe["percentiles"] = pd.Categorical(
        dataframe["percentiles"],
        categories=[
            "1st_percentile",
            "10th_percentile",
            "50th_percentile",
            "90th_percentile",
            "99th_percentile",
        ],
        ordered=True,
    )

    plot = (
        ggplot(
            dataframe,
            aes(
                x="30_min_interval",
                y="aud_per_MWh",
                color="percentiles",
                group="percentiles",
            ),
        )
        + geom_point()
        + geom_line()
        + labs(
            title=title,
            x="Time Interval (30 Minutes)",
            y="Price ($/MWh)",
            color="Percentiles",
        )
        + theme_bw()
        + theme(
            plot_title=element_text(size=20, ha="center"),
            axis_title=element_text(size=15),
            axis_text_x=element_text(size=12, rotation=45, hjust=5),
            axis_text_y=element_text(size=12),
            legend_title=element_text(size=15),
            legend_text_legend=element_text(size=12),
        )
    )  # TODO: need proper formatting of legends-ordering, colour etc.
    plot.save(f"plot/{output_file_name}", width=20, height=12)


if __name__ == "__main__":
    grouped_df = get_5_min_interval(CLEAN_DATA_INTERVAL_CSV_PATH, Region.VIC.value)
    avg_30_min_dataframe = get_time_weighted_avg_30_min_res(grouped_df)
    percentiles_df = create_percentile_df(avg_30_min_dataframe)
    final_df = long_format_conversion(percentiles_df)
    create_visualisation(final_df, "Victoria plot", "vic.png")
    print(final_df.head(20))

# /Users/reibalachandran/.pyenv/versions/AEMO5MinDataTask/bin/python /Users/reibalachandran/Desktop/PythonProjects/AEMO-5-min-data-task/src/analytics.py
