import logging
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


def filter_on_start_and_end_date(data, start_time, end_time):
    data["START_DATE"] = pd.to_datetime(data["START_DATE"], format="%Y/%m/%d %H:%M:%S")
    data["END_DATE"] = np.where(
        data["END_DATE"] == "2999/12/31 00:00:00",
        "2100/12/31 00:00:00",
        data["END_DATE"],
    )
    data["END_DATE"] = pd.to_datetime(data["END_DATE"], format="%Y/%m/%d %H:%M:%S")
    data = data[(data["START_DATE"] < end_time) & (data["END_DATE"] > start_time)]
    return data


def filter_on_effective_date(data, start_time, end_time):
    data["EFFECTIVEDATE"] = pd.to_datetime(
        data["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
    )
    data = data[data["EFFECTIVEDATE"] < end_time]
    return data


def filter_on_settlementdate(data, start_time, end_time):
    data["SETTLEMENTDATE"] = pd.to_datetime(
        data["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
    )
    data = data[
        (data["SETTLEMENTDATE"] > start_time) & (data["SETTLEMENTDATE"] <= end_time)
    ]
    return data


def filter_on_timestamp(data, start_time, end_time):
    try:
        data["TIMESTAMP"] = pd.to_datetime(
            data["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S"
        )
    except Exception as e:
        logging.error(e)
        # if date format is wrong, str may be too short
        med_str_len = np.median(data["TIMESTAMP"].str.len())
        not_data = data.loc[data["TIMESTAMP"].str.len() < med_str_len, :]
        data = data.loc[data["TIMESTAMP"].str.len() >= med_str_len, :]
        data["TIMESTAMP"] = pd.to_datetime(
            data["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S"
        )
        logging.warning("Rows with incorrect data formats omitted")
        logging.warning(not_data)
    finally:
        data = data[(data["TIMESTAMP"] > start_time) & (data["TIMESTAMP"] <= end_time)]
    return data


def filter_on_interval_datetime(data, start_time, end_time):
    data["INTERVAL_DATETIME"] = pd.to_datetime(
        data["INTERVAL_DATETIME"], format="%Y/%m/%d %H:%M:%S"
    )
    data = data[
        (data["INTERVAL_DATETIME"] > start_time)
        & (data["INTERVAL_DATETIME"] <= end_time)
    ]
    return data


# Not tested, just for nemlite integration.
def filter_on_date_and_peroid(data, start_time, end_time):
    data = construct_interval_datetime_from_period_id(data)
    data = data[
        (data["SETTLEMENTDATE"] > start_time)
        & (data["SETTLEMENTDATE"] - timedelta(minutes=30) <= end_time)
    ]
    return data


# Not tested, just for nemlite integration.
def filter_on_date_and_interval(data, start_time, end_time):
    data["SETTLEMENTDATE"] = pd.to_datetime(
        data["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
    )
    data = data[
        (data["SETTLEMENTDATE"] > start_time) & (data["SETTLEMENTDATE"] <= end_time)
    ]
    return data


# Not tested, just for nemlite integration.
def filter_on_last_changed(data, start_time, end_time):
    data["LASTCHANGED"] = pd.to_datetime(
        data["LASTCHANGED"], format="%Y/%m/%d %H:%M:%S"
    )
    data = data[data["LASTCHANGED"] < end_time]
    return data


def filter_on_column_value(data, filter_cols, filter_values):
    for filter_col, filter_values in zip(filter_cols, filter_values):
        if filter_values is not None:
            data = data[data[filter_col].isin(filter_values)]
    return data


# Not tested, just for nemlite integration.
def construct_interval_datetime_from_period_id(data):
    data["SETTLEMENTDATE"] = np.vectorize(date_2_interval_datetime)(
        data["SETTLEMENTDATE"], data["PERIODID"]
    )
    return data


# Not tested, just for nemlite integration.
def date_2_interval_datetime(date, period):
    datetime_obj = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
    datetime_obj = datetime(
        year=datetime_obj.year, month=datetime_obj.month, day=datetime_obj.day, hour=4
    )
    datetime_obj = datetime_obj + timedelta(minutes=(float(period) * 30))
    return datetime_obj
