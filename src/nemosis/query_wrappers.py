import pandas as pd
from datetime import datetime, timedelta
from nemosis import defaults, _parse_datetime


def dispatch_date_setup(start_time, end_time):
    start_time = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    start_time = start_time - timedelta(hours=4)
    start_time = start_time.replace(hour=0, minute=0)
    start_time = start_time - timedelta(seconds=1)
    start_time = datetime.isoformat(start_time).replace("-", "/").replace("T", " ")
    end_time = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")
    end_time = end_time - timedelta(hours=4, seconds=1)
    end_time = datetime.isoformat(end_time).replace("-", "/").replace("T", " ")
    end_time = end_time[:10]
    date_padding = " 00:00:00"
    end_time = end_time + date_padding
    return start_time, end_time


def dispatch_half_hour_setup(start_time, end_time):
    start_time = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    start_time = datetime(
        year=start_time.year,
        month=start_time.month,
        day=start_time.day,
        hour=start_time.hour,
        minute=((start_time.minute // 30) * 30),
    )
    start_time = start_time.isoformat().replace("T", " ").replace("-", "/")
    return start_time, end_time


def fcas4s_finalise(data, start_time, table_name):
    for column in data.select_dtypes(["object"]).columns:
        data[column] = data[column].map(lambda x: x.strip())
    return data


def most_recent_records_before_start_time(data, start_time, table_name):
    date_col = defaults.primary_date_columns[table_name]
    group_cols = defaults.effective_date_group_col[table_name]
    records_from_after_start = data[data[date_col] >= start_time].copy()
    records_from_before_start = data[data[date_col] < start_time].copy()
    records_from_before_start = records_from_before_start.sort_values(date_col)
    if len(group_cols) > 0:
        most_recent_from_before_start = records_from_before_start.groupby(
            group_cols, as_index=False
        ).last()
        group_cols = group_cols + [date_col]
        most_recent_from_before_start = pd.merge(
            most_recent_from_before_start.loc[:, group_cols],
            records_from_before_start,
            "inner",
            group_cols,
        )
    else:
        most_recent_from_before_start = records_from_before_start.tail(1)

    mod_table = pd.concat(
        [records_from_after_start, most_recent_from_before_start], sort=False
    )
    return mod_table


def drop_duplicates_by_primary_key(data, start_time, table_name):
    data = data.drop_duplicates(defaults.table_primary_keys[table_name])
    return data


def convert_genconid_effectivedate_to_datetime_format(data, start_time, table_name):
    if "GENCONID_EFFECTIVEDATE" in data.columns:
        data["GENCONID_EFFECTIVEDATE"] = _parse_datetime(data["GENCONID_EFFECTIVEDATE"])
    return data
