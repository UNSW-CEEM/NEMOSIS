import defaults
import pandas as pd


def dispatch_constraint_finalise(data, table_name, start_time):
    return data.rename(columns={'CONSTRAINTID': 'GENCONID', 'GENCONID_EFFECTIVEDATE': 'EFFECTIVEDATE',
                                'GENCONID_VERSIONNO': 'VERSIONNO'})


def dispatch_date_setup(start_time, end_time):
    start_time = start_time[:10]
    date_padding = ' 00:00:00'
    start_time = start_time + date_padding
    end_time = end_time[:10]
    date_padding = ' 23:55:00'
    end_time = end_time + date_padding
    return start_time, end_time


def start_and_end_finalise(data, table_name, start_time):
    group_cols = derive_group_cols(table_name, 'START_DATE')
    mod_table = most_recent_records_before_start_time(data, start_time, 'START_DATE', table_name, group_cols)
    return mod_table


def effective_date_finalise(data, table_name, start_time):
    group_cols = defaults.effective_date_group_col[table_name]
    mod_table = most_recent_records_before_start_time(data, start_time, 'EFFECTIVEDATE', table_name, group_cols)
    return mod_table


def fcas4s_finalise(data, table_name, start_time):
    for column in data.select_dtypes(['object']).columns:
        data[column] = data[column].map(lambda x: x.strip())
    return data


def most_recent_records_before_start_time(data, start_time, date_col, table_name, group_cols):
    records_from_after_start = data[data[date_col] > start_time].copy()
    records_from_before_start = data[data[date_col] <= start_time].copy()
    records_from_before_start = records_from_before_start.sort_values(date_col)
    most_recent_from_before_start = records_from_before_start.groupby(group_cols, as_index=False).last()
    group_cols = group_cols + [date_col]
    most_recent_from_before_start = pd.merge(most_recent_from_before_start.loc[:,group_cols], records_from_before_start, 'inner',
                                             group_cols)
    mod_table = pd.concat([records_from_after_start, most_recent_from_before_start])
    return mod_table


def derive_group_cols(table_name, date_col, also_exclude=None):
    exclude_from_group_cols = [date_col, 'VERSIONNO']
    if also_exclude is not None:
        exclude_from_group_cols.append(also_exclude)
    group_cols = [column for column in defaults.table_primary_keys[table_name] if column not in exclude_from_group_cols]
    return group_cols




