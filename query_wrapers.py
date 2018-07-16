import standard_queries
import defaults
import pandas as pd
import downloader
import os


def dispatch(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
             filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_settlementdate, select_columns,
                                             search='start_to_end', filter_cols=filter_cols,
                                             filter_values=filter_values)
    return table


def dispatch_interval_datetime(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
             filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_interval_datetime, select_columns,
                                             search='start_to_end', filter_cols=filter_cols,
                                             filter_values=filter_values)
    return table


def dispatch_constraint(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
             filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_date_and_interval, select_columns,
                                             search='start_to_end', filter_cols=filter_cols,
                                             filter_values=filter_values)
    table = table.rename(columns={'CONSTRAINTID': 'GENCONID', 'GENCONID_EFFECTIVEDATE': 'EFFECTIVEDATE',
                                  'GENCONID_VERSIONNO': 'VERSIONNO'})
    return table


def date_and_period(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
             filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_date_and_peroid, select_columns,
                                             search='start_to_end', filter_cols=filter_cols,
                                             filter_values=filter_values)
    return table


def dispatch_date(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                  filter_values=None):
    start_time = start_time[:10]
    date_padding = ' 00:00:00'
    start_time = start_time + date_padding
    end_time = end_time[:10]
    date_padding = ' 23:55:00'
    end_time = end_time + date_padding
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_settlementdate, select_columns,
                                             search='start_to_end', filter_cols=filter_cols,
                                             filter_values=filter_values)
    return table


def start_and_end(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                  filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_start_and_end_date, select_columns,
                                             search='last', filter_cols=filter_cols, filter_values=filter_values)
    group_cols = derive_group_cols(table_name, 'START_DATE')
    mod_table = most_recent_records_before_start_time(table, start_time, 'START_DATE', table_name, group_cols)
    return mod_table


def last_changed(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                 filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location, None, select_columns,
                                             search='last', filter_cols=filter_cols, filter_values=filter_values)
    return table


def effective_date(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                               filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_effective_date, select_columns,
                                             search='last', filter_cols=filter_cols, filter_values=filter_values)
    group_cols = defaults.effective_date_group_col[table_name]
    mod_table = most_recent_records_before_start_time(table, start_time, 'EFFECTIVEDATE', table_name, group_cols)
    return mod_table


def effective_date_search_all(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                               filter_values=None):
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location,
                                             standard_queries.filter_on_effective_date, select_columns,
                                             search='all', filter_cols=filter_cols, filter_values=filter_values)
    group_cols = defaults.effective_date_group_col[table_name]
    mod_table = most_recent_records_before_start_time(table, start_time, 'EFFECTIVEDATE', table_name, group_cols)
    return mod_table


def no_filter(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                               filter_values=None):
    table = standard_queries.compile_generic(start_time, end_time, table_name, raw_data_location, select_columns,
                                             search='all', filter_cols=filter_cols, filter_values=filter_values)
    return table


def fcas4s(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                               filter_values=None):
    table = standard_queries.compile_generic_fcas(start_time, end_time, table_name, raw_data_location,
                                                  standard_queries.filter_on_timestamp,select_columns,
                                                  search='start_to_end', filter_cols=filter_cols,
                                                  filter_values=filter_values)
    return table


def static_table(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                               filter_values=None):

    path_and_name = raw_data_location + '/' + defaults.names[table_name]
    if not os.path.isfile(path_and_name):
        downloader.download_csv(defaults.static_table_url[table_name], raw_data_location, path_and_name)

    table = pd.read_csv(raw_data_location + '/' + defaults.names[table_name], dtype=str,
                        names=defaults.table_columns[table_name])
    if select_columns is not None:
        table = table.loc[:, select_columns]
    for column in table.select_dtypes(['object']).columns:
        table[column] = table[column].map(lambda x: x.strip())
    return table


def static_table_xl(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                    filter_values=None):

    path_and_name = raw_data_location + '/' +defaults.names[table_name] + '.xls'
    if not os.path.isfile(path_and_name):
        downloader.download_xl(defaults.static_table_url[table_name], raw_data_location, path_and_name)

    xls = pd.ExcelFile(path_and_name)
    table = pd.read_excel(xls, 'Generators and Scheduled Loads', dtype=str)
    table = table.loc[:, select_columns]
    if filter_cols is not None:
        table = standard_queries.filters(table, filter_cols, filter_values)
    table = table.drop_duplicates(['DUID'])

    return table


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


