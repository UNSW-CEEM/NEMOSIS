import downloader
import os
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import defaults
from time import time
import feather
from calendar import monthrange


def compile_generic(start_time, end_time, table_name, raw_data_location, date_filter, select_columns=None,
                    search='all', filter_cols=None, filter_values=None):
    if search == 'all':
        start_search = defaults.nem_data_model_start_time
    elif search == 'last':
        start_search = end_time
    elif search == 'start_to_end':
        start_search = start_time
    start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    start_search = datetime.strptime(start_search, '%Y/%m/%d %H:%M:%S')
    data_tables = []
    for year, month in year_and_month_gen(start_search, end_time):
        data = pre_compile_setup(table_name, month, year, raw_data_location, select_columns)
        if date_filter is not None:
            data = date_filter(data, start_time, end_time)
        data_tables.append(data)
    all_data = pd.concat(data_tables)
    if filter_cols is not None:
        all_data = filters(all_data, filter_cols, filter_values)
    return all_data


def compile_generic_fcas(start_time, end_time, table_name, raw_data_location, date_filter, select_columns=None,
                         search='all', filter_cols=None, filter_values=None):
    start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    data_tables = []
    for year, month, day, index in year_month_day_index_gen(start_time, end_time):
        data = pre_compile_setup_fcas(table_name, month, year, day, index, raw_data_location, select_columns)
        if date_filter is not None:
            data = date_filter(data, start_time, end_time)
        data_tables.append(data)
    all_data = pd.concat(data_tables)
    if filter_cols is not None:
        all_data = filters(all_data, filter_cols, filter_values)
    for column in all_data.select_dtypes(['object']).columns:
        all_data[column] = all_data[column].map(lambda x: x.strip())
    return all_data


def filter_on_start_and_end_date(data, start_time, end_time):
    data['START_DATE'] = pd.to_datetime(data['START_DATE'], format='%Y/%m/%d %H:%M:%S')
    data['END_DATE'] = np.where(data['END_DATE'] == '2999/12/31 00:00:00', '2100/12/31 00:00:00', data['END_DATE'])
    data['END_DATE'] = pd.to_datetime(data['END_DATE'], format='%Y/%m/%d %H:%M:%S')
    data = data[(data['START_DATE'] <= end_time) & (data['END_DATE'] > start_time)]
    return data


def filter_on_last_changed(data, start_time, end_time):
    data['LASTCHANGED'] = pd.to_datetime(data['LASTCHANGED'], format='%Y/%m/%d %H:%M:%S')
    data = data[(data['LASTCHANGED'] < end_time) & (data['LASTCHANGED'] >= start_time)]
    return data


def filter_on_effective_date(data, start_time, end_time):
    data['EFFECTIVEDATE'] = pd.to_datetime(data['EFFECTIVEDATE'], format='%Y/%m/%d %H:%M:%S')
    data = data[data['EFFECTIVEDATE'] <= end_time]
    return data


def filter_on_settlementdate(data, start_time, end_time):
    data['SETTLEMENTDATE'] = pd.to_datetime(data['SETTLEMENTDATE'], format='%Y/%m/%d %H:%M:%S')
    data = data[(data['SETTLEMENTDATE'] >= start_time) & (data['SETTLEMENTDATE'] < end_time)]
    return data


def filter_on_timestamp(data, start_time, end_time):
    data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'], format='%Y/%m/%d %H:%M:%S')
    data = data[(data['TIMESTAMP'] >= start_time) & (data['TIMESTAMP'] < end_time)]
    return data


def filter_on_interval_datetime(data, start_time, end_time):
    data['INTERVAL_DATETIME'] = pd.to_datetime(data['INTERVAL_DATETIME'], format='%Y/%m/%d %H:%M:%S')
    data = data[(data['INTERVAL_DATETIME'] >= start_time) & (data['INTERVAL_DATETIME'] < end_time)]
    return data


def filter_on_date_and_peroid(data, start_time, end_time):
    data = construct_interval_datetime_from_period_id(data)
    data = data[(data['SETTLEMENTDATE'] > start_time  - timedelta(minutes=30)) & (data['SETTLEMENTDATE'] < end_time)]
    return data


def filter_on_date_and_interval(data, start_time, end_time):
    data['SETTLEMENTDATE'] = pd.to_datetime(data['SETTLEMENTDATE'], format='%Y/%m/%d %H:%M:%S')
    data = data[(data['SETTLEMENTDATE'] >= start_time) & (data['SETTLEMENTDATE'] < end_time)]
    return data


def year_and_month_gen(start_time, end_time):

    if start_time.day == 1 and start_time.hour == 0 and start_time.minute == 0:
        if start_time.month == 1:
            start_time = start_time.replace(month=12)
            start_time = start_time.replace(year=start_time.year - 1)
        else:
            start_time = start_time.replace(month=start_time.month - 1)

    end_year = end_time.year
    start_year = start_time.year
    for year in range(start_year, end_year + 1):
        if year == end_year:
            end_month = end_time.month
        else:
            end_month = 12
        if year == start_year:
            start_month = start_time.month - 1
        else:
            start_month = 0
        for month in defaults.months[start_month:end_month]:
            yield year, month


def year_month_day_index_gen(start_time, end_time):

    if start_time.day == 1 and start_time.hour == 0 and start_time.minute == 0:
        if start_time.month == 1:
            start_time = start_time.replace(month=12)
            start_time = start_time.replace(year=start_time.year - 1)
        else:
            start_time = start_time.replace(month=start_time.month - 1)

    end_year = end_time.year
    start_year = start_time.year
    for year in range(start_year, end_year + 1):
        if year == end_year:
            end_month = end_time.month
        else:
            end_month = 12
        if year == start_year:
            start_month = start_time.month - 1
        else:
            start_month = 0
        for month in defaults.months[start_month:end_month]:
            for day in range(1, monthrange(int(year), int(month))[1] + 1):
                if ((day < start_time.day and int(month) == start_time.month and year == start_year)
                        or (day > end_time.day and int(month) == end_time.month and year == end_year)):
                    continue
                for hour in range(23, -1, -1):
                    if (hour < start_time.hour and int(month) == start_time.month and year == start_year
                        and start_time.day == day) \
                            or (hour > end_time.hour  and int(month) == end_time.month and year == end_year and
                                end_time.day == day):
                        continue
                    for minute in range(55, -1, -5):
                        index = str(hour).zfill(2) + str(minute).zfill(2)
                        yield year, month, str(day).zfill(2), index


def filters(data, filter_cols, filter_values):
    for filter_col, filter_values in zip(filter_cols, filter_values):
        if filter_values is not None:
            data = data[data[filter_col].isin(filter_values)]
    return data


def pre_compile_setup(name, month, year, raw_data_location, usecols):
    print('Compiling {} for {} / {}.'.format(name, month, year))
    # Add the year and month information to the generic AEMO file name
    filename_full = defaults.names[name] + "_" + str(year) + str(month) + "010000.CSV"
    # Check if file already exists in the raw data folder.
    path_and_name = raw_data_location + '/' + filename_full
    # Add the year and month information to the generic AEMO file name
    filename_full_feather = defaults.names[name] + "_" + str(year) + str(month) + "010000.feather"
    # Check if file already exists in the raw data folder.
    path_and_name_feather = raw_data_location + '/' + filename_full_feather
    data = pd.DataFrame(columns=usecols)

    if not os.path.isfile(path_and_name):
        downloader.run(year, month, defaults.aemo_data_url, filename_full, raw_data_location)

    if os.path.isfile(path_and_name_feather) and os.stat(path_and_name_feather).st_size > 2000:
        data = feather.read_dataframe(path_and_name_feather, usecols)
    elif os.path.isfile(path_and_name):
        headers = pd.read_csv(path_and_name, skiprows=[0], nrows=1).columns
        columns = [column for column in defaults.table_columns[name] if column in headers]
        data = pd.read_csv(path_and_name, skiprows=[0], dtype=str, usecols=columns)
        data = data[:-1]
        if os.path.isfile(path_and_name_feather):
            os.unlink(path_and_name_feather)
        data.to_feather(path_and_name_feather)
        if usecols is not None:
            for column in columns:
                if column not in usecols:
                    del data[column]

    return data


def pre_compile_setup_fcas(name, month, year, day, index, raw_data_location, usecols):
    print('Compiling {} for year {} month {} day {} index {}'.format(name, year, month, day, index))
    # Add the year and month information to the generic AEMO file name
    filename_full = defaults.names[name] + "_" + str(year) + str(month) + day + index + ".CSV"
    # Check if file already exists in the raw data folder.
    path_and_name = raw_data_location + '/' + filename_full
    # Add the year and month information to the generic AEMO file name
    filename_full_feather = defaults.names[name] + "_" + str(year) + str(month) + day + index + ".feather"
    # Check if file already exists in the raw data folder.
    path_and_name_feather = raw_data_location + '/' + filename_full_feather
    data = pd.DataFrame(columns=usecols)

    if not os.path.isfile(path_and_name):
        downloader.run_fcas4s(year, month, day, index, defaults.fcas_4_url, defaults.fcas_4_url_hist, filename_full,
                              raw_data_location)

    if os.path.isfile(path_and_name_feather) and os.stat(path_and_name_feather).st_size > 2000:
        data = feather.read_dataframe(path_and_name_feather, usecols)
    elif os.path.isfile(path_and_name):
        data = pd.read_csv(path_and_name, skiprows=[0], dtype=str, names=defaults.table_columns[name])
        if os.path.isfile(path_and_name_feather):
            os.unlink(path_and_name_feather)
        data.to_feather(path_and_name_feather)
    if usecols is not None:
        data = data.loc[:, usecols]
    return data


def construct_interval_datetime_from_period_id(data):
    data['SETTLEMENTDATE'] = np.vectorize(date_2_interval_datetime)(data['SETTLEMENTDATE'], data['PERIODID'])
    return data


def date_2_interval_datetime(date, period):
    datetime_obj = datetime.strptime(date, '%Y/%m/%d %H:%M:%S')
    datetime_obj = datetime_obj + timedelta(minutes=((float(period)-1)*30))
    return datetime_obj


