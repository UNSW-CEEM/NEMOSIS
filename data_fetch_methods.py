import filters
import defaults
import pandas as pd
import downloader
import maps
import os
from datetime import datetime, timedelta
import feather


def main_data_compiling_loop(start_time, end_time, table_name, raw_data_location, select_columns=None, filter_cols=None,
                             filter_values=None):

    # Generic setup common to all tables.
    if select_columns is None:
        select_columns = defaults.table_columns[table_name]

    # Pre loop setup, done at table type basis.
    date_filter = maps.filter_map[table_name]
    setup_function = maps.setup_map[table_name]
    if setup_function is not None:
        start_time, end_time = setup_function(start_time, end_time)
    else:
        date_filter = None

    search_type = maps.search_type_map[table_name]
    if search_type == 'all':
        start_search = defaults.nem_data_model_start_time
    elif search_type == 'last':
        start_search = end_time
    elif search_type == 'start_to_end':
        start_search = start_time

    start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    start_search = datetime.strptime(start_search, '%Y/%m/%d %H:%M:%S')

    data_tables = []

    for year, month, day, index in maps.date_gen_map[table_name](start_time, end_time):
        # Write the file names and paths for where the data is stored in the cache.
        filename_full, path_and_name, filename_full_feather, path_and_name_feather = \
            maps.write_filename_map[table_name](table_name, month, year, day, index, raw_data_location)

        # If the data needed is not in the cache then download it.
        if not os.path.isfile(path_and_name):
            maps.downloader_map[table_name](year, month, day, index, filename_full, raw_data_location)

        # If the data exists in feather format the read in the data. If it only exists as a csv then read in from the
        # csv and save to feather.
        if os.path.isfile(path_and_name_feather) and os.stat(path_and_name_feather).st_size > 2000:
            data = feather.read_dataframe(path_and_name_feather, select_columns)
        elif os.path.isfile(path_and_name):
            # Check what headers the data has.
            headers = pd.read_csv(path_and_name, skiprows=[0], nrows=1).columns

            if defaults.table_types[table_name] == 'MMS':
                # Remove columns from the table column list if they are not in the header, this deals with the fact AEMO
                # has added and removed columns over time.
                columns = [column for column in defaults.table_columns[table_name] if column in headers]
                # Read the data from a csv.
                data = pd.read_csv(path_and_name, skiprows=[0], dtype=str, usecols=columns)
                data = data[:-1]
            elif defaults.table_types[table_name] == 'FCAS':
                data = pd.read_csv(path_and_name, skiprows=[0], dtype=str, names=defaults.table_columns[table_name])

            # Remove feather files of the same name, deals with case of corrupted files.
            if os.path.isfile(path_and_name_feather):
                os.unlink(path_and_name_feather)
            # Write to feather file.
            data.to_feather(path_and_name_feather)
            # Delete any columns in data that were not explicitly selected.
            if select_columns is not None:
                for column in columns:
                    if column not in select_columns:
                        del data[column]

            # Filter by the start and end time.
            if date_filter is not None:
                data = date_filter(data, start_time, end_time)

            data_tables.append(data)

        all_data = pd.concat(data_tables)

        all_data = maps.finalise_map[table_name](all_data, table_name, start_time)

        if filter_cols is not None:
            all_data = filters.filter_on_column_value(all_data, filter_cols, filter_values)

    return all_data


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
        table = filters.filters(table, filter_cols, filter_values)
    table = table.drop_duplicates(['DUID'])

    return table