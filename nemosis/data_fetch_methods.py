import os as _os
import glob as _glob
import pandas as _pd
from datetime import datetime as _datetime
from nemosis import filters as _filters
from nemosis import downloader as _downloader
from nemosis import processing_info_maps as _processing_info_maps
from nemosis import defaults as _defaults
from nemosis import custom_tables as _custom_tables


def dynamic_data_compiler(start_time, end_time, table_name, raw_data_location,
                          select_columns=None, filter_cols=None,
                          filter_values=None, fformat='feather',
                          keep_csv=True, data_merge=True,
                          parse_data_types=True,
                          **kwargs):
    """
    Downloads and compiles data for all dynamic tables. For non-CSV formats,
    will save data typed as strings/objects. To save typed data (e.g.
    appropriate cols are Float or Int), use cache_compiler.
    Args:
        start_time (str): format 'yyyy/mm/dd HH:MM:SS'.
        end_time (str): format 'yyyy/mm/dd HH:MM:SS'.
        table_name (str): table as per Wiki.
        raw_data_location (str): directory to download and cache data to.
                                 existing data will be used if in this dir.
        select_columns (list): return select columns.
        filter_cols (list): filter on columns.
        filter_values (list): filter index n filter col such that values are
                              equal to index n filter value.
        fformat (string): "csv", "feather" or "parquet" for storage and access.
                          Stored parquet and feather files will store columns
                          as object type (compatbile with GUI use). For
                          type inference for a cache, use cache_compiler.
        keep_csv (bool): retains CSVs in cache.
        data_merge (bool): concatenate DataFrames and return one DataFrame.
                           If False, will not return any data.
        parse_data_types (bool): infers data types of columns when reading
                                 data. default True for API use.
        **kwargs: additional arguments passed to the pd.to_{fformat}() function

    Returns:
        all_data (pd.Dataframe): All data concatenated.
    """
    print('Compiling data for table {}.'.format(table_name))
    start_time, end_time, select_columns,\
        date_filter, start_search, search_type =\
        _set_up_dynamic_compilers(table_name, start_time, end_time,
                                  select_columns)
    start_time = _datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time = _datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    start_search = _datetime.strptime(start_search, '%Y/%m/%d %H:%M:%S')

    data_tables = _dynamic_data_fetch_loop(start_search, start_time, end_time,
                                           table_name, raw_data_location,
                                           select_columns, date_filter,
                                           search_type, fformat=fformat,
                                           keep_csv=keep_csv,
                                           data_merge=data_merge,
                                           write_kwargs=kwargs)
    if data_merge:
        all_data = _pd.concat(data_tables, sort=False)
        finalise_data = _processing_info_maps.finalise[table_name]
        if finalise_data is not None:
            for function in finalise_data:
                all_data = function(all_data, start_time, table_name)

        if parse_data_types:
            all_data = _infer_column_data_types(all_data)
        if filter_cols is not None:
            all_data = _filters.filter_on_column_value(all_data, filter_cols,
                                                       filter_values)
        return all_data


def cache_compiler(start_time, end_time, table_name, raw_data_location,
                   fformat='feather', **kwargs):
    """
    Downloads and compiles typed data for all dynamic tables as either parquet
    or feather format (i.e. will save data with columns as appropriate data
    types such as Int, Float or Datetime=False).

    Should not be used in a cache
    that is used to store csvs (such as the cache for the GUI).

    Args:
        start_time (str): format 'yyyy/mm/dd HH:MM:SS'.
        end_time (str): format 'yyyy/mm/dd HH:MM:SS'.
        table_name (str): table as per Wiki.
        raw_data_location (str): directory to download and cache data to.
                                 existing data will be used if in this dir.
        fformat (string): "feather" or "parquet" for storage and access.
                          Stored parquet and feather files will store columns
                          as object type (compatbile with GUI use). For
                          type inference for a cache, use cache_compiler.
        **kwargs: additional arguments passed to the pd.to_{fformat}() function

    Returns:
        Nothing
    """
    if fformat != "parquet" and fformat != "feather":
        print("Argument fformat must be 'feather' or 'parquet'")
        return
    print(f'Caching data for table {table_name}')

    start_time, end_time, select_columns, _, start_search, _ =\
        _set_up_dynamic_compilers(table_name, start_time, end_time,
                                  None)
    start_time = _datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time = _datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    start_search = _datetime.strptime(start_search, '%Y/%m/%d %H:%M:%S')
    read_function = {'feather': _pd.read_feather,
                     'csv': _pd.read_csv,
                     'parquet': _pd.read_parquet}
    table_type = _defaults.table_types[table_name]
    date_gen = _processing_info_maps.date_gen[table_type](start_search,
                                                          end_time)
    for year, month, day, index in date_gen:
        data = None
        filename_stub, full_filename,\
            path_and_name = _create_filename(table_name, table_type,
                                             raw_data_location,
                                             fformat, day, month,
                                             year, index)
        if _glob.glob(full_filename):
            data = read_function[fformat](full_filename,
                                          columns=select_columns)
        else:
            retain_csv = False
            _download_data(table_name, table_type, filename_stub, day, month,
                           year, index, raw_data_location)
            data, printstr =\
                _read_data_and_create_file(read_function, fformat, table_name,
                                           day, month, year, index,
                                           path_and_name, full_filename,
                                           retain_csv, select_columns, kwargs,
                                           dtypes="all")
    return


def static_table(table_name, raw_data_location, select_columns=None,
                 filter_cols=None, filter_values=None):
    print('Retrieving static table {}.'.format(table_name))
    path_and_name = raw_data_location + '/' + _defaults.names[table_name]
    if not _os.path.isfile(path_and_name):
        print('Downloading data for table {}.'.format(table_name))
        _downloader.download_csv(_defaults.static_table_url[table_name],
                                 raw_data_location, path_and_name)

    table = _pd.read_csv(raw_data_location + '/' + _defaults.names[table_name],
                         dtype=str,
                         names=_defaults.table_columns[table_name])
    if select_columns is not None:
        table = table.loc[:, select_columns]
    for column in table.select_dtypes(['object']).columns:
        table[column] = table[column].map(lambda x: x.strip())

    if filter_cols is not None:
        table = _filters.filter_on_column_value(table, filter_cols,
                                                filter_values)
    return table


def static_table_FCAS_elements_file(table_name, raw_data_location,
                                    select_columns=None, filter_cols=None,
                                    filter_values=None):
    print('Retrieving static table {}.'.format(table_name))
    path_and_name = raw_data_location + '/' + _defaults.names[table_name]
    if not _os.path.isfile(path_and_name):
        print('Downloading data for table {}.'.format(table_name))
        _downloader.download_elements_file(
            _defaults.static_table_url[table_name],
            raw_data_location, path_and_name
        )
    table = _pd.read_csv(raw_data_location + '/' + _defaults.names[table_name],
                         dtype=str,
                         names=_defaults.table_columns[table_name])
    if select_columns is not None:
        table = table.loc[:, select_columns]
    for column in table.select_dtypes(['object']).columns:
        table[column] = table[column].map(lambda x: x.strip())
    if filter_cols is not None:
        table = _filters.filter_on_column_value(table, filter_cols,
                                                filter_values)
    return table


def static_table_xl(table_name, raw_data_location, select_columns=None,
                    filter_cols=None, filter_values=None):
    path_and_name = (raw_data_location + '/'
                     + _defaults.names[table_name] + '.xls')
    print('Retrieving static table {}.'.format(table_name))
    if not _os.path.isfile(path_and_name):
        print('Downloading data for table {}.'.format(table_name))
        _downloader.download_xl(_defaults.static_table_url[table_name],
                                raw_data_location, path_and_name)
    xls = _pd.ExcelFile(path_and_name)
    table = _pd.read_excel(xls, _defaults.reg_exemption_list_tabs[table_name],
                           dtype=str)
    if select_columns is not None:
        table = table.loc[:, select_columns]
    if filter_cols is not None:
        table = _filters.filter_on_column_value(table, filter_cols,
                                                filter_values)
    if table_name in _defaults.table_primary_keys.keys():
        primary_keys = _defaults.table_primary_keys[table_name]
        table = table.drop_duplicates(primary_keys)
    table.dropna(axis=0, how='all', inplace=True)
    table.dropna(axis=1, how='all', inplace=True)
    return table


def _set_up_dynamic_compilers(table_name, start_time, end_time,
                              select_columns):
    '''
    Set up function for compilers that deal with dynamic data.

    Returns: start_time, end_time, select_columns, date_filter,
             start_search, search_type.
    '''
    # Generic setup common to all tables.
    if select_columns is None:
        select_columns = _defaults.table_columns[table_name]

    # Pre loop setup, done at table type basis.
    date_filter = _processing_info_maps.filter[table_name]
    setup_function = _processing_info_maps.setup[table_name]
    if setup_function is not None:
        start_time, end_time = setup_function(start_time, end_time)

    search_type = _processing_info_maps.search_type[table_name]

    if search_type == 'all':
        start_search = _defaults.nem_data_model_start_time
    elif search_type == 'start_to_end':
        start_search = start_time
    elif search_type == 'end':
        start_search = end_time
    return start_time, end_time, select_columns,\
        date_filter, start_search, search_type


def _dynamic_data_fetch_loop(start_search, start_time, end_time, table_name,
                             raw_data_location, select_columns,
                             date_filter, search_type, fformat='feather',
                             keep_csv=True, data_merge=True,
                             write_kwargs={}):
    '''
    Loops through generated dates and checks if the appropriate file exists.

    If it does, reads in the data from the file and performs filtering.

    If it does not, check if the CSV exists:
    1. If it does, read the data in and write any required files
       (parquet or feather).
    2. If it does not, download data then do the same as 1.
    '''
    data_tables = []
    read_function = {'feather': _pd.read_feather,
                     'csv': _pd.read_csv,
                     'parquet': _pd.read_parquet}
    table_type = _defaults.table_types[table_name]
    date_gen = _processing_info_maps.date_gen[table_type](start_search,
                                                          end_time)
    for year, month, day, index in date_gen:
        data = None
        filename_stub, full_filename,\
            path_and_name = _create_filename(table_name, table_type,
                                             raw_data_location,
                                             fformat, day, month,
                                             year, index)
        if _glob.glob(full_filename):
            data = read_function[fformat](full_filename,
                                          columns=select_columns)
        elif _glob.glob(path_and_name + '.[cC][sS][vV]'):
            data, printstr =\
                _read_data_and_create_file(read_function, fformat, table_name,
                                           day, month, year, index,
                                           path_and_name, full_filename,
                                           keep_csv, select_columns,
                                           write_kwargs)
        else:
            _download_data(table_name, table_type, filename_stub, day, month,
                           year, index, raw_data_location)
            data, printstr =\
                _read_data_and_create_file(read_function, fformat, table_name,
                                           day, month, year, index,
                                           path_and_name, full_filename,
                                           keep_csv, select_columns,
                                           write_kwargs)
        if data is not None:
            if date_filter is not None:
                data = date_filter(data, start_time, end_time)
            if data_merge:
                data_tables.append(data)
        else:
            print(printstr + ' FAILED')

    return data_tables


def _create_filename(table_name, table_type, raw_data_location, fformat,
                     day, month, year, index):
    '''
    Gather:
    - the file name, based on file naming rules
    - potential file path (if data exists in cache)

    Returns: filename_stub, full_filename and path_and_name
    '''
    filename_stub, path_and_name = \
        _processing_info_maps.write_filename[table_type](table_name, month,
                                                         year, day, index,
                                                         raw_data_location)
    full_filename = path_and_name + f'.{fformat}'
    return filename_stub, full_filename, path_and_name


def _read_data_and_create_file(read_function, fformat, table_name,
                               day, month, year, index,
                               path_and_name, full_filename, keep_csv,
                               select_columns, write_kwargs, dtypes="str"):
    '''
    Reads CSV file, returns data from file and write to appropriate fformat.
    Data is returned with selected columns, but data retains all columns.

    If a CSV is not available, will print compilation failed and return None.

    Returns: data, printstr or None, printstr
    '''
    printstr = (f'Creating {fformat} file for '
                + f'{table_name}, {year}, {month}')
    if day is None:
        output = (printstr)
    else:
        output = (printstr + f' {day}, {index}')
    print(output)
    try:
        csv_file = _glob.glob(path_and_name + '.[cC][sS][vV]')[0]
    except IndexError:
        return None, printstr
    read_csv_func = read_function['csv']
    data, columns = _determine_columns_and_read_csv(table_name,
                                                    csv_file,
                                                    read_csv_func,
                                                    dtypes)
    if fformat != 'csv':
        _write_to_format(data, fformat, full_filename, write_kwargs)
    if not keep_csv:
        _os.remove(csv_file)
    if select_columns is not None:
        for column in columns:
            if column not in select_columns:
                del data[column]
    return data, printstr


def _determine_columns_and_read_csv(table_name, csv_file, read_csv_func, 
                                    dtypes):
    '''
    Used by read_data_and_create_file
    Determining columns:
    - If the table is an MMS table, check header of CSV for actual columns.
      Then remove any columns from lookup table if not in actual columns.
      This is done as AEMO has added and removed columns over time.
    - If the table is not an MMS table, use columns from the lookup table.

    Reading csv:
    - To preserve compatability with previous versions of NEMOSIS and
      thus any existing data caches, read in all columns as strings.

    Returns: data, columns
    '''
    if dtypes == "all":
        type = None
    else:
        type = str
    if _defaults.table_types[table_name] == 'MMS':
        headers = read_csv_func(csv_file, skiprows=[0],
                                nrows=1).columns.tolist()
        columns = [column for column in _defaults.table_columns[table_name]
                   if column in headers]
        data = read_csv_func(csv_file, skiprows=[0], usecols=columns,
                             dtype=type)
        data = data[:-1]
    else:
        columns = _defaults.table_columns[table_name]
        data = read_csv_func(csv_file, skiprows=[0], names=columns, dtype=type)
    return data, columns


def _write_to_format(data, fformat, full_filename, write_kwargs):
    '''
    Used by read_data_and_create_file
    Writes the DataFrame to a non-CSV format is a non_CSV format is specified.
    '''
    write_function = {'feather': data.to_feather,
                      'parquet': data.to_parquet}
    # Remove files of the same name - deals with case of corrupted files.
    if _os.path.isfile(full_filename) and fformat != 'csv':
        _os.unlink(full_filename)
    # Write to required format
    if fformat == 'feather':
        write_function[fformat](full_filename, **write_kwargs)
    elif fformat == 'parquet':
        write_function[fformat](full_filename, index=False,
                                **write_kwargs)
    return


def _download_data(table_name, table_type, filename_stub,
                   day, month, year, index, raw_data_location):
    '''
    Dispatch table to downloader to be downloaded.

    Returns: nothing
    '''
    if day is None:
        print(f'Downloading data for table {table_name}, '
              + f'year {year}, month {month}')
    else:
        print(f'Downloading data for table {table_name}, '
              + f'year {year}, month {month}, day {day},'
              + f'time {index}.')

    _processing_info_maps.downloader[table_type](year, month, day,
                                                 index, filename_stub,
                                                 raw_data_location)
    return


def _infer_column_data_types(data):
    """
    Infer datatype of DataFrame assuming inference need only be carried out
    for any columns with dtype "object". Adapted from StackOverflow.

    If the column is an object type, attempt conversions to (in order of):
    1. datetime
    2. numeric

    Returns: Data with inferred types.
    """
    def _get_series_type(series):
        if series.dtype == "object":
            try:
                col_new = _pd.to_datetime(series)
                return col_new
            except Exception as e:
                try:
                    col_new = _pd.to_numeric(series)
                    return col_new
                except Exception as e:
                    return series
        else:
            return series

    for col in data:
        series = data[col]
        typed = _get_series_type(series)
        data[col] = typed
    return data

# GUI wrappers and mappers below


def _dynamic_data_wrapper_for_gui(start_time, end_time, table,
                                  raw_data_location, columns, filter_cols,
                                  filter_values):
    data = dynamic_data_compiler(start_time=start_time, end_time=end_time,
                                 table_name=table,
                                 raw_data_location=raw_data_location,
                                 select_columns=columns,
                                 filter_cols=filter_cols,
                                 filter_values=filter_values,
                                 parse_data_types=False)
    return data


def _static_table_wrapper_for_gui(start_time, end_time, table_name,
                                  raw_data_location, select_columns=None,
                                  filter_cols=None, filter_values=None):
    table = static_table(table_name, raw_data_location, select_columns,
                         filter_cols, filter_values)
    return table


def _static_table_FCAS_elements_file_wrapper_for_gui(start_time,
                                                     end_time, table_name,
                                                     raw_data_location,
                                                     select_columns=None,
                                                     filter_cols=None,
                                                     filter_values=None):
    table = static_table_FCAS_elements_file(table_name, raw_data_location,
                                            select_columns, filter_cols,
                                            filter_values)
    return table


def _static_table_xl_wrapper_for_gui(start_time, end_time, table_name,
                                     raw_data_location, select_columns=None,
                                     filter_cols=None, filter_values=None):
    table = static_table_xl(table_name, raw_data_location, select_columns,
                            filter_cols, filter_values)
    return table


_method_map = {'DISPATCHLOAD': _dynamic_data_wrapper_for_gui,
               'DISPATCHPRICE': _dynamic_data_wrapper_for_gui,
               'TRADINGLOAD': _dynamic_data_wrapper_for_gui,
               'TRADINGPRICE': _dynamic_data_wrapper_for_gui,
               'TRADINGREGIONSUM': _dynamic_data_wrapper_for_gui,
               'TRADINGINTERCONNECT': _dynamic_data_wrapper_for_gui,
               'DISPATCH_UNIT_SCADA': _dynamic_data_wrapper_for_gui,
               'DISPATCHCONSTRAINT': _dynamic_data_wrapper_for_gui,
               'DUDETAILSUMMARY': _dynamic_data_wrapper_for_gui,
               'DUDETAIL': _dynamic_data_wrapper_for_gui,
               'GENCONDATA': _dynamic_data_wrapper_for_gui,
               'SPDREGIONCONSTRAINT': _dynamic_data_wrapper_for_gui,
               'SPDCONNECTIONPOINTCONSTRAINT': _dynamic_data_wrapper_for_gui,
               'SPDINTERCONNECTORCONSTRAINT': _dynamic_data_wrapper_for_gui,
               'FCAS_4_SECOND': _dynamic_data_wrapper_for_gui,
               'ELEMENTS_FCAS_4_SECOND':
               _static_table_FCAS_elements_file_wrapper_for_gui,
               'VARIABLES_FCAS_4_SECOND': _static_table_wrapper_for_gui,
               'Generators and Scheduled Loads':
               _static_table_xl_wrapper_for_gui,
               'FCAS Providers': _static_table_xl_wrapper_for_gui,
               'BIDDAYOFFER_D': _dynamic_data_wrapper_for_gui,
               'BIDPEROFFER_D': _dynamic_data_wrapper_for_gui,
               'FCAS_4s_SCADA_MAP': _custom_tables.fcas4s_scada_match,
               'PLANTSTATS': _custom_tables.plant_stats,
               'DISPATCHINTERCONNECTORRES': _dynamic_data_wrapper_for_gui,
               'DISPATCHREGIONSUM': _dynamic_data_wrapper_for_gui,
               'LOSSMODEL': _dynamic_data_wrapper_for_gui,
               'LOSSFACTORMODEL': _dynamic_data_wrapper_for_gui,
               'MNSP_DAYOFFER': _dynamic_data_wrapper_for_gui,
               'MNSP_PEROFFER': _dynamic_data_wrapper_for_gui,
               'MNSP_INTERCONNECTOR': _dynamic_data_wrapper_for_gui,
               'INTERCONNECTOR': _dynamic_data_wrapper_for_gui,
               'INTERCONNECTORCONSTRAINT': _dynamic_data_wrapper_for_gui,
               'MARKET_PRICE_THRESHOLDS': _dynamic_data_wrapper_for_gui}