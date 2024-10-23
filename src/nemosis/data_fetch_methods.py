import logging
import os as _os
import glob as _glob
import pandas as _pd
from datetime import datetime as _datetime
from nemosis import filters as _filters
from nemosis import downloader as _downloader
from nemosis import processing_info_maps as _processing_info_maps
from nemosis import defaults as _defaults
from nemosis import custom_tables as _custom_tables
from nemosis import _infer_column_data_types
from nemosis.custom_errors import UserInputError, NoDataToReturn, DataMismatchError

logger = logging.getLogger(__name__)


def dynamic_data_compiler(
    start_time,
    end_time,
    table_name,
    raw_data_location,
    select_columns=None,
    filter_cols=None,
    filter_values=None,
    fformat="feather",
    keep_csv=True,
    parse_data_types=True,
    rebuild=False,
    **kwargs,
):
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
        select_columns (list or str): a list of columns to return or the string
                                      'all' to return all columns from AMEO raw
                                      data, 'all argument must be used will
                                      fformat='csv'. Default is None, will return a
                                      default set of columns.
        filter_cols (list): filter on columns.
        filter_values (tuple[list]): filter index n filter col such that values are
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
        rebuild (bool): If True then cache files are rebuilt
                        even if they exist already. False by default.
        **kwargs: additional arguments passed to the pd.to_{fformat}() function

    Returns:
        all_data (pd.Dataframe): All data concatenated.
    """
    if not _os.path.isdir(raw_data_location):
        raise UserInputError("The raw_data_location provided does not exist.")

    if table_name not in _defaults.dynamic_tables:
        raise UserInputError("Table name provided is not a dynamic table.")

    if fformat not in ["csv", "feather", "parquet"]:
        raise UserInputError("Argument fformat must be 'csv', 'feather' or 'parquet'")

    if select_columns == "all" and fformat != "csv":
        raise UserInputError(
            "If select_columns='all' is used fformat='csv' must be used."
        )

    (
        start_time,
        end_time,
        select_columns,
        date_filter,
        start_search,
    ) = _set_up_dynamic_compilers(table_name, start_time, end_time, select_columns)

    if filter_cols and not set(filter_cols).issubset(set(select_columns)):
        raise UserInputError(
            (
                "Filter columns not valid. They must be a part of "
                + "select_columns or the table defaults."
            )
        )

    logger.info(f"Compiling data for table {table_name}")

    start_time = _datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    end_time = _datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")
    start_search = _datetime.strptime(start_search, "%Y/%m/%d %H:%M:%S")
    data_tables = _dynamic_data_fetch_loop(
        start_search,
        start_time,
        end_time,
        table_name,
        raw_data_location,
        select_columns,
        date_filter,
        fformat=fformat,
        keep_csv=keep_csv,
        rebuild=rebuild,
        write_kwargs=kwargs,
    )
    if data_tables:
        all_data = _pd.concat(data_tables, sort=False)
        finalise_data = _processing_info_maps.finalise[table_name]
        if finalise_data is not None:
            for function in finalise_data:
                all_data = function(all_data, start_time, table_name)

        if parse_data_types:
            all_data = _infer_column_data_types(all_data)
        if filter_cols is not None:
            if not set(filter_cols).issubset(set(all_data.columns)):
                missing_columns = [
                    col for col in filter_cols if col not in all_data.columns
                ]
                UserInputError(f"Filter columns {missing_columns} not in data.")
            else:
                all_data = _filters.filter_on_column_value(
                    all_data, filter_cols, filter_values
                )
        logger.info(f"Returning {table_name}.")
        return all_data
    else:
        raise NoDataToReturn(
            (
                f"Compiling data for table {table_name} failed. "
                + "This probably because none of the requested data "
                + "could be download from AEMO. Check your internet "
                + "connection and that the requested data is archived on: "
                + "https://nemweb.com.au see nemosis.defaults for table specific urls."
            )
        )


def cache_compiler(
    start_time,
    end_time,
    table_name,
    raw_data_location,
    select_columns=None,
    fformat="feather",
    rebuild=False,
    keep_csv=False,
    **kwargs,
):
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
        select_columns (list or str): a list of columns to return, or the string
                                     'all' to return all columns from AMEO raw
                                     data. Determines which columns are included in cache file.
                                     Default is None, will return a default set of columns.
        fformat (string): "feather" or "parquet" for storage and access.
                          Stored parquet and feather files will store columns
                          as object type (compatbile with GUI use). For
                          type inference for a cache, use cache_compiler.
        rebuild (bool): If True then cache files are rebuilt
                        even if they exist already. False by default.
        keep_csv (bool): If True raw CSVs from AEMO are not deleted after
                         the cache is built. False by default
        **kwargs: additional arguments passed to the pd.to_{fformat}() function

    Returns:
        Nothing
    """
    if not _os.path.isdir(raw_data_location):
        raise UserInputError("The raw_data_location provided does not exist.")

    if table_name not in _defaults.dynamic_tables:
        raise UserInputError("Table name provided is not a dynamic table.")

    if fformat != "parquet" and fformat != "feather":
        raise UserInputError("Argument fformat must be 'feather' or 'parquet'")

    if select_columns is not None and not rebuild:
        raise UserInputError(
            (
                "The select_columns argument must be used with rebuild=True "
                + "to ensure the cache is built with the correct columns."
            )
        )

    logger.info(f"Caching data for table {table_name}")

    (
        start_time,
        end_time,
        select_columns,
        _,
        start_search,
    ) = _set_up_dynamic_compilers(table_name, start_time, end_time, select_columns)
    start_time = _datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    end_time = _datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")
    start_search = _datetime.strptime(start_search, "%Y/%m/%d %H:%M:%S")

    _dynamic_data_fetch_loop(
        start_search,
        start_time,
        end_time,
        table_name,
        raw_data_location,
        select_columns,
        date_filter=None,
        fformat=fformat,
        keep_csv=keep_csv,
        caching_mode=True,
        rebuild=rebuild,
        write_kwargs=kwargs,
    )
    return


def static_table(
    table_name,
    raw_data_location,
    select_columns=None,
    filter_cols=None,
    filter_values=None,
    update_static_file=False,
):
    """
    Downloads and compiles data for all static tables.
    Args:
        table_name (str): table as per Wiki.
        raw_data_location (str): directory to download and cache data to.
                                 existing data will be used if in this dir.
        select_columns (list or str): a list of columns to return or the string
                                      'all' to return all columns from AMEO raw
                                      data, 'all argument must be used will
                                      fformat='csv'. Default is None, will return a
                                      default set of columns.
        filter_cols (list): filter on columns.
        filter_values (tuple[list]): filter index n filter col such that values are
                              equal to index n filter value.
        update_static_file (bool): If True download latest version of file
                                   even if a version already exists.
                                   Default is False.

    Returns:
        data (pd.Dataframe)
    """
    if not _os.path.isdir(raw_data_location):
        raise UserInputError("The raw_data_location provided does not exist.")

    if table_name not in _defaults.static_tables:
        raise UserInputError("Table name provided is not a static table.")

    if filter_cols and not set(filter_cols).issubset(set(select_columns)):
        raise UserInputError(
            (
                "Filter columns not valid. They must be a part of "
                + "select_columns or the table defaults."
            )
        )

    logger.info(f"Retrieving static table {table_name}")
    path_and_name = _os.path.join(raw_data_location, _defaults.names[table_name])
    if not _os.path.isfile(path_and_name) or update_static_file:
        logger.info(f"Downloading data for table {table_name}")
        try:
            static_downloader_map[table_name](
                _defaults.static_table_url[table_name], path_and_name
            )
        except:
            raise NoDataToReturn(
                (
                    f"Compiling data for table {table_name} failed. "
                    + "This probably because none of the requested data "
                    + "could be download from AEMO. Check your internet "
                    + "connection and that the requested data is archived on: "
                    + "https://nemweb.com.au see nemosis.defaults for table specific urls."
                )
            )

    table = static_file_reader_map[table_name](path_and_name, table_name)

    if select_columns != "all":

        if select_columns is None:
            select_columns = _defaults.table_columns[table_name]

        read_cols = _validate_select_columns(table, select_columns, path_and_name)

        if not read_cols:
            raise DataMismatchError(
                (
                    f"None of columns {select_columns} are in {path_and_name}. "
                    "This may be caused by user input if the 'select_columns' "
                    "argument is being used, or by changed AEMO data formats. "
                    "This error can be avoided by using the argument select_columns='all'."
                )
            )

        table = table.loc[:, read_cols]

    for column in table.select_dtypes(["object"]).columns:
        table[column] = table[column].map(lambda x: _strip_if_string(x))

    if filter_cols is not None:
        if not set(filter_cols).issubset(set(table.columns)):
            missing_columns = [col for col in filter_cols if col not in table.columns]
            UserInputError(f"Filter columns {missing_columns} not in data.")
        else:
            table = _filters.filter_on_column_value(table, filter_cols, filter_values)

    static_table_finalisers = static_data_finaliser_map[table_name]
    for finaliser in static_table_finalisers:
        table = finaliser(table, table_name)

    return table


def _strip_if_string(x):
    if isinstance(x, str):
        x = x.strip()
    return x


def _get_read_function(fformat, table_type, day):
    if fformat == "feather":
        func = _pd.read_feather
    elif fformat == "parquet":
        func = _pd.read_parquet
    elif fformat == "csv":
        if table_type == "MMS":
            func = _read_mms_csv
        elif table_type == "FCAS":
            func = _read_fcas_causer_pays_csv
        elif table_type == "BIDDING":
            if day is None:
                func = _read_mms_csv
            else:
                func = _read_constructed_csv
        elif table_type in ['DAILY_REGION_SUMMARY', "NEXT_DAY_DISPATCHLOAD", "INTERMITTENT_GEN_SCADA"]:
            func = _read_constructed_csv
    return func


def _count_csv_lines(file_path):
    with open(file_path, 'rb') as f:
        return sum(1 for _ in f)


def _read_mms_csv(path_and_name, dtype=None, usecols=None, nrows=None, names=None):
    last_line_number = _count_csv_lines(path_and_name) - 1
    data = _pd.read_csv(
        path_and_name,
        skiprows=[0, last_line_number],
        dtype=dtype,
        usecols=usecols,
        nrows=nrows,
        names=names,
    )
    return data


def _read_constructed_csv(
    path_and_name, dtype=None, usecols=None, nrows=None, names=None
):
    data = _pd.read_csv(
        path_and_name, dtype=dtype, usecols=usecols, nrows=nrows, names=names
    )
    return data


def _read_fcas_causer_pays_csv(
    path_and_name, dtype=None, usecols=None, nrows=None, names=None
):
    data = _pd.read_csv(
        path_and_name, dtype=dtype, usecols=usecols, nrows=nrows, names=names
    )
    return data


def _read_static_csv(path_and_name, table_name):
    return _pd.read_csv(
        path_and_name, dtype=str, names=_defaults.table_columns[table_name]
    )


def _read_excel(path_and_name, table_name):
    xls = _pd.ExcelFile(path_and_name)
    for tab_option in _defaults.reg_exemption_list_tabs[table_name]:
        try:
            return _pd.read_excel(xls, tab_option, dtype=str)
        except ValueError as e:
            pass
    raise NoDataToReturn(f"""
        The excel file did not have any of the expected tabs {_defaults.reg_exemption_list_tabs[table_name]}.
        """)


def _finalise_excel_data(data, table_name):
    if table_name in _defaults.table_primary_keys.keys():
        primary_keys = _defaults.table_primary_keys[table_name]
        data = data.drop_duplicates(primary_keys)
    data = data.dropna(axis=0, how="all")
    data = data.dropna(axis=1, how="all")
    return data


def _finalise_generators_and_scheduled_loads(data, table_name):
    data = data.replace(to_replace=['', ' '], value='-')
    data = data.fillna('-')
    return data
    

def _finalise_csv_data(data, table_name):
    return data


static_downloader_map = {
    "VARIABLES_FCAS_4_SECOND": _downloader.download_csv,
    "ELEMENTS_FCAS_4_SECOND": _downloader.download_elements_file,
    "Generators and Scheduled Loads": _downloader.download_xl,
    "_downloader.download_xl": _downloader.download_xl,
}

static_file_reader_map = {
    "VARIABLES_FCAS_4_SECOND": _read_static_csv,
    "ELEMENTS_FCAS_4_SECOND": _read_static_csv,
    "Generators and Scheduled Loads": _read_excel,
    "FCAS Providers": _read_excel,
}

static_data_finaliser_map = {
    "VARIABLES_FCAS_4_SECOND": [_finalise_csv_data],
    "ELEMENTS_FCAS_4_SECOND": [_finalise_csv_data],
    "Generators and Scheduled Loads": [_finalise_excel_data, _finalise_generators_and_scheduled_loads],
    "FCAS Providers": [_finalise_excel_data],
}


def static_table_FCAS_elements_file(
    table_name,
    raw_data_location,
    select_columns=None,
    filter_cols=None,
    filter_values=None,
    update_static_file=False,
):
    table = static_table(
        table_name,
        raw_data_location,
        select_columns,
        filter_cols,
        filter_values,
        update_static_file,
    )
    return table


def static_table_xl(
    table_name,
    raw_data_location,
    select_columns=None,
    filter_cols=None,
    filter_values=None,
    update_static_file=False,
):
    table = static_table(
        table_name,
        raw_data_location,
        select_columns,
        filter_cols,
        filter_values,
        update_static_file,
    )
    return table


def _set_up_dynamic_compilers(table_name, start_time, end_time, select_columns):
    """
    Set up function for compilers that deal with dynamic data.

    Returns: start_time, end_time, select_columns, defaults_columns,
             date_filter, start_search, search_type.
    """
    # Generic setup common to all tables.
    default_cols = _defaults.table_columns[table_name]
    if select_columns is None:
        select_columns = default_cols

    # Pre loop setup, done at table type basis.
    date_filter = _processing_info_maps.filter[table_name]
    setup_function = _processing_info_maps.setup[table_name]
    if setup_function is not None:
        start_time, end_time = setup_function(start_time, end_time)

    search_type = _processing_info_maps.search_type[table_name]

    if search_type == "all":
        start_search = _defaults.nem_data_model_start_time
    elif search_type == "start_to_end":
        start_search = start_time
    elif search_type == "end":
        start_search = end_time
    return start_time, end_time, select_columns, date_filter, start_search


def _dynamic_data_fetch_loop(
    start_search,
    start_time,
    end_time,
    table_name,
    raw_data_location,
    select_columns,
    date_filter,
    fformat="feather",
    keep_csv=True,
    caching_mode=False,
    rebuild=False,
    write_kwargs={},
):
    """
    Loops through generated dates and checks if the appropriate file exists.

    If it does, reads in the data from the file and performs filtering.

    If it does not, check if the CSV exists:
    1. If it does, read the data in and write any required files
       (parquet or feather).
    2. If it does not, download data then do the same as 1.
    """
    data_tables = []

    table_type = _defaults.table_types[table_name]
    date_gen = _processing_info_maps.date_gen[table_type](start_search, end_time)

    for year, month, day, index in date_gen:
        check_for_next_data_chunk = True
        chunk = 0
        while check_for_next_data_chunk:
            chunk += 1

            filename_stub, full_filename, path_and_name = _create_filename(
                table_name, table_type, raw_data_location, fformat, day, month, year, chunk, index
            )

            if not (
                _glob.glob(full_filename) or _glob.glob(path_and_name + ".[cC][sS][vV]")
            ) or (not _glob.glob(path_and_name + ".[cC][sS][vV]") and rebuild):
                _download_data(
                    table_name,
                    table_type,
                    filename_stub,
                    day,
                    month,
                    year,
                    chunk,
                    index,
                    raw_data_location,
                )

            if _glob.glob(full_filename) and fformat != "csv" and not rebuild:
                if not caching_mode:
                    data = _get_read_function(fformat, table_type, day)(full_filename)
                else:
                    data = None
                    logger.info(
                        f"Cache for {table_name} in date range already compiled in"
                        + f" {raw_data_location}."
                    )

            elif _glob.glob(path_and_name + ".[cC][sS][vV]"):

                if select_columns != "all":
                    read_all_columns = False
                else:
                    read_all_columns = True

                if not caching_mode:
                    dtypes = "str"
                else:
                    dtypes = "all"

                csv_path_and_name = _glob.glob(path_and_name + ".[cC][sS][vV]")[0]

                csv_read_function = _get_read_function(
                    fformat="csv", table_type=table_type, day=day
                )
                data = _determine_columns_and_read_csv(
                    table_name,
                    csv_path_and_name,
                    csv_read_function,
                    read_all_columns=read_all_columns,
                    dtypes=dtypes,
                )

                if caching_mode:
                    data = _perform_column_selection(data, select_columns, full_filename)

                if data is not None and fformat != "csv":
                    _log_file_creation_message(fformat, table_name, year, month, day, index)
                    _write_to_format(data, fformat, full_filename, write_kwargs)

                if not keep_csv:
                    _os.remove(_glob.glob(path_and_name + ".[cC][sS][vV]")[0])
            else:
                data = None

            if not caching_mode and data is not None:

                if date_filter is not None:
                    data = date_filter(data, start_time, end_time)

                data = _perform_column_selection(data, select_columns, full_filename)

                data_tables.append(data)
            elif not caching_mode and chunk == 1:
                logger.warning(f"Loading data from {full_filename} failed.")

            if data is None or '#' not in filename_stub:
                check_for_next_data_chunk = False

    return data_tables


def _perform_column_selection(data, select_columns, full_filename):
    if select_columns != "all":
        keep_cols = _validate_select_columns(data, select_columns, full_filename)
        if keep_cols:
            data = data.loc[:, keep_cols]
        else:
            raise DataMismatchError(
                (
                    f"None of columns {select_columns} are in {full_filename}. "
                    "This may be caused by user input if the 'select_columns' "
                    "argument is being used, or by changed AEMO data formats. "
                    "This error can be avoided by using the argument select_columns='all'."
                )
            )
    return data


def _create_filename(
    table_name, table_type, raw_data_location, fformat, day, month, year, chunk, index
):
    """
    Gather:
    - the file name, based on file naming rules
    - potential file path (if data exists in cache)

    Returns: filename_stub, full_filename and path_and_name
    """
    filename_stub, path_and_name = _processing_info_maps.write_filename[table_type](
        table_name, month, year, day, chunk, index, raw_data_location
    )
    full_filename = path_and_name + f".{fformat}"
    return filename_stub, full_filename, path_and_name


def _validate_select_columns(data, select_columns, full_filename):
    """
    Checks whether select_columns are in the file. If at least one is,
    then it will return any of select_columns that are available as well as
    the date col (for date filtering).  If not, it will return an empty list.

    Returns: List
    """
    file_cols = data.columns
    available_cols = file_cols[file_cols.isin(select_columns)].tolist()
    rejected_cols = set(select_columns) - set(available_cols)
    if not available_cols:
        return []
    else:
        if rejected_cols:
            logger.warning(
                f"{rejected_cols} not in {full_filename}. "
                + f"Loading {available_cols}"
            )
        return available_cols


def _log_file_creation_message(fformat, table_name, year, month, day, index):
    logstr = f"Creating {fformat} file for " + f"{table_name}, {year}, {month}"
    if day is None:
        output = logstr
    else:
        output = logstr + f" {day}, {index}"

    logger.info(output)


def _determine_columns_and_read_csv(
    table_name, csv_file, read_csv_func, dtypes, read_all_columns=False
):
    """
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
    """
    if dtypes == "all":
        type = None
    else:
        type = str
    if (
        _defaults.table_types[table_name] in ["MMS", "BIDDING", "DAILY_REGION_SUMMARY", "NEXT_DAY_DISPATCHLOAD",
                                              "INTERMITTENT_GEN_SCADA"]
        and not read_all_columns
    ):
        headers = read_csv_func(csv_file, nrows=1).columns.tolist()
        columns = [
            column
            for column in _defaults.table_columns[table_name]
            if column in headers
        ]
        data = read_csv_func(csv_file, usecols=columns, dtype=type)
    elif (
        _defaults.table_types[table_name] in ["MMS", "BIDDING", "DAILY_REGION_SUMMARY", "NEXT_DAY_DISPATCHLOAD"]
        and read_all_columns
    ):
        data = read_csv_func(csv_file, dtype=type)
    else:
        columns = _defaults.table_columns[table_name]
        data = read_csv_func(csv_file, names=columns, dtype=type)
    return data


def _write_to_format(data, fformat, full_filename, write_kwargs):
    """
    Used by read_data_and_create_file
    Writes the DataFrame to a non-CSV format is a non_CSV format is specified.
    """
    write_function = {"feather": data.to_feather, "parquet": data.to_parquet}
    # Remove files of the same name - deals with case of corrupted files.
    if _os.path.isfile(full_filename) and fformat != "csv":
        _os.unlink(full_filename)
    # Write to required format
    if fformat == "feather":
        write_function[fformat](full_filename, **write_kwargs)
    elif fformat == "parquet":
        write_function[fformat](full_filename, index=False, **write_kwargs)
    return


def _download_data(
    table_name, table_type, filename_stub, day, month, year, chunk, index, raw_data_location
):
    """
    Dispatch table to downloader to be downloaded.

    Returns: nothing
    """
    if chunk == 1:
        if day is None:
            logger.info(
                f"Downloading data for table {table_name}, " + f"year {year}, month {month}"
            )
        elif index is None:
            logger.info(
                f"Downloading data for table {table_name}, "
                + f"year {year}, month {month}, day {day}"
            )
        else:
            logger.info(
                f"Downloading data for table {table_name}, "
                + f"year {year}, month {month}, day {day},"
                + f"time {index}."
            )

    _processing_info_maps.downloader[table_type](
        year, month, day, chunk, index, filename_stub, raw_data_location
    )
    return

  
# GUI wrappers and mappers below


def _dynamic_data_wrapper_for_gui(
    start_time, end_time, table, raw_data_location, columns, filter_cols, filter_values
):
    data = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name=table,
        raw_data_location=raw_data_location,
        select_columns=columns,
        filter_cols=filter_cols,
        filter_values=filter_values,
        parse_data_types=False,
    )
    return data


def _static_table_wrapper_for_gui(
    start_time,
    end_time,
    table_name,
    raw_data_location,
    select_columns=None,
    filter_cols=None,
    filter_values=None,
):
    table = static_table(
        table_name, raw_data_location, select_columns, filter_cols, filter_values
    )
    return table


_method_map = {
    "DISPATCHLOAD": _dynamic_data_wrapper_for_gui,
    "DISPATCHPRICE": _dynamic_data_wrapper_for_gui,
    "TRADINGLOAD": _dynamic_data_wrapper_for_gui,
    "TRADINGPRICE": _dynamic_data_wrapper_for_gui,
    "TRADINGREGIONSUM": _dynamic_data_wrapper_for_gui,
    "TRADINGINTERCONNECT": _dynamic_data_wrapper_for_gui,
    "DISPATCH_UNIT_SCADA": _dynamic_data_wrapper_for_gui,
    "DISPATCHCONSTRAINT": _dynamic_data_wrapper_for_gui,
    "DUDETAILSUMMARY": _dynamic_data_wrapper_for_gui,
    "PARTICIPANT": _dynamic_data_wrapper_for_gui,
    "DUDETAIL": _dynamic_data_wrapper_for_gui,
    "GENCONDATA": _dynamic_data_wrapper_for_gui,
    "SPDREGIONCONSTRAINT": _dynamic_data_wrapper_for_gui,
    "SPDCONNECTIONPOINTCONSTRAINT": _dynamic_data_wrapper_for_gui,
    "SPDINTERCONNECTORCONSTRAINT": _dynamic_data_wrapper_for_gui,
    "FCAS_4_SECOND": _dynamic_data_wrapper_for_gui,
    "ELEMENTS_FCAS_4_SECOND": _static_table_wrapper_for_gui,
    "VARIABLES_FCAS_4_SECOND": _static_table_wrapper_for_gui,
    "Generators and Scheduled Loads": _static_table_wrapper_for_gui,
    "FCAS Providers": _static_table_wrapper_for_gui,
    "BIDDAYOFFER_D": _dynamic_data_wrapper_for_gui,
    "BIDPEROFFER_D": _dynamic_data_wrapper_for_gui,
    "FCAS_4s_SCADA_MAP": _custom_tables.fcas4s_scada_match,
    "PLANTSTATS": _custom_tables.plant_stats,
    "DISPATCHINTERCONNECTORRES": _dynamic_data_wrapper_for_gui,
    "DISPATCHREGIONSUM": _dynamic_data_wrapper_for_gui,
    "LOSSMODEL": _dynamic_data_wrapper_for_gui,
    "LOSSFACTORMODEL": _dynamic_data_wrapper_for_gui,
    "MNSP_DAYOFFER": _dynamic_data_wrapper_for_gui,
    "MNSP_PEROFFER": _dynamic_data_wrapper_for_gui,
    "MNSP_INTERCONNECTOR": _dynamic_data_wrapper_for_gui,
    "INTERCONNECTOR": _dynamic_data_wrapper_for_gui,
    "INTERCONNECTORCONSTRAINT": _dynamic_data_wrapper_for_gui,
    "MARKET_PRICE_THRESHOLDS": _dynamic_data_wrapper_for_gui,
    "DAILY_REGION_SUMMARY": _dynamic_data_wrapper_for_gui,
    "NEXT_DAY_DISPATCHLOAD": _dynamic_data_wrapper_for_gui,
    "INTERMITTENT_GEN_SCADA": _dynamic_data_wrapper_for_gui,
    "ROOFTOP_PV_ACTUAL": _dynamic_data_wrapper_for_gui
}
