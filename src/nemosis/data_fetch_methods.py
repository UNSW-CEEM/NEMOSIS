import logging
import os as _os
import glob as _glob
import pandas as _pd
from datetime import datetime as _datetime, timedelta as _timedelta
from nemosis import downloader as _downloader
from nemosis.filters import filter_on_column_value as _filter_on_column_value
from nemosis import processing_info_maps as _processing_info_maps
from nemosis import date_generators as _date_generators
from nemosis import defaults as _defaults
from nemosis import query_wrappers as _query_wrappers
from nemosis.value_parser import _infer_column_data_types
from nemosis.date_generators import parse_datetime_py as _parse_datetime_py
from nemosis.custom_errors import UserInputError, NoDataToReturn, DataMismatchError

logger = logging.getLogger(__name__)


def _validate_raw_data_location(raw_data_location):
    """Validate (and create-if-needed) the user's raw_data_location.

    Common to dynamic_data_compiler, cache_compiler, and static_table.
    Rejects None (typo / unset config) and paths that exist as files
    (clearer than the downstream 'not a directory' error). If the path
    doesn't yet exist as a directory, create it — first-run UX is
    smoother than forcing every caller to mkdir up front.
    """
    if raw_data_location is None:
        raise UserInputError("The raw_data_location provided is None.")
    if _os.path.isfile(raw_data_location):
        raise UserInputError(
            f"The raw_data_location {raw_data_location} provided "
            f"exists as a file, not a directory."
        )
    if not _os.path.isdir(raw_data_location):
        _os.makedirs(raw_data_location)


def dynamic_data_compiler(
    start_time,
    end_time,
    table_name,
    raw_data_location,
    select_columns=None,
    filter_cols=None,
    filter_values=None,
    fformat="parquet",
    keep_csv=False,
    keep_zip=True,
    parse_data_types=True,
    rebuild=False,
    **kwargs,
):
    """
    Downloads and compiles data for all dynamic tables. For non-CSV formats,
    will save data typed as strings/objects. To save typed data (e.g.
    appropriate cols are Float or Int), use cache_compiler.
    Args:
        start_time (datetime): A native datetime. (Timezone unaware) 
                               For legacy reasons, may be a string 
                               of format 'yyyy/mm/dd HH:MM:SS'.
        end_time (datetime):   A native datetime. (Timezone unaware) 
                               For legacy reasons, may be a string 
                               of format 'yyyy/mm/dd HH:MM:SS'.
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
        keep_csv (bool): If True, raw CSVs from AEMO are retained in the
                         cache directory after the typed feather/parquet
                         is written. False by default — lean cache.
        keep_zip (bool): If True, downloaded AEMO archive zips are
                         retained in the cache directory after extraction.
                         True by default — keeps the compressed archive so
                         subsequent runs (e.g. cache rebuilds, format
                         changes) don't have to re-download from AEMO
                         (see #56). Set False for a leaner cache when
                         re-download cost is not a concern.
        data_merge (bool): concatenate DataFrames and return one DataFrame.
                           If False, will not return any data.
        parse_data_types (bool): infers data types of columns when reading
                                 data. default True for API use.
        rebuild (bool): If True then cache files are rebuilt
                        (redownload, re-unzip, re-convert)
                        even if they exist already. False by default.
        **kwargs: additional arguments passed to the pd.to_{fformat}() function

    Returns:
        all_data (pd.Dataframe): All data concatenated.
    """

    _validate_raw_data_location(raw_data_location)

    if table_name not in _defaults.dynamic_tables:
        raise UserInputError("Table name provided is not a dynamic table.")

    if fformat not in ["csv", "feather", "parquet"]:
        raise UserInputError("Argument fformat must be 'csv', 'feather' or 'parquet'")

    if select_columns == "all" and fformat != "csv":
        raise UserInputError(
            "If select_columns='all' is used fformat='csv' must be used."
        )

    _validate_user_select_columns(select_columns, table_name)
    _validate_user_select_columns_includes_pk(select_columns, table_name)
    _validate_filter_args(filter_cols, filter_values)
    _validate_time_window(start_time, end_time)

    # Remember whether the user explicitly asked for columns, so we can do a
    # post-load check below. Columns inherited from defaults are allowed to
    # silently be missing from old data vintages (e.g. RAISE1SECRRP before
    # 1-sec FCAS existed); columns the user explicitly typed should not be.
    user_select_columns = select_columns

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

    # cast from string to datetime, if not already datetime
    start_time = _parse_datetime_py(start_time, midnight='start')
    end_time = _parse_datetime_py(end_time, midnight='end')
    start_search = _parse_datetime_py(start_search, midnight='start')
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
        keep_zip=keep_zip,
        rebuild=rebuild,
        write_kwargs=kwargs,
        user_select_columns=user_select_columns,
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
                # Was previously `UserInputError(...)` (no `raise`) — a
                # silent no-op that returned unfiltered data. Now
                # properly raised; mostly unreachable thanks to
                # `_check_loaded_select_columns` below, but kept as
                # defence in depth (e.g. filter_cols came from defaults
                # because user passed select_columns=None, and the
                # default filter column isn't in this AEMO file
                # vintage).
                raise UserInputError(
                    f"Filter columns {missing_columns} not in data for "
                    f"table {table_name}. Available columns: "
                    f"{sorted(all_data.columns)}."
                )
            else:
                all_data = _filter_on_column_value(
                    all_data, filter_cols, filter_values
                )
        _check_loaded_select_columns(all_data, user_select_columns, table_name)
        # Reset index so callers can do .loc[0] / .iloc[0] on the returned
        # DataFrame; without this, rows carry the indices from the underlying
        # parquet/feather/CSV (e.g. 20162, 20167, ...).
        all_data = all_data.reset_index(drop=True)
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
    fformat="parquet",
    rebuild=False,
    keep_csv=False,
    keep_zip=True,
    **kwargs,
):
    """
    Downloads and compiles typed data for all dynamic tables as either parquet
    or feather format (i.e. will save data with columns as appropriate data
    types such as Int, Float or Datetime=False).

    Should not be used in a cache
    that is used to store csvs (such as the cache for the GUI).

    Args:
        start_time (datetime): A native datetime. (Timezone unaware) 
                               For legacy reasons, may be a string 
                               of format 'yyyy/mm/dd HH:MM:SS'.
        end_time (datetime):   A native datetime. (Timezone unaware) 
                               For legacy reasons, may be a string 
                               of format 'yyyy/mm/dd HH:MM:SS'.
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
                        (redownload, re-unzip, re-convert)
                        even if they exist already. False by default.
        keep_csv (bool): If True, raw CSVs from AEMO are retained
                         alongside the typed feather/parquet after the
                         cache is built. False by default — lean cache.
        keep_zip (bool): If True, downloaded AEMO archive zips are
                         retained in the cache directory after
                         extraction. True by default — keeps the
                         compressed archive so subsequent runs (e.g.
                         cache rebuilds, format changes) don't have to
                         re-download from AEMO (see #56). Set False
                         for a leaner cache when re-download cost is
                         not a concern.
        **kwargs: additional arguments passed to the pd.to_{fformat}() function

    Returns:
        Nothing
    """
    
    _validate_raw_data_location(raw_data_location)

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

    _validate_user_select_columns(select_columns, table_name)
    _validate_user_select_columns_includes_pk(select_columns, table_name)
    _validate_time_window(start_time, end_time)

    user_select_columns = select_columns

    logger.info(f"Caching data for table {table_name}")

    (
        start_time,
        end_time,
        select_columns,
        _,
        start_search,
    ) = _set_up_dynamic_compilers(table_name, start_time, end_time, select_columns)
    start_time = _parse_datetime_py(start_time, midnight='start')
    end_time = _parse_datetime_py(end_time, midnight='end')
    start_search = _parse_datetime_py(start_search, midnight='start')

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
        keep_zip=keep_zip,
        caching_mode=True,
        rebuild=rebuild,
        write_kwargs=kwargs,
        user_select_columns=user_select_columns,
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
    if table_name == "FCAS Providers":
        # Deprecated: AEMO emptied the `Ancillary Services` sheet of
        # `NEM Registration and Exemption List.xlsx` and moved the data
        # to a weekly archive on nemweb. The existing NEMOSIS handler
        # reads the (now-empty) sheet and chokes on the dedup step.
        # See issue #92.
        raise UserInputError(
            "The 'FCAS Providers' static table is no longer available "
            "via NEMOSIS. AEMO migrated this data out of "
            "'NEM Registration and Exemption List.xlsx' to a weekly "
            "archive at "
            "https://www.nemweb.com.au/REPORTS/CURRENT/ANCILLARY_SERVICES_REPORTS/ "
            "(file pattern: PUBLIC_ANCILLARY_SERVICES_YYYYMMDD.zip). "
            "See issue #92 for tracking. "
            "The 'Generators and Scheduled Loads' table still works."
        )

    _validate_raw_data_location(raw_data_location)

    if table_name not in _defaults.static_tables:
        raise UserInputError("Table name provided is not a static table.")

    _validate_user_select_columns(select_columns, table_name)
    _validate_user_select_columns_includes_pk(select_columns, table_name)
    _validate_filter_args(filter_cols, filter_values)

    # Remember whether the user explicitly asked for columns, so we can
    # enforce strict membership after the file is loaded. Defaults-
    # inherited columns are allowed to silently be missing from the
    # static file (e.g. AEMO drops a column from the registration
    # workbook); user-typed columns should not be — that's almost always
    # a typo, and silently returning a stub DataFrame is worse than
    # raising. Mirrors the dynamic_data_compiler contract.
    user_select_columns = select_columns

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
        except Exception:
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

    # Reject before filtering: catches the silent-stub bug where a typo
    # in user-typed select_columns (e.g. 'duid' instead of 'DUID') used
    # to ship a DataFrame missing the requested column with only a
    # WARNING. Mirrors `_check_loaded_select_columns` in
    # dynamic_data_compiler.
    _check_loaded_select_columns(table, user_select_columns, table_name)

    if filter_cols is not None:
        if not set(filter_cols).issubset(set(table.columns)):
            missing_columns = [col for col in filter_cols if col not in table.columns]
            # Was previously `UserInputError(...)` (no `raise`) — a
            # silent no-op that returned unfiltered data. Now properly
            # raised; this path is mostly unreachable thanks to the
            # post-load select_columns check above, but kept as a
            # defence in depth (e.g. filter_cols came from defaults
            # because user passed select_columns=None, and the default
            # filter column isn't in this AEMO file vintage).
            raise UserInputError(
                f"Filter columns {missing_columns} not in data for "
                f"table {table_name}. Available columns: "
                f"{sorted(table.columns)}."
            )
        else:
            table = _filter_on_column_value(table, filter_cols, filter_values)

    static_table_finalisers = static_data_finaliser_map[table_name]
    for finaliser in static_table_finalisers:
        table = finaliser(table, table_name)

    # Reset index so callers can do .loc[0] / .iloc[0] on the returned
    # DataFrame; without this, rows carry the original on-disk indices
    # after filtering.
    table = table.reset_index(drop=True)
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
    with _pd.ExcelFile(path_and_name) as xls:
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
    "Generators and Scheduled Loads": _downloader.download_xlsx,
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


def _iteration_overlaps_window(year, month, day, index, user_start, user_end):
    """Does this date_gen iteration's data period overlap the user's
    [user_start, user_end) window?

    Used to distinguish iterations the user actually asked for from
    book-keeping iterations the fetch loop makes:
      - the 1-day buffer-back month before `start_time`,
      - the ~120 historical months a `search_type='all'` table scans
        to compute "snapshot as of start_time".

    A user asking for 16 months of an effective-date table should not
    be warned that we didn't get data for 1997-11; they didn't ask for
    1997-11. A user asking for 16 months of a current/scraped table
    against AEMO's 6-week retention SHOULD be warned that ~450 days of
    their request had no data.

    Period extents (right-open, matching AEMO's end-of-interval
    convention):
      - (year, month, day=None, index=None): one calendar month
      - (year, month, day, index=None):       one calendar day
      - (year, month, day, index="HHMM"):     one 5-minute FCAS slot
    """
    year_i = int(year)
    month_i = int(month)
    if day is None:
        period_start = _datetime(year_i, month_i, 1)
        if month_i == 12:
            period_end = _datetime(year_i + 1, 1, 1)
        else:
            period_end = _datetime(year_i, month_i + 1, 1)
    elif index is None:
        day_i = int(day)
        period_start = _datetime(year_i, month_i, day_i)
        period_end = period_start + _timedelta(days=1)
    else:
        day_i = int(day)
        hour_i = int(index[:2])
        minute_i = int(index[2:])
        period_start = _datetime(year_i, month_i, day_i, hour_i, minute_i)
        period_end = period_start + _timedelta(minutes=5)
    return period_start < user_end and period_end > user_start


def _dynamic_data_fetch_loop(
    start_search,
    start_time,
    end_time,
    table_name,
    raw_data_location,
    select_columns,
    date_filter,
    fformat="parquet",
    keep_csv=False,
    keep_zip=True,
    caching_mode=False,
    rebuild=False,
    write_kwargs={},
    user_select_columns=None,
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
    # Uniform 1-day buffer-back on the file-fetch side, so any rows whose
    # timestamps lie just inside [start_time, end_time] but live in the prior
    # file are loaded. The post-load filter is strict on the query window, so
    # this affects fetches, not returns. FCAS uses per-interval files
    # (timestamp-precise filenames), so it doesn't need the look-back.
    date_gen_func = _processing_info_maps.date_gen[table_type]
    if date_gen_func is not _date_generators.year_month_day_index_gen:
        start_search = start_search - _timedelta(days=1)
    date_gen = date_gen_func(start_search, end_time)

    # Track per-period success across the user's requested window so we
    # can emit a single coverage-gap summary at the end. Without it, a
    # multi-day query against e.g. INTERMITTENT_GEN_SCADA whose range
    # mostly falls outside AEMO's Reports/Current/ retention window
    # silently returns just the in-window days — the only signal is N
    # separate per-file warnings that an aggregating user is unlikely
    # to read.
    #
    # We count only iterations whose period overlaps the user's
    # [start_time, end_time) window — so the buffer-back month (always
    # one month before start_time) and the historical scan months for
    # `search_type='all'` tables are excluded from the denominator.
    # Any gap that remains is a gap in the user's actual request.
    requested_periods = 0
    successful_requested_periods = 0

    for year, month, day, index in date_gen:
        in_user_window = _iteration_overlaps_window(
            year, month, day, index, start_time, end_time
        )
        if in_user_window:
            requested_periods += 1
        period_has_data = False
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
                    keep_zip=keep_zip,
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
                    data = _perform_column_selection(
                        data, select_columns, full_filename, user_select_columns
                    )

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

                data = _perform_column_selection(
                    data, select_columns, full_filename, user_select_columns
                )

                data_tables.append(data)
                period_has_data = True
            elif not caching_mode and chunk == 1:
                # Demoted from WARNING to DEBUG: when we reach here, the
                # cache file doesn't exist on disk — which means the
                # download upstream already failed and the downloader
                # already emitted its own warning (e.g. "PUBLIC_X not
                # downloaded (404 ...)"). Repeating "Loading data from
                # .../FILE.parquet failed" at WARNING level reads as
                # local cache corruption and confuses users who hit the
                # 404 path for historical / future / missing months.
                logger.debug(f"No cached data file present at {full_filename} (upstream download likely 404'd).")

            if data is None or '#' not in filename_stub:
                check_for_next_data_chunk = False
        if period_has_data and in_user_window:
            successful_requested_periods += 1

    # Warn iff the user's requested window has a gap, AND we did get at
    # least some data (zero-data is already covered by NoDataToReturn
    # in the caller — a duplicate WARNING here would be noise).
    if (
        not caching_mode
        and 0 < successful_requested_periods < requested_periods
    ):
        missing = requested_periods - successful_requested_periods
        coverage = successful_requested_periods / requested_periods
        logger.warning(
            f"Partial coverage for {table_name}: only "
            f"{successful_requested_periods}/{requested_periods} "
            f"periods in the requested window returned data "
            f"({coverage:.0%}). {missing} period(s) missing — see "
            f"per-period WARNINGs above for the specific files. "
            f"Common causes: requested range extends beyond AEMO's "
            f"Reports/Current/ retention window (typically ~6-7 weeks "
            f"for the current/scraped tables); requested month not "
            f"yet published in the MMSDM archive; requested range "
            f"pre-dates AEMO data."
        )

    return data_tables


def _perform_column_selection(data, select_columns, full_filename, user_select_columns=None):
    if select_columns != "all":
        keep_cols = _validate_select_columns(
            data, select_columns, full_filename, user_select_columns
        )
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


def _validate_select_columns(data, select_columns, full_filename, user_select_columns=None):
    """
    Checks whether select_columns are in the file. If at least one is,
    then it will return any of select_columns that are available as well as
    the date col (for date filtering).  If not, it will return an empty list.

    The warning about missing columns is only escalated to WARNING level
    when the missing column was explicitly typed by the user (i.e. is in
    user_select_columns). Missing columns that came from
    defaults.table_columns are an internal NEMOSIS-vs-AEMO-vintage mismatch
    (e.g. RAISE1SECRRP / LOWER1SECRRP didn't exist in DISPATCHPRICE before
    1-sec FCAS in late 2023) and would otherwise spam WARNING on every
    historical cache read — so those are demoted to DEBUG.

    Returns: List
    """
    file_cols = data.columns
    available_cols = file_cols[file_cols.isin(select_columns)].tolist()
    rejected_cols = set(select_columns) - set(available_cols)
    if not available_cols:
        return []
    else:
        if rejected_cols:
            user_set = set(user_select_columns or [])
            user_typed_missing = rejected_cols & user_set
            if user_typed_missing:
                logger.warning(
                    f"{rejected_cols} not in {full_filename}. "
                    + f"Loading {available_cols}"
                )
            else:
                # All rejected columns came from defaults — quiet noise.
                logger.debug(
                    f"{rejected_cols} not in {full_filename} "
                    "(columns from defaults, not in this vintage). "
                    f"Loading {available_cols}"
                )
        return available_cols


def _validate_user_select_columns(select_columns, table_name):
    """Up-front sanity check on user-supplied select_columns: shape only.

    The strict membership check (does every name actually exist?) happens
    after the data has been loaded — see _check_loaded_select_columns —
    because NEMOSIS legitimately allows users to ask for any column the
    AEMO file contains, not just those listed in defaults.table_columns.
    """
    if select_columns is None or select_columns == "all":
        return
    if not isinstance(select_columns, (list, tuple)):
        raise UserInputError(
            f"select_columns must be a list or the string 'all', "
            f"got {type(select_columns).__name__}."
        )


def _validate_user_select_columns_includes_pk(select_columns, table_name):
    """Ensure user-supplied select_columns contains every primary-key
    column for tables whose processing pipeline dedupes on PK.

    Without this, the dedup step inside finalise (`drop_duplicates_by_primary_key`
    for dynamic tables, or `_finalise_excel_data` for the Generators
    static table) raises a bare pandas `KeyError: Index([...], dtype='str')`
    that points at pandas internals rather than the user's actual
    mistake — they omitted a column NEMOSIS needs internally for
    correct dedup semantics.

    Defaults always include the PK columns (by construction of
    `defaults.table_columns`), so this check only fires when the user
    explicitly types a select_columns subset that's missing one. Pass
    `select_columns=None` to fall back to defaults.
    """
    if select_columns is None or select_columns == "all":
        return
    pk = _defaults.table_primary_keys.get(table_name)
    if not pk:
        return
    # Dynamic-table dedup runs `drop_duplicates_by_primary_key` from the
    # finalise pipeline.
    finalise = _processing_info_maps.finalise.get(table_name)
    is_dynamic_pk_dedup = bool(
        finalise and any(
            fn is _query_wrappers.drop_duplicates_by_primary_key for fn in finalise
        )
    )
    # Static-table dedup runs `_finalise_excel_data`, which does
    # `data.drop_duplicates(primary_keys)` if the table is in
    # `defaults.table_primary_keys`.
    static_finalisers = static_data_finaliser_map.get(table_name, [])
    is_static_pk_dedup = _finalise_excel_data in static_finalisers
    if not (is_dynamic_pk_dedup or is_static_pk_dedup):
        return
    missing = [c for c in pk if c not in select_columns]
    if missing:
        raise UserInputError(
            f"select_columns for {table_name} must include the table's "
            f"primary-key columns so NEMOSIS can dedupe correctly. "
            f"Missing: {missing}. Full primary key: {pk}. Either add the "
            f"missing column(s) to select_columns, or pass "
            f"select_columns=None to use the table defaults (which "
            f"already include the primary key)."
        )


def _check_loaded_select_columns(all_data, user_select_columns, table_name):
    """Post-load check: every column the user explicitly asked for must
    actually have made it into the returned DataFrame.

    Catches the silent-stub bug where select_columns=['SETTLEMENTDATE', 'rrp']
    (lowercase typo) used to return a 1-column DataFrame with just
    SETTLEMENTDATE and a WARNING. We only validate columns the user
    explicitly typed — defaults-inherited columns that happen to be missing
    from a particular vintage (e.g. RAISE1SECRRP pre-2023) keep the legacy
    warning behaviour.
    """
    if user_select_columns is None or user_select_columns == "all":
        return
    missing = [c for c in user_select_columns if c not in all_data.columns]
    if missing:
        raise UserInputError(
            f"select_columns contains {missing} which are not present in "
            f"the data for table {table_name}. Available columns: "
            f"{sorted(all_data.columns)}. Check for typos (column names "
            f"are case-sensitive); to use a column not in the NEMOSIS "
            f"default set, see the README section "
            f"'Accessing additional table columns'."
        )


def _validate_time_window(start_time, end_time):
    """Reject inverted (end < start) or zero-length (end == start) windows.

    Without this, an inverted window silently returns an empty DataFrame
    and a zero-length window downloads a whole month just to return
    (0, N) rows.

    We parse locally for the comparison and discard the result; the caller
    re-parses through its existing flow. Cheap and avoids changing the
    call-site shape.
    """
    if start_time is None or end_time is None:
        raise UserInputError(
            "start_time and end_time are required, got "
            f"start_time={start_time!r}, end_time={end_time!r}."
        )
    # parse_datetime_py raises ValueError for bad strings / tz-aware
    # datetimes. We let it propagate unchanged here so the existing error
    # contract (e.g. tz-aware → "Conversion between timezones not
    # implemented") is preserved.
    start_dt = _parse_datetime_py(start_time, midnight='start')
    end_dt = _parse_datetime_py(end_time, midnight='end')
    if end_dt < start_dt:
        raise UserInputError(
            f"end_time ({end_time}) is before start_time ({start_time}). "
            "Pass a window where end_time > start_time."
        )
    if end_dt == start_dt:
        raise UserInputError(
            f"start_time ({start_time}) and end_time ({end_time}) resolve "
            "to the same instant — zero-length window. Pass a window "
            "where end_time > start_time."
        )


def _validate_filter_args(filter_cols, filter_values):
    """Validate filter_cols / filter_values shape before downstream code zips
    them. Without this:
      - filter_cols supplied with filter_values=None raises a bare
        ``TypeError: 'NoneType' object is not iterable`` from inside zip().
      - filter_cols and filter_values of mismatched length silently drop the
        trailing filter_cols (zip truncates to the shorter), which can
        quietly produce a much wider result than the user expected — the
        README's "exclude intervention" pattern is one such case.
    """
    if filter_cols is None and filter_values is None:
        return
    if filter_cols is None:
        raise UserInputError(
            "filter_values was provided but filter_cols is None. "
            "Pass both, or neither."
        )
    if filter_values is None:
        raise UserInputError(
            "filter_cols was provided but filter_values is None. "
            "filter_values must be a tuple of lists, one list per "
            "filter_col (e.g. filter_cols=['REGIONID'], "
            "filter_values=(['SA1'],))."
        )
    if not isinstance(filter_cols, (list, tuple)):
        raise UserInputError(
            f"filter_cols must be a list, got {type(filter_cols).__name__}."
        )
    if not isinstance(filter_values, (list, tuple)):
        raise UserInputError(
            f"filter_values must be a tuple of lists, got "
            f"{type(filter_values).__name__}."
        )
    if len(filter_cols) != len(filter_values):
        raise UserInputError(
            f"filter_cols has {len(filter_cols)} entries but filter_values "
            f"has {len(filter_values)}. Each filter_col needs exactly one "
            f"corresponding list of values in filter_values "
            f"(e.g. filter_cols=['REGIONID', 'INTERVENTION'], "
            f"filter_values=(['SA1'], [0]))."
        )


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
    Writes the DataFrame to a non-CSV format if a non-CSV format is specified.
    """
    write_function = {"feather": data.to_feather, "parquet": data.to_parquet}
    # Remove files of the same name - deals with case of corrupted files.
    if _os.path.isfile(full_filename) and fformat != "csv":
        _os.unlink(full_filename)
    # Write to required format
    try:
        if fformat == "feather":
            write_function[fformat](full_filename, **write_kwargs)
        elif fformat == "parquet":
            write_function[fformat](full_filename, index=False, **write_kwargs)
        return
    except Exception:
        # tidy up incomplete file
        if _os.path.isfile(full_filename):
            _os.unlink(full_filename)
        raise


def _download_data(
    table_name, table_type, filename_stub, day, month, year, chunk, index, raw_data_location,
    keep_zip=True,
):
    """
    Dispatch table to downloader to be downloaded.

    Returns: nothing

    Logging is honest about whether we actually contacted AEMO: the
    `run*` functions return True if a network fetch occurred, False if
    a previously-downloaded zip on disk was reused, or None if the
    attempt failed (in which case a warning has already been emitted
    downstream). Both branches also extract the zip; the verb on a real
    fetch is "Downloading and extracting" to make that explicit, so the
    "Extracting cached zip" message reads as a proper subset (skipped
    the network) rather than something different. Users aren't misled
    into thinking every call hammers AEMO when in fact only the extract
    step is being repeated (see #TBD discussion in user-testing
    exploration).
    """
    fetched = _processing_info_maps.downloader[table_type](
        year, month, day, chunk, index, filename_stub, raw_data_location,
        keep_zip=keep_zip,
    )

    if chunk == 1 and fetched is not None:
        verb = "Downloading and extracting data" if fetched else "Extracting cached zip"
        if day is None:
            logger.info(
                f"{verb} for table {table_name}, "
                + f"year {year}, month {month}"
            )
        elif index is None:
            logger.info(
                f"{verb} for table {table_name}, "
                + f"year {year}, month {month}, day {day}"
            )
        else:
            logger.info(
                f"{verb} for table {table_name}, "
                + f"year {year}, month {month}, day {day},"
                + f"time {index}."
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

