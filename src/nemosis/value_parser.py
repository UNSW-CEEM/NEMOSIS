import pandas as pd

from nemosis import defaults as _defaults


def _parse_datetime(series):
    """
    Attempts to parse a column into a datetime
    If unable to (because the data is not a datetime), will raise a ValueError
    Args:
        series: a numpy array (pandas column)

    Returns:
        series (np.Array)
    """

    try:
        # this first format is the most common
        return pd.to_datetime(series, format=_defaults.date_formats[0])
    except ValueError as e:
        try:
            # this format with milliseconds is used in some bidding columns
            return pd.to_datetime(series, format=_defaults.date_formats[1])
        except ValueError as e:
            # this format is used in some 4-second FCAS data
            return pd.to_datetime(series, format=_defaults.date_formats[2])


def _parse_column(series):
    """
    Attempts to parse a column into a datetime or numeric.
    If unable to, returns the original column
    Args:
        series: a numpy array (pandas column)

    Returns:
        series (np.Array)
    """

    try:
        return _parse_datetime(series)
    except ValueError:
        try:
            col_new = pd.to_numeric(series)
            return col_new
        except ValueError as e:
            return series


def _infer_column_data_types(data):
    """
    Infer datatype of DataFrame assuming inference need only be carried out
    for any columns with dtype "object". Adapted from StackOverflow.

    If the column is an object type, attempt conversions to (in order of):
    1. datetime
    2. numeric

    Returns: Data with inferred types.
    """

    for col in data:
        data[col] = _parse_column(data[col])

    return data
