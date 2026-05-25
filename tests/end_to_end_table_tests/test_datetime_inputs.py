"""End-to-end tests for accepting datetime / date inputs to the public
compilers (#44, #53).

For legacy reasons NEMOSIS has always taken `start_time` / `end_time`
as strings of `"YYYY/MM/DD HH:MM:SS"`. As of this change, callers may
also pass:

- a timezone-naive `datetime.datetime` — used verbatim (treated as
  market time)
- a `datetime.date` — converted to midnight; `end_time` resolves to
  midnight at the *start of the next day* so a request bounded by a
  date covers the whole of that date

These tests verify the equivalence at the public API level: the same
window expressed three different ways must produce the same data.
"""
from datetime import date, datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from nemosis import cache_compiler, dynamic_data_compiler


# 30-minute window in May 2018 — long enough to fetch real dispatch
# price rows from the fixture, short enough to keep the tests fast.
START_STR = "2018/05/01 00:00:00"
END_STR = "2018/05/01 00:30:00"
START_DT = datetime(2018, 5, 1, 0, 0, 0)
END_DT = datetime(2018, 5, 1, 0, 30, 0)


def _sort_for_compare(df):
    """Order rows deterministically so frame equality is robust to
    upstream sort instability."""
    return df.sort_values(list(df.columns)).reset_index(drop=True)


def test_dynamic_data_compiler_accepts_datetime(nemosis_fixture):
    """A `datetime` and a matching string MUST return identical data."""
    via_str = dynamic_data_compiler(
        start_time=START_STR, end_time=END_STR,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
    )
    via_dt = dynamic_data_compiler(
        start_time=START_DT, end_time=END_DT,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
    )
    assert_frame_equal(_sort_for_compare(via_str), _sort_for_compare(via_dt))


def test_dynamic_data_compiler_accepts_date(nemosis_fixture):
    """A `date` end_time must resolve to midnight at the *start of the
    next day* so the day is fully covered. Whole-day-of-May-1 expressed
    via two dates must equal the same range expressed via strings."""
    via_str = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00", end_time="2018/05/02 00:00:00",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
    )
    via_date = dynamic_data_compiler(
        start_time=date(2018, 5, 1), end_time=date(2018, 5, 1),
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
    )
    assert_frame_equal(_sort_for_compare(via_str), _sort_for_compare(via_date))


def test_cache_compiler_accepts_datetime(nemosis_fixture):
    """cache_compiler must accept the same input shapes as
    dynamic_data_compiler — just confirms no exception and that a
    parquet lands in the cache."""
    cache_compiler(
        start_time=START_DT, end_time=END_DT,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
    )
    assert list(nemosis_fixture.glob("*DISPATCHPRICE*.parquet")), (
        "cache should be populated when datetime inputs are supplied"
    )


def test_cache_compiler_accepts_date(nemosis_fixture):
    """date inputs flow through cache_compiler the same way."""
    cache_compiler(
        start_time=date(2018, 5, 1), end_time=date(2018, 5, 1),
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
    )
    assert list(nemosis_fixture.glob("*DISPATCHPRICE*.parquet"))


# ---------------------------------------------------------------------------
# Tables with a `setup_function` in processing_info_maps massage start/end
# before the main fetch loop. Those wrappers (dispatch_date_setup,
# dispatch_half_hour_setup) used to call `datetime.strptime` directly,
# which broke when the public API started accepting datetime/date inputs.
# This regression test covers the BIDDAYOFFER_D path (dispatch_date_setup).
# ---------------------------------------------------------------------------

def test_dispatch_date_setup_path_accepts_datetime(nemosis_fixture):
    """BIDDAYOFFER_D routes through dispatch_date_setup before the main
    fetch loop — verify datetime inputs survive that pre-processing."""
    via_str = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00", end_time="2018/05/01 00:30:00",
        table_name="BIDDAYOFFER_D",
        raw_data_location=str(nemosis_fixture),
    )
    via_dt = dynamic_data_compiler(
        start_time=datetime(2018, 5, 1), end_time=datetime(2018, 5, 1, 0, 30),
        table_name="BIDDAYOFFER_D",
        raw_data_location=str(nemosis_fixture),
    )
    assert_frame_equal(_sort_for_compare(via_str), _sort_for_compare(via_dt))
