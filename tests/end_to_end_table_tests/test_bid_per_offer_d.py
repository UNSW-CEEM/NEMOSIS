"""Tests for BIDPEROFFER_D.

Per-unit per-five-minute bid offers (up to 10 price bands). The MMS
monthly archive is the source at every era NEMOSIS supports — AEMO
stopped publishing these files between March 2021 and July 2024, so
coverage jumps over that gap.

Boundary tests are NOT generated for this table — bids are partitioned
by AEMO trading day (04:05 → 04:00 next day), not calendar day, so
calendar-aligned queries between 00:00 and 04:00 fall in the previous
trading-day archive and our boundary helper's row-count assertions
break. See `_boundaries.py::_HELPER_DOES_NOT_COVER` for the full list
of skipped tables.

DUID ∈ {AGLHAL, HDWF2}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

FIXTURED_DUIDS = {"AGLHAL", "HDWF2"}


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/02/01 00:00:00",
    "2024/09/01 00:00:00",
])
def test_trading_day_buffer_back_at_start_of_month(nemosis_fixture, era_start):
    """A calendar-midnight query on day 1 must reach into the prev-month
    archive for the prior trading day's 00:05 → 04:00 rows and stitch them
    to day 1's 04:05 → onward rows. The [day1 00:00, day1 05:15] window is
    the canonical shape that exercises this — the buffer-back only fires
    when start_time is exactly first-of-month midnight (see issue #70)."""
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=5, minutes=15)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="BIDPEROFFER_D",
        raw_data_location=str(nemosis_fixture),
        select_columns=["INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL"],
        filter_cols=["DUID", "BIDTYPE"],
        filter_values=(["AGLHAL"], ["ENERGY"]),
    )

    timestamps = sorted(data["INTERVAL_DATETIME"].tolist())
    assert len(timestamps) == 63                                      # 5h15min / 5min
    assert timestamps[0] == start + pd.Timedelta(minutes=5)           # 00:05
    assert timestamps[-1] == start + pd.Timedelta(hours=5, minutes=15)  # 05:15
    diffs = {b - a for a, b in zip(timestamps, timestamps[1:])}
    assert diffs == {pd.Timedelta(minutes=5)}


def test_bids_returned_for_fixtured_duids(nemosis_fixture):
    """Smoke: both DUIDs present in a simple 1h window."""
    data = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00",
        end_time="2018/05/01 01:00:00",
        table_name="BIDPEROFFER_D",
        raw_data_location=str(nemosis_fixture),
        select_columns=["INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL"],
    )

    assert not data.empty
    assert set(data["DUID"]) <= FIXTURED_DUIDS
