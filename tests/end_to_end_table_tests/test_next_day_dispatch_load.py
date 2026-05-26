"""Tests for NEXT_DAY_DISPATCHLOAD.

Scrape-pattern version of DISPATCHLOAD published as part of the
next-day dispatch reports. Five-minute resolution, DUID-keyed.

Fixtured era: recent (scrape endpoint keeps only a few months of data).
"""
from nemosis import dynamic_data_compiler


import pandas as pd


def test_recent_day_returns_rows_for_fixtured_duids(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2026/05/15 00:00:00",
        end_time="2026/05/15 01:00:00",
        table_name="NEXT_DAY_DISPATCHLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW"],
    )

    assert not data.empty
    assert set(data["DUID"]) <= {"AGLHAL", "HDWF2"}


# Market-day stitch at 04:00. See test_daily_region_summary.py for the
# full explanation — NEXT_DAY_DISPATCH files share the same convention:
# `{date} 04:05 → {date+1} 04:00` per file, with 04:00 in the prior file.

def test_end_market_day_returns_0400_row_from_previous_file(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2026/05/15 03:55:00",
        end_time="2026/05/15 04:00:00",
        table_name="NEXT_DAY_DISPATCHLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW"],
    )
    assert set(data["SETTLEMENTDATE"].unique()) == {pd.Timestamp("2026-05-15 04:00:00")}
    assert set(data["DUID"]) <= {"AGLHAL", "HDWF2"}
    assert not data.duplicated(["SETTLEMENTDATE", "DUID"]).any()


def test_start_market_day_returns_0405_row_from_current_file(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2026/05/15 04:00:00",
        end_time="2026/05/15 04:05:00",
        table_name="NEXT_DAY_DISPATCHLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW"],
    )
    assert set(data["SETTLEMENTDATE"].unique()) == {pd.Timestamp("2026-05-15 04:05:00")}
    assert set(data["DUID"]) <= {"AGLHAL", "HDWF2"}
    assert not data.duplicated(["SETTLEMENTDATE", "DUID"]).any()
