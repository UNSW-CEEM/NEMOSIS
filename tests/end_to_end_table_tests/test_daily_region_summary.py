"""Tests for DAILY_REGION_SUMMARY.

Scrape-pattern table: NEMOSIS fetches AEMO's rolling current-data index at
/Reports/Current/Daily_Reports/, finds the filename matching a date stub,
downloads the zip, and extracts the DAILY_REGION_SUMMARY section from the
multi-table PUBLIC_DAILY file.

The fixture freezes one recent day (see ERAS["recent"] in spec.py) plus
the prior day (NEMOSIS's 1-day buffer). Unrelated sections of PUBLIC_DAILY
have been stripped to keep the fixture small.
"""
from nemosis import dynamic_data_compiler


import pandas as pd


def test_recent_day_returns_rows_for_filtered_regions(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2026/05/15 00:00:00",
        end_time="2026/05/15 01:00:00",
        table_name="DAILY_REGION_SUMMARY",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"],
    )

    assert not data.empty
    assert set(data["REGIONID"]) == {"SA1", "NSW1"}


# ---------------------------------------------------------------------------
# Market-day boundary at 04:00. Daily files cover `{date} 04:05 → {date+1}
# 04:00` — so the 04:00 row lives in the *previous* daily file. NEMOSIS's
# `current_gen` always subtracts 1 day (unconditional buffer-back), so a
# query ending at 04:00 must still pull the prior day's file to retrieve
# that final row.
# ---------------------------------------------------------------------------

def test_end_market_day_returns_0400_row_from_previous_file(nemosis_fixture):
    """[03:55, 04:00]: filter is start-exclusive end-inclusive, so only
    the 04:00 row qualifies — and it lives in the `20260514` daily file,
    not `20260515`. Non-empty proves buffer-back fired correctly."""
    data = dynamic_data_compiler(
        start_time="2026/05/15 03:55:00",
        end_time="2026/05/15 04:00:00",
        table_name="DAILY_REGION_SUMMARY",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"],
    )
    assert set(data["SETTLEMENTDATE"].unique()) == {pd.Timestamp("2026-05-15 04:00:00")}
    assert set(data["REGIONID"]) == {"SA1", "NSW1"}
    assert not data.duplicated(["SETTLEMENTDATE", "REGIONID"]).any()


def test_start_market_day_returns_0405_row_from_current_file(nemosis_fixture):
    """[04:00, 04:05]: start-exclusive means 04:00 is dropped; 04:05 is
    the first row of the `20260515` daily file. Tests the stitch from
    the consumer side — the current day's file must be fetched."""
    data = dynamic_data_compiler(
        start_time="2026/05/15 04:00:00",
        end_time="2026/05/15 04:05:00",
        table_name="DAILY_REGION_SUMMARY",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"],
    )
    assert set(data["SETTLEMENTDATE"].unique()) == {pd.Timestamp("2026-05-15 04:05:00")}
    assert set(data["REGIONID"]) == {"SA1", "NSW1"}
    assert not data.duplicated(["SETTLEMENTDATE", "REGIONID"]).any()
