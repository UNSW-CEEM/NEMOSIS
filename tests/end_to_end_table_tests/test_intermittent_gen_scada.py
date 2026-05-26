"""Tests for INTERMITTENT_GEN_SCADA.

Scrape-pattern table publishing SCADA output for semi-scheduled
generators in the next-day window. Keyed on RUN_DATETIME + DUID.

Fixtured era: recent (scrape endpoint retention is short).
"""
from nemosis import dynamic_data_compiler


import pandas as pd


def test_recent_day_returns_rows_for_fixtured_duids(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2026/05/15 00:00:00",
        end_time="2026/05/15 01:00:00",
        table_name="INTERMITTENT_GEN_SCADA",
        raw_data_location=str(nemosis_fixture),
        select_columns=["RUN_DATETIME", "DUID", "SCADA_VALUE"],
    )

    assert not data.empty
    assert set(data["DUID"]) <= {"AGLHAL", "HDWF2"}


# Market-day stitch at 04:00 — same convention as the other scrape tables.
# AGLHAL is a peaking plant (scheduled, not intermittent) so only HDWF2
# appears in this table's fixture. SCADA_TYPE is part of the PK — each
# (RUN_DATETIME, DUID) has two rows: ELAV (Element Availability) and
# LOCL (Local SCADA value).

def test_end_market_day_returns_0400_row_from_previous_file(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2026/05/15 03:55:00",
        end_time="2026/05/15 04:00:00",
        table_name="INTERMITTENT_GEN_SCADA",
        raw_data_location=str(nemosis_fixture),
        select_columns=["RUN_DATETIME", "DUID", "SCADA_TYPE", "SCADA_VALUE"],
    )
    assert set(data["RUN_DATETIME"].unique()) == {pd.Timestamp("2026-05-15 04:00:00")}
    assert set(data["DUID"]) == {"HDWF2"}
    assert set(data["SCADA_TYPE"]) == {"ELAV", "LOCL"}
    assert not data.duplicated(["RUN_DATETIME", "DUID", "SCADA_TYPE"]).any()


def test_start_market_day_returns_0405_row_from_current_file(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2026/05/15 04:00:00",
        end_time="2026/05/15 04:05:00",
        table_name="INTERMITTENT_GEN_SCADA",
        raw_data_location=str(nemosis_fixture),
        select_columns=["RUN_DATETIME", "DUID", "SCADA_TYPE", "SCADA_VALUE"],
    )
    assert set(data["RUN_DATETIME"].unique()) == {pd.Timestamp("2026-05-15 04:05:00")}
    assert set(data["DUID"]) == {"HDWF2"}
    assert set(data["SCADA_TYPE"]) == {"ELAV", "LOCL"}
    assert not data.duplicated(["RUN_DATETIME", "DUID", "SCADA_TYPE"]).any()
