"""Tests for BIDDAYOFFER_D.

Per-unit daily bid offers (price bands, daily energy constraint,
rebid explanations). MMS monthly archive at every era — the archive
just has a hole between March 2021 and July 2024 that we skip over.

Fixtured eras: 2018-05, 2021-02, 2024-09; DUID ∈ {AGLHAL, HDWF2}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler


# Verified against raw AEMO MMSDM archives for each fixtured trading day —
# HDWF2 bids in exactly these 7 services and no others. See
# scripts/verify_hdwf2_bidtypes.py to re-check.
EXPECTED_BIDTYPES = {
    "ENERGY", "RAISEREG", "LOWERREG",
    "RAISE5MIN", "RAISE60SEC", "LOWER5MIN", "LOWER60SEC",
}


@pytest.mark.parametrize("era_start,prev_day", [
    ("2018/05/01 00:00:00", "2018-04-30"),
    ("2021/02/01 00:00:00", "2021-01-31"),
    ("2024/09/01 00:00:00", "2024-08-31"),
])
def test_day_offers_at_calendar_boundary(nemosis_fixture, era_start, prev_day):
    """At calendar midnight of day 1, NEMOSIS returns the previous trading
    day's bids (trading-day convention — day 1's bid doesn't start until
    04:05). AGLHAL is energy-only; HDWF2 bids every FCAS service as well."""
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="BIDDAYOFFER_D",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "BIDTYPE"],
    )

    assert len(data) == 8
    assert set(data["DUID"]) == {"AGLHAL", "HDWF2"}
    assert set(data["SETTLEMENTDATE"]) == {pd.Timestamp(prev_day)}
    assert set(data[data["DUID"] == "AGLHAL"]["BIDTYPE"]) == {"ENERGY"}
    assert set(data[data["DUID"] == "HDWF2"]["BIDTYPE"]) == EXPECTED_BIDTYPES
