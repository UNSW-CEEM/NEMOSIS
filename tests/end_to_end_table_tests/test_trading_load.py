"""Tests for TRADINGLOAD.

Per-unit 30-minute trading summary (INITIALMW, TOTALCLEARED, etc.).
**Discontinued before 2022-01** — AEMO stopped publishing the monthly
archive for this table when trading merged with dispatch. Only
pre-2022 eras are fixtured.

Boundary tests (`test_trading_load_boundary`) are auto-generated from
`spec.DYNAMIC_TABLES["TRADINGLOAD"]["eras"]` by `_boundaries.py` —
30-min stride throughout.

DUID ∈ {AGLHAL, HDWF2}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_DUIDS = {"AGLHAL", "HDWF2"}


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/05/01 00:00:00",
])
def test_one_hour_gives_two_half_hourly_intervals_per_duid(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="TRADINGLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW"],
    )

    assert set(data["DUID"]) == FIXTURED_DUIDS
    for duid in FIXTURED_DUIDS:
        assert len(data[data["DUID"] == duid]) == 2


@pytest.mark.parametrize(
    "case", boundary_cases("TRADINGLOAD"), ids=lambda c: c.id
)
def test_trading_load_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="TRADINGLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW"],
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="DUID",
        expected_entities=FIXTURED_DUIDS,
    )
