"""Tests for TRADINGPRICE.

Per-region trading price summary. Interval granularity changed on 2021-10-01
(the 5MS reform cutover): before that date trading was 30-minute, after it
matches dispatch at 5-min. The tests make that transition explicit via era
parametrisation and a dedicated cutover test.

Boundary tests (`test_trading_price_boundary`) are auto-generated from
`spec.DYNAMIC_TABLES["TRADINGPRICE"]["eras"]` by `_boundaries.py`. The
helper picks the right stride per era automatically (30 → 5 at 2021-10).

REGIONID ∈ {SA1, NSW1}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_REGIONS = {"SA1", "NSW1"}


@pytest.mark.parametrize("era_start,interval_minutes,expected_per_hour", [
    ("2018/05/01 00:00:00", 30, 2),    # pre-5-min era — 2 half-hour intervals/hour
    ("2021/05/01 00:00:00", 30, 2),
    ("2022/06/01 00:00:00", 5, 12),    # post-5MS (cutover was 2021-10-01)
    ("2024/08/01 00:00:00", 5, 12),
])
def test_interval_granularity_matches_era(
    nemosis_fixture, era_start, interval_minutes, expected_per_hour,
):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="TRADINGPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP"],
    )

    assert set(data["REGIONID"]) == FIXTURED_REGIONS
    for region in FIXTURED_REGIONS:
        region_data = data[data["REGIONID"] == region].sort_values("SETTLEMENTDATE")
        assert len(region_data) == expected_per_hour


def test_5ms_cutover_stride_transition(nemosis_fixture):
    """Query a window straddling the 2021-10-01 5MS cutover and assert the
    stride transition is reproduced exactly: two 30-min rows from the Sept
    file, then twelve 5-min rows from the Oct file."""
    data = dynamic_data_compiler(
        start_time="2021/09/30 23:00:00",
        end_time="2021/10/01 01:00:00",
        table_name="TRADINGPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP"],
    )

    expected = [
        pd.Timestamp("2021-09-30 23:30:00"),
        pd.Timestamp("2021-10-01 00:00:00"),
    ] + [pd.Timestamp("2021-10-01 00:00:00") + pd.Timedelta(minutes=5 * i)
         for i in range(1, 13)]

    assert set(data["REGIONID"]) == FIXTURED_REGIONS
    for region in FIXTURED_REGIONS:
        timestamps = sorted(data[data["REGIONID"] == region]["SETTLEMENTDATE"].tolist())
        assert timestamps == expected


@pytest.mark.parametrize(
    "case", boundary_cases("TRADINGPRICE"), ids=lambda c: c.id
)
def test_trading_price_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="TRADINGPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP"],
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="REGIONID",
        expected_entities=FIXTURED_REGIONS,
    )
