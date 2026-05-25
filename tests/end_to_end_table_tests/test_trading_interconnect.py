"""Tests for TRADINGINTERCONNECT.

Per-interconnector trading summary. Same 30-min → 5-min transition
at 2021-10-01 (5MS reform) as TRADINGPRICE.

Boundary tests (`test_trading_interconnect_boundary`) are auto-generated
from `spec.DYNAMIC_TABLES["TRADINGINTERCONNECT"]["eras"]` by
`_boundaries.py`. The helper picks the right stride per era automatically.

INTERCONNECTORID ∈ {VIC1-NSW1}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_INTERCONNECTORS = {"VIC1-NSW1"}


@pytest.mark.parametrize("era_start,expected_per_hour", [
    ("2018/05/01 00:00:00", 2),   # 30-min era
    ("2021/05/01 00:00:00", 2),
    ("2022/06/01 00:00:00", 12),  # post-5MS (cutover was 2021-10-01)
    ("2024/08/01 00:00:00", 12),
])
def test_interval_granularity_matches_era(nemosis_fixture, era_start, expected_per_hour):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="TRADINGINTERCONNECT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "INTERCONNECTORID", "METEREDMWFLOW"],
    )

    assert set(data["INTERCONNECTORID"]) == FIXTURED_INTERCONNECTORS
    assert len(data) == expected_per_hour


def test_5ms_cutover_stride_transition(nemosis_fixture):
    """Query a window straddling the 2021-10-01 5MS cutover and assert the
    stride transition: two 30-min rows from the Sept file, then twelve 5-min
    rows from the Oct file."""
    data = dynamic_data_compiler(
        start_time="2021/09/30 23:00:00",
        end_time="2021/10/01 01:00:00",
        table_name="TRADINGINTERCONNECT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "INTERCONNECTORID", "METEREDMWFLOW"],
    )

    expected = [
        pd.Timestamp("2021-09-30 23:30:00"),
        pd.Timestamp("2021-10-01 00:00:00"),
    ] + [pd.Timestamp("2021-10-01 00:00:00") + pd.Timedelta(minutes=5 * i)
         for i in range(1, 13)]

    assert set(data["INTERCONNECTORID"]) == FIXTURED_INTERCONNECTORS
    for link in FIXTURED_INTERCONNECTORS:
        timestamps = sorted(data[data["INTERCONNECTORID"] == link]["SETTLEMENTDATE"].tolist())
        assert timestamps == expected


@pytest.mark.parametrize(
    "case", boundary_cases("TRADINGINTERCONNECT"), ids=lambda c: c.id
)
def test_trading_interconnect_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="TRADINGINTERCONNECT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "INTERCONNECTORID", "METEREDMWFLOW"],
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="INTERCONNECTORID",
        expected_entities=FIXTURED_INTERCONNECTORS,
    )
