"""Tests for TRADINGREGIONSUM.

Per-region 30-minute trading summary. Discontinued in 2022 (same
reason as TRADINGLOAD), so only pre-2022 eras are fixtured.

Boundary tests (`test_trading_region_sum_boundary`) are auto-generated
from `spec.DYNAMIC_TABLES["TRADINGREGIONSUM"]["eras"]` by `_boundaries.py`
— 30-min stride throughout.

REGIONID ∈ {SA1, NSW1}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_REGIONS = {"SA1", "NSW1"}


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/05/01 00:00:00",
])
def test_one_hour_gives_two_half_hourly_intervals_per_region(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="TRADINGREGIONSUM",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"],
    )

    assert set(data["REGIONID"]) == FIXTURED_REGIONS
    for region in FIXTURED_REGIONS:
        assert len(data[data["REGIONID"] == region]) == 2


@pytest.mark.parametrize(
    "case", boundary_cases("TRADINGREGIONSUM"), ids=lambda c: c.id
)
def test_trading_region_sum_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="TRADINGREGIONSUM",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "TOTALDEMAND"],
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="REGIONID",
        expected_entities=FIXTURED_REGIONS,
    )
