"""Tests for DISPATCHREGIONSUM.

Per-region five-minute summary: total demand, cleared generation, FCAS
enablements, etc. Has INTERVENTION column like DISPATCHPRICE, so tests
filter INTERVENTION == 0 to pin the normal-run timeline.

Boundary tests (`test_dispatch_region_sum_boundary`) are auto-generated
from `spec.DYNAMIC_TABLES["DISPATCHREGIONSUM"]["eras"]` by
`_boundaries.py`.

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
    "2024/08/01 00:00:00",
])
def test_one_hour_gives_twelve_intervals_per_region(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)
    expected = [start + pd.Timedelta(minutes=5 * i) for i in range(1, 13)]

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="DISPATCHREGIONSUM",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "INTERVENTION", "TOTALDEMAND"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert set(data["REGIONID"]) == FIXTURED_REGIONS
    for region in FIXTURED_REGIONS:
        timestamps = sorted(data[data["REGIONID"] == region]["SETTLEMENTDATE"].tolist())
        assert timestamps == expected


@pytest.mark.parametrize(
    "case", boundary_cases("DISPATCHREGIONSUM"), ids=lambda c: c.id
)
def test_dispatch_region_sum_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="DISPATCHREGIONSUM",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "INTERVENTION", "TOTALDEMAND"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="REGIONID",
        expected_entities=FIXTURED_REGIONS,
    )
