"""Tests for ROOFTOP_PV_ACTUAL.

Per-region rooftop PV generation at 30-min resolution. Introduced
around 2019. Has a TYPE column with both SATELLITE (regional estimate
updated through the day) and DAILY (next-day reconciled) values —
a given (REGIONID, INTERVAL_DATETIME) can have up to two rows.

Boundary matrix (`test_rooftop_pv_actual_boundary`) filters TYPE=SATELLITE
so the helper sees one row per (REGIONID, interval) and its row-count
assertions hold. The matrix is auto-generated from
`spec.DYNAMIC_TABLES["ROOFTOP_PV_ACTUAL"]["eras"]` by `_boundaries.py`
— 30-min stride throughout.

REGIONID ∈ {SA1, NSW1}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_REGIONS = {"SA1", "NSW1"}


@pytest.mark.parametrize("era_start", [
    "2021/05/01 00:00:00",
    "2024/08/01 00:00:00",
])
def test_one_hour_returns_two_half_hourly_rows_per_region_per_type(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="ROOFTOP_PV_ACTUAL",
        raw_data_location=str(nemosis_fixture),
        select_columns=["INTERVAL_DATETIME", "REGIONID", "TYPE", "POWER"],
    )

    assert set(data["REGIONID"]) == FIXTURED_REGIONS
    # Two half-hour intervals in one hour. Each region may appear under
    # either SATELLITE or DAILY type (or both).
    for region in FIXTURED_REGIONS:
        by_type = data[data["REGIONID"] == region].groupby("TYPE").size()
        assert (by_type == 2).all(), by_type


@pytest.mark.parametrize(
    "case", boundary_cases("ROOFTOP_PV_ACTUAL"), ids=lambda c: c.id
)
def test_rooftop_pv_actual_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="ROOFTOP_PV_ACTUAL",
        raw_data_location=str(nemosis_fixture),
        select_columns=["INTERVAL_DATETIME", "REGIONID", "TYPE", "POWER"],
        filter_cols=["TYPE"],
        filter_values=(["SATELLITE"],),
    )

    assert_boundary_shape(
        data, case,
        date_col="INTERVAL_DATETIME",
        entities_col="REGIONID",
        expected_entities=FIXTURED_REGIONS,
    )
