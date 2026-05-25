"""Tests for DISPATCHINTERCONNECTORRES.

Per-interconnector five-minute flow data (MW, losses, constraints).
Has INTERVENTION column like other dispatch tables.

Boundary tests (`test_dispatch_interconnectorres_boundary`) are
auto-generated from `spec.DYNAMIC_TABLES["DISPATCHINTERCONNECTORRES"]
["eras"]` by `_boundaries.py`.

INTERCONNECTORID ∈ {VIC1-NSW1}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_INTERCONNECTORS = {"VIC1-NSW1"}


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/05/01 00:00:00",
    "2024/08/01 00:00:00",
])
def test_one_hour_gives_twelve_intervals_per_interconnector(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)
    expected = [start + pd.Timedelta(minutes=5 * i) for i in range(1, 13)]

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="DISPATCHINTERCONNECTORRES",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "INTERCONNECTORID", "INTERVENTION", "METEREDMWFLOW"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert set(data["INTERCONNECTORID"]) == FIXTURED_INTERCONNECTORS
    timestamps = sorted(data["SETTLEMENTDATE"].tolist())
    assert timestamps == expected


@pytest.mark.parametrize(
    "case", boundary_cases("DISPATCHINTERCONNECTORRES"), ids=lambda c: c.id
)
def test_dispatch_interconnectorres_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="DISPATCHINTERCONNECTORRES",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "INTERCONNECTORID", "INTERVENTION", "METEREDMWFLOW"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="INTERCONNECTORID",
        expected_entities=FIXTURED_INTERCONNECTORS,
    )
