"""Tests for the DISPATCHLOAD dispatch table.

DISPATCHLOAD publishes one row per dispatchable unit (DUID) per five-minute
interval, giving per-unit dispatch targets, ramp rates, availability, etc.
Like DISPATCHPRICE it has both a "normal" and an "intervention" run per
interval; the tests filter INTERVENTION == 0 to pin the normal run.

Boundary tests (`test_dispatch_load_boundary`) are auto-generated from
`spec.DYNAMIC_TABLES["DISPATCHLOAD"]["eras"]` by `_boundaries.py`.

The fixture pre-filters rows to DUID ∈ {AGLHAL, HDWF2}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_DUIDS = {"AGLHAL", "HDWF2"}


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/05/01 00:00:00",
    "2024/08/01 00:00:00",
])
def test_one_hour_gives_twelve_intervals_per_duid(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)
    expected = [start + pd.Timedelta(minutes=5 * i) for i in range(1, 13)]

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="DISPATCHLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INTERVENTION", "INITIALMW"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert set(data["DUID"]) == FIXTURED_DUIDS
    for duid in FIXTURED_DUIDS:
        timestamps = sorted(data[data["DUID"] == duid]["SETTLEMENTDATE"].tolist())
        assert timestamps == expected


def test_duid_filter_narrows_to_one_unit(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00",
        end_time="2018/05/01 01:00:00",
        table_name="DISPATCHLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW"],
        filter_cols=["DUID"],
        filter_values=(["AGLHAL"],),
    )

    assert set(data["DUID"]) == {"AGLHAL"}


@pytest.mark.parametrize(
    "case", boundary_cases("DISPATCHLOAD"), ids=lambda c: c.id
)
def test_dispatch_load_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="DISPATCHLOAD",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "INTERVENTION", "INITIALMW"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="DUID",
        expected_entities=FIXTURED_DUIDS,
    )
