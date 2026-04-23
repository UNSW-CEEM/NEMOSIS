"""Tests for the DISPATCHPRICE dispatch table.

DISPATCHPRICE publishes one row per region per five-minute dispatch interval.
Settlement timestamps are period-ending, so a request for 00:00–01:00 returns
the twelve intervals labelled 00:05 through 01:00.

The fixture pre-filters rows to REGIONID ∈ {SA1, NSW1} and covers six AEMO
eras:
    2018-05 — pre-5-min-trading baseline
    2020-01 — year boundary, pre-5-min format
    2021-05 — first month of the daily BIDMOVE_COMPLETE bidding layout
    2022-01 — year boundary, 5-min dispatch
    2024-08 — PUBLIC_DVD → PUBLIC_ARCHIVE# filename cutover
    2025-01 — year boundary, PUBLIC_ARCHIVE# format

The boundary matrix (`test_dispatch_price_boundary`) is auto-generated from
`spec.DYNAMIC_TABLES["DISPATCHPRICE"]["eras"]` by
`tests/end_to_end_table_tests/_boundaries.py` — see that module for flavour
and time-of-day semantics.
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
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP", "INTERVENTION"],
        filter_cols=["INTERVENTION"],       # exclude intervention dispatch runs,
        filter_values=([0],),                # which happen sporadically
    )

    assert set(data["REGIONID"]) == FIXTURED_REGIONS
    for region in FIXTURED_REGIONS:
        timestamps = sorted(data[data["REGIONID"] == region]["SETTLEMENTDATE"].tolist())
        assert timestamps == expected


def test_region_filter_narrows_to_one_region(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00",
        end_time="2018/05/01 01:00:00",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP"],
        filter_cols=["REGIONID"],
        filter_values=(["SA1"],),
    )

    assert set(data["REGIONID"]) == {"SA1"}


def test_select_columns_returns_only_those_columns(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00",
        end_time="2018/05/01 00:30:00",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "RRP"],
    )

    assert list(data.columns) == ["SETTLEMENTDATE", "RRP"]


def test_multi_day_cross_month_query(nemosis_fixture):
    """3-day window straddling Aug→Sep 2024 exercises `year_and_month_gen`'s
    multi-month iteration (which the boundary matrix's <=5.5h windows
    don't reach). 3 days × 288 intervals = 864 timestamps per region."""
    data = dynamic_data_compiler(
        start_time="2024/08/30 00:00:00",
        end_time="2024/09/02 00:00:00",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP", "INTERVENTION"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert set(data["REGIONID"]) == FIXTURED_REGIONS
    for region in FIXTURED_REGIONS:
        timestamps = sorted(data[data["REGIONID"] == region]["SETTLEMENTDATE"].tolist())
        assert len(timestamps) == 864
        assert timestamps[0] == pd.Timestamp("2024-08-30 00:05:00")
        assert timestamps[-1] == pd.Timestamp("2024-09-02 00:00:00")


def test_sub_stride_window_returns_empty(nemosis_fixture):
    """30-second window falls between 5-min stride points — should return
    an empty frame (not error), confirming the filter layer handles
    zero-row cases without surfacing an exception."""
    data = dynamic_data_compiler(
        start_time="2024/08/01 00:30:00",
        end_time="2024/08/01 00:30:30",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP"],
    )
    assert data.empty


@pytest.mark.parametrize(
    "case", boundary_cases("DISPATCHPRICE"), ids=lambda c: c.id
)
def test_dispatch_price_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP", "INTERVENTION"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="REGIONID",
        expected_entities=FIXTURED_REGIONS,
    )
