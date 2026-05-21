"""Tests for DISPATCH_UNIT_SCADA.

Per-unit measured output (SCADAVALUE) at five-minute resolution. Unlike
DISPATCHLOAD this table has no INTERVENTION column, so every interval
produces exactly one row per DUID.

Boundary tests (`test_dispatch_unit_scada_boundary`) are auto-generated
from `spec.DYNAMIC_TABLES["DISPATCH_UNIT_SCADA"]["eras"]` by
`_boundaries.py`.

Filtered to DUID ∈ {AGLHAL, HDWF2}.
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
        table_name="DISPATCH_UNIT_SCADA",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
    )

    assert set(data["DUID"]) == FIXTURED_DUIDS
    for duid in FIXTURED_DUIDS:
        timestamps = sorted(data[data["DUID"] == duid]["SETTLEMENTDATE"].tolist())
        assert timestamps == expected


@pytest.mark.parametrize(
    "case", boundary_cases("DISPATCH_UNIT_SCADA"), ids=lambda c: c.id
)
def test_dispatch_unit_scada_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="DISPATCH_UNIT_SCADA",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="DUID",
        expected_entities=FIXTURED_DUIDS,
    )
