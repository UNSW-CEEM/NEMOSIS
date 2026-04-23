"""Tests for DISPATCHCONSTRAINT.

Per-constraint five-minute state (RHS, marginal value, LHS terms). The
pinned CONSTRAINTID `DATASNAP_DFS_Q_CLST` binds every 5-min interval on
day 1 across every fixtured era (verified empirically 2018-05 → 2025-01),
so the standard boundary matrix applies here.

Fixtured eras: 2018-05, 2020-01, 2021-05, 2022-01, 2024-08, 2025-01;
CONSTRAINTID ∈ {DATASNAP_DFS_Q_CLST}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler

from _boundaries import assert_boundary_shape, boundary_cases

FIXTURED_CONSTRAINTS = {"DATASNAP_DFS_Q_CLST"}


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/05/01 00:00:00",
    "2024/08/01 00:00:00",
])
def test_constraint_rows_are_within_requested_window(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=2)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="DISPATCHCONSTRAINT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "CONSTRAINTID", "INTERVENTION", "RHS"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert set(data["CONSTRAINTID"]) == FIXTURED_CONSTRAINTS
    assert data["SETTLEMENTDATE"].min() >= start
    assert data["SETTLEMENTDATE"].max() <= end


@pytest.mark.parametrize(
    "case", boundary_cases("DISPATCHCONSTRAINT"), ids=lambda c: c.id
)
def test_dispatch_constraint_boundary(nemosis_fixture, case):
    data = dynamic_data_compiler(
        start_time=case.start_str,
        end_time=case.end_str,
        table_name="DISPATCHCONSTRAINT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "CONSTRAINTID", "INTERVENTION", "RHS"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )

    assert_boundary_shape(
        data, case,
        date_col="SETTLEMENTDATE",
        entities_col="CONSTRAINTID",
        expected_entities=FIXTURED_CONSTRAINTS,
    )
