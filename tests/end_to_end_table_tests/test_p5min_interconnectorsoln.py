"""Real P5MIN interconnector forecasts across the archive rename and year end.

Fixtures retain two links, every run and all twelve target intervals. July 2024
lacks the local price adjustment fields added in the later source schema.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from nemosis import cache_compiler, defaults, dynamic_data_compiler
from nemosis.write_file_names import write_p5min_file_names

TABLE = "P5MIN_INTERCONNECTORSOLN"
KEY = ["RUN_DATETIME", "INTERVAL_DATETIME", "INTERCONNECTORID"]
LOCAL_COLUMNS = {
    "LOCAL_PRICE_ADJUSTMENT_EXPORT", "LOCALLY_CONSTRAINED_EXPORT",
    "LOCAL_PRICE_ADJUSTMENT_IMPORT", "LOCALLY_CONSTRAINED_IMPORT",
}


def fetch(cache, start, end, **kwargs):
    return dynamic_data_compiler(start, end, TABLE, str(cache), **kwargs)


def assert_forecast_runs(data, start, end, links):
    runs = pd.date_range(start + timedelta(minutes=5), end, freq="5min")
    assert set(data.INTERCONNECTORID) == links
    assert set(data.RUN_DATETIME) == set(runs)
    assert len(data) == len(runs) * len(links) * 12
    assert not data.duplicated(KEY).any()
    for column in ["RUN_DATETIME", "INTERVAL_DATETIME", "LASTCHANGED"]:
        assert pd.api.types.is_datetime64_any_dtype(data[column])
    for column in ["MWFLOW", "METEREDMWFLOW", "IMPORTLIMIT", "EXPORTLIMIT"]:
        assert pd.api.types.is_numeric_dtype(data[column])
        assert data[column].notna().all()
    for (_, run), rows in data.groupby(["INTERCONNECTORID", "RUN_DATETIME"]):
        assert set(rows.INTERVAL_DATETIME) == set(
            pd.date_range(run, periods=12, freq="5min")
        )


@pytest.mark.parametrize("boundary", [datetime(2024, 8, 1), datetime(2025, 1, 1)])
@pytest.mark.parametrize("offset", [-1, 0])
@pytest.mark.parametrize("fformat", ["csv", "parquet", "feather"])
def test_run_window_at_month_boundary(nemosis_fixture, boundary, offset, fformat):
    start = boundary + timedelta(hours=offset)
    end = start + timedelta(hours=1)
    data = fetch(nemosis_fixture, start, end, fformat=fformat)
    assert_forecast_runs(data, start, end, {"VIC1-NSW1", "V-SA"})
    assert data.INTERVAL_DATETIME.max() == end + timedelta(minutes=55)


def test_cross_month_query_preserves_vintages_and_missing_fields(nemosis_fixture):
    start, end = datetime(2024, 7, 31, 23, 30), datetime(2024, 8, 1, 0, 30)
    data = fetch(nemosis_fixture, start, end)
    assert_forecast_runs(data, start, end, {"VIC1-NSW1", "V-SA"})
    target = data.loc[
        (data.INTERCONNECTORID == "V-SA") & (data.INTERVAL_DATETIME == end)
    ]
    assert target.RUN_DATETIME.nunique() == 12
    july = data.loc[data.RUN_DATETIME < datetime(2024, 8, 1)]
    august = data.loc[data.RUN_DATETIME > datetime(2024, 8, 1)]
    assert july[list(LOCAL_COLUMNS)].isna().all().all()
    assert august[list(LOCAL_COLUMNS)].notna().all().all()


@pytest.mark.parametrize("fformat", ["csv", "parquet", "feather"])
def test_select_and_filter_flows_and_signed_limits(nemosis_fixture, fformat):
    columns = KEY + [
        "INTERVENTION", "METEREDMWFLOW", "MWFLOW", "MWLOSSES", "IMPORTLIMIT",
        "EXPORTLIMIT", "MARGINALVALUE", "IMPORTGENCONID", "LASTCHANGED",
    ]
    start, end = datetime(2024, 7, 1), datetime(2024, 7, 1, 0, 5)
    data = fetch(
        nemosis_fixture, start, end, fformat=fformat, select_columns=columns,
        filter_cols=["INTERCONNECTORID", "INTERVENTION"],
        filter_values=(["V-SA"], [0]),
    )
    assert_forecast_runs(data, start, end, {"V-SA"})
    assert set(data.columns) == set(columns)
    assert set(data.INTERVENTION) == {0}
    first = data.loc[data.INTERVAL_DATETIME == end].iloc[0]
    assert first.METEREDMWFLOW == pytest.approx(177.48317)
    assert first.MWFLOW == pytest.approx(245)
    assert first.MWLOSSES == pytest.approx(8.0323)
    assert first.IMPORTLIMIT == pytest.approx(-550)  # Preserve AEMO's direction.
    assert first.EXPORTLIMIT == pytest.approx(600)
    assert first.IMPORTGENCONID == "S:V_550_HY_TEST"


@pytest.mark.parametrize("fformat", ["parquet", "feather"])
def test_typed_cache_rebuild_and_reuse(nemosis_fixture, fformat):
    start, end = datetime(2025, 1, 2), datetime(2025, 1, 2, 0, 5)
    original = fetch(nemosis_fixture, start, end, fformat=fformat)
    assert set(original.columns) == set(defaults.table_columns[TABLE])
    cached = list(nemosis_fixture.glob(f"*.{fformat}"))
    modified = {p: p.stat().st_mtime_ns for p in cached}
    pd.testing.assert_frame_equal(
        original, fetch(nemosis_fixture, start, end, fformat=fformat),
    )
    assert {p: p.stat().st_mtime_ns for p in cached} == modified
    cache_compiler(
        start, end, TABLE, str(nemosis_fixture), fformat=fformat, rebuild=True,
    )
    stored = getattr(pd, f"read_{fformat}")(cached[0])
    assert pd.api.types.is_datetime64_any_dtype(stored.INTERVAL_DATETIME)
    assert pd.api.types.is_numeric_dtype(stored.MWFLOW)
    assert pd.api.types.is_numeric_dtype(stored.LOCALLY_CONSTRAINED_IMPORT)
    # Full-month numeric inference can promote integer-only query values to float.
    pd.testing.assert_frame_equal(
        original, fetch(nemosis_fixture, start, end, fformat=fformat),
        check_dtype=False,
    )


def test_all_columns_csv(nemosis_fixture):
    start, end = datetime(2025, 1, 2), datetime(2025, 1, 2, 0, 5)
    data = fetch(nemosis_fixture, start, end, fformat="csv", select_columns="all")
    assert_forecast_runs(data, start, end, {"VIC1-NSW1", "V-SA"})
    assert set(defaults.table_columns[TABLE]).issubset(data.columns)


@pytest.mark.parametrize(
    "year,month,expected",
    [
        ("2024", "07", "PUBLIC_DVD_P5MIN_INTERCONNECTORSOLN_ALL_202407010000"),
        ("2024", "08", "PUBLIC_ARCHIVE#P5MIN_INTERCONNECTORSOLN#ALL#FILE01#202408010000"),
    ],
)
def test_archive_names(year, month, expected, tmp_path):
    stub, path = write_p5min_file_names(TABLE, month, year, None, 1, None, str(tmp_path))
    assert stub == expected
    assert path == str(tmp_path / expected)
