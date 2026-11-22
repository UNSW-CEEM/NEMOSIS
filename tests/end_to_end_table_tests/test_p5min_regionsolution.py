"""Regional P5MIN forecasts retain every run and its target intervals.

Real AEMO fixtures cover the August 2024 filename change and a year boundary.
Unlike dispatch, there are twelve target intervals per region per run, so the
single-timestamp boundary helper is not suitable for this table.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from nemosis import (
    cache_compiler,
    defaults,
    downloader,
    dynamic_data_compiler,
    processing_info_maps,
)
from nemosis.write_file_names import write_p5min_file_names
from nemosis.custom_errors import UserInputError

TABLE = "P5MIN_REGIONSOLUTION"
KEY = ["RUN_DATETIME", "INTERVAL_DATETIME", "REGIONID"]


def fetch(cache, start, end, **kwargs):
    return dynamic_data_compiler(start, end, TABLE, str(cache), **kwargs)


def assert_forecast_runs(data, start, end, regions):
    runs = pd.date_range(start + timedelta(minutes=5), end, freq="5min")
    assert set(data.REGIONID) == regions
    assert set(data.RUN_DATETIME) == set(runs)
    assert len(data) == len(runs) * len(regions) * 12
    assert not data.duplicated(KEY).any()
    assert pd.api.types.is_datetime64_any_dtype(data.RUN_DATETIME)
    assert pd.api.types.is_datetime64_any_dtype(data.INTERVAL_DATETIME)
    assert pd.api.types.is_numeric_dtype(data.RRP)
    assert data.RRP.notna().all()
    for (_, run), rows in data.groupby(["REGIONID", "RUN_DATETIME"]):
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
    assert_forecast_runs(data, start, end, {"NSW1", "SA1"})
    # Forecast targets extend beyond the run-time query's upper bound.
    assert data.INTERVAL_DATETIME.max() == end + timedelta(minutes=55)


def test_cross_month_query_keeps_overlapping_forecast_vintages(nemosis_fixture):
    start = datetime(2024, 7, 31, 23, 30)
    end = datetime(2024, 8, 1, 0, 30)
    data = fetch(nemosis_fixture, start, end)
    assert_forecast_runs(data, start, end, {"NSW1", "SA1"})
    target = data[
        (data.REGIONID == "NSW1")
        & (data.INTERVAL_DATETIME == datetime(2024, 8, 1, 0, 30))
    ]
    assert target.RUN_DATETIME.nunique() == 12


def test_select_and_filter_regional_price(nemosis_fixture):
    data = fetch(
        nemosis_fixture,
        datetime(2024, 7, 1),
        datetime(2024, 7, 1, 0, 5),
        select_columns=KEY + ["INTERVENTION", "RRP"],
        filter_cols=["REGIONID", "INTERVENTION"],
        filter_values=(["NSW1"], [0]),
    )
    assert_forecast_runs(
        data, datetime(2024, 7, 1), datetime(2024, 7, 1, 0, 5), {"NSW1"}
    )
    assert set(data.columns) == set(KEY + ["INTERVENTION", "RRP"])
    first = data.loc[data.INTERVAL_DATETIME == datetime(2024, 7, 1, 0, 5)].iloc[0]
    assert first.RRP == pytest.approx(61.62)  # Original AEMO archive value.


def test_all_columns_csv(nemosis_fixture):
    data = fetch(
        nemosis_fixture,
        datetime(2024, 8, 1),
        datetime(2024, 8, 1, 0, 5),
        fformat="csv",
        select_columns="all",
    )
    assert_forecast_runs(
        data, datetime(2024, 8, 1), datetime(2024, 8, 1, 0, 5), {"NSW1", "SA1"}
    )
    assert "TOTALDEMAND" in data


@pytest.mark.parametrize("fformat", ["csv", "parquet", "feather"])
def test_physical_and_fcas_forecasts(nemosis_fixture, fformat):
    data = fetch(
        nemosis_fixture,
        datetime(2024, 7, 1),
        datetime(2024, 7, 1, 0, 5),
        fformat=fformat,
        filter_cols=["REGIONID", "INTERVENTION"],
        filter_values=(["NSW1"], [0]),
    )
    assert_forecast_runs(
        data, datetime(2024, 7, 1), datetime(2024, 7, 1, 0, 5), {"NSW1"}
    )
    first = data.loc[data.INTERVAL_DATETIME == datetime(2024, 7, 1, 0, 5)].iloc[0]
    # Original AEMO values exercise supply, renewables and forecast FCAS prices.
    for column, expected in {
        "TOTALDEMAND": 8298.26,
        "AVAILABLEGENERATION": 13033.832,
        "DISPATCHABLEGENERATION": 8066.25,
        "DEMANDFORECAST": -37,
        "NETINTERCHANGE": -272.01,
        "UIGF": 732.83337,
        "SS_SOLAR_UIGF": 0,
        "SS_WIND_UIGF": 732.83337,
        "SEMISCHEDULE_CLEAREDMW": 732.832,
        "RAISE6SECRRP": 1.11,
        "RAISE60SECRRP": 0.39,
        "RAISE5MINRRP": 0.39,
        "RAISEREGRRP": 10.98,
    }.items():
        assert pd.api.types.is_numeric_dtype(data[column])
        assert first[column] == pytest.approx(expected)
    assert data.TOTALDEMAND.nunique() > 1  # Preserve the forecast path.
    assert pd.api.types.is_datetime64_any_dtype(data.LASTCHANGED)
    # These columns are absent from this source vintage; never fabricate zeroes.
    assert "RAISE1SECRRP" not in data
    assert "SS_SOLAR_AVAILABILITY" not in data


@pytest.mark.parametrize("fformat", ["parquet", "feather"])
def test_rebuild_upgrades_price_only_cache(nemosis_fixture, fformat):
    start, end = datetime(2024, 8, 2), datetime(2024, 8, 2, 0, 5)
    original_columns = KEY + ["INTERVENTION", "RRP", "ROP", "LASTCHANGED"]
    cache_compiler(
        start, end, TABLE, str(nemosis_fixture),
        select_columns=original_columns, fformat=fformat, rebuild=True,
    )
    with pytest.raises(UserInputError, match="TOTALDEMAND"):
        fetch(
            nemosis_fixture, start, end, fformat=fformat,
            select_columns=KEY + ["TOTALDEMAND", "RAISE1SECRRP"],
        )
    cache_compiler(
        start, end, TABLE, str(nemosis_fixture), fformat=fformat, rebuild=True,
    )
    data = fetch(nemosis_fixture, start, end, fformat=fformat)
    assert set(data.columns) == set(defaults.table_columns[TABLE])
    assert data.TOTALDEMAND.notna().all()
    assert data.RAISE1SECRRP.notna().all()


def test_cache_reuse_and_rebuild(nemosis_fixture, monkeypatch):
    start, end = datetime(2024, 8, 2), datetime(2024, 8, 2, 1)
    original = fetch(nemosis_fixture, start, end)
    cached = list(nemosis_fixture.glob("*.parquet"))
    assert cached
    modified = {p: p.stat().st_mtime_ns for p in cached}
    assert not list(nemosis_fixture.glob("*.CSV"))

    real_download = downloader.download_unzip_csv

    def cached_zips_only(url, *args, **kwargs):
        if "FILE01" in url:
            pytest.fail("A cached archive must not be downloaded again")
        return real_download(url, *args, **kwargs)  # Probe for FILE02.

    with monkeypatch.context() as patch:
        patch.setattr(downloader, "download_unzip_csv", cached_zips_only)
        repeated = fetch(nemosis_fixture, start, end)
    pd.testing.assert_frame_equal(original, repeated)
    assert {p: p.stat().st_mtime_ns for p in cached} == modified

    # Typed-cache compilation and rebuild use the same forecast schema.
    cache_compiler(start, end, TABLE, str(nemosis_fixture), rebuild=True)
    stored = pd.read_parquet(cached[0])
    assert pd.api.types.is_datetime64_any_dtype(stored.INTERVAL_DATETIME)
    assert pd.api.types.is_numeric_dtype(stored.RRP)
    # Typed caches infer numerics over the full month, so an integer-only query
    # may become float when other targets in the month contain fractions.
    pd.testing.assert_frame_equal(
        original, fetch(nemosis_fixture, start, end), check_dtype=False,
    )


def test_multipart_archives_are_concatenated(tmp_path, monkeypatch):
    calls = []

    def download(year, month, day, chunk, index, stub, directory, **kwargs):
        calls.append(chunk)
        if chunk > 2:
            return None
        target = f"2025/01/02 00:{5 * chunk:02d}:00"
        (tmp_path / f"{stub}.CSV").write_text(
            "C,AEMO\n"
            "I,P5MIN,REGIONSOLUTION,9,RUN_DATETIME,INTERVAL_DATETIME,REGIONID,RRP\n"
            f'D,P5MIN,REGIONSOLUTION,9,"2025/01/02 00:05:00","{target}",NSW1,{chunk}\n'
            "C,END OF REPORT,4\n"
        )
        return True

    monkeypatch.setitem(processing_info_maps.downloader, "P5MIN", download)
    data = fetch(tmp_path, datetime(2025, 1, 2), datetime(2025, 1, 2, 0, 5))
    assert calls == [1, 2, 3]
    assert data.RRP.tolist() == [1, 2]
    assert data.INTERVAL_DATETIME.tolist() == [
        datetime(2025, 1, 2, 0, 5),
        datetime(2025, 1, 2, 0, 10),
    ]


@pytest.mark.parametrize(
    "year,month,expected",
    [
        ("2024", "07", "PUBLIC_DVD_P5MIN_REGIONSOLUTION_ALL_202407010000"),
        ("2024", "08", "PUBLIC_ARCHIVE#P5MIN_REGIONSOLUTION#ALL#FILE01#202408010000"),
        ("2025", "01", "PUBLIC_ARCHIVE#P5MIN_REGIONSOLUTION#ALL#FILE01#202501010000"),
    ],
)
def test_archive_names(year, month, expected, tmp_path):
    stub, path = write_p5min_file_names(
        TABLE, month, year, None, 1, None, str(tmp_path)
    )
    assert stub == expected
    assert path == str(tmp_path / expected)
    assert defaults.primary_date_columns[TABLE] == "RUN_DATETIME"
