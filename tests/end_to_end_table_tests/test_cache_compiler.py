"""Tests for `cache_compiler` — the public sibling of `dynamic_data_compiler`
that writes typed feather / parquet files to disk for downstream consumers.

Verifies (a) the round-trip through cache → dynamic_data_compiler preserves
typed columns, (b) `select_columns` narrows the cached file itself, not
just the returned frame, (c) cache directory is auto-created when missing,
(d) keep_csv=True is the default behaviour, and (e) partial cache files
are cleaned up when a feather/parquet write fails mid-flight.
"""
import pandas as pd
import pytest

from nemosis import cache_compiler, data_fetch_methods, dynamic_data_compiler
from nemosis.custom_errors import UserInputError


START = "2018/05/01 00:00:00"
END = "2018/05/01 00:30:00"


@pytest.mark.parametrize("fformat", ["feather", "parquet"])
def test_round_trip_preserves_dtypes(nemosis_fixture, fformat):
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        fformat=fformat,
    )
    data = dynamic_data_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        fformat=fformat,
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP", "INTERVENTION"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )
    assert not data.empty
    assert data["SETTLEMENTDATE"].dtype == "datetime64[ns]"
    assert data["REGIONID"].dtype == "object"
    # If all dtypes were object, typing failed somewhere in the pipeline.
    assert not all(data.dtypes == "object")


def test_select_columns_narrows_cached_file(nemosis_fixture):
    """`cache_compiler` with `select_columns` should write only those
    columns to the cached file, not just filter on read."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        fformat="parquet",
        select_columns=["SETTLEMENTDATE", "REGIONID"],
        rebuild=True,
    )
    # Cache writes one parquet per fetched month — NEMOSIS's prev-month
    # buffer means day-1 midnight queries always pull April too.
    parquet_files = list(nemosis_fixture.glob("*.parquet"))
    assert len(parquet_files) >= 1, parquet_files
    for path in parquet_files:
        on_disk = pd.read_parquet(path)
        assert set(on_disk.columns) == {"SETTLEMENTDATE", "REGIONID"}, path


def test_creates_cache_directory_when_missing(nemosis_fixture):
    """cache_compiler builds caches — making the user mkdir first is needless
    friction. If the destination is missing, create it."""
    target = nemosis_fixture / "new_subdir_that_does_not_exist"
    assert not target.exists()

    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(target),
    )

    assert target.is_dir()
    assert list(target.glob("*.feather")), "cache should be populated"


def test_raises_when_cache_path_is_a_file(nemosis_fixture):
    """A path that points at a regular file is clearly a typo, not a
    cache directory to create — surface that as UserInputError."""
    not_a_dir = nemosis_fixture / "oops.txt"
    not_a_dir.write_text("I am not a directory")

    with pytest.raises(UserInputError, match="exists as a file"):
        cache_compiler(
            start_time=START, end_time=END,
            table_name="DISPATCHPRICE",
            raw_data_location=str(not_a_dir),
        )


def test_keep_csv_true_by_default_keeps_fetched_csv(nemosis_fixture):
    """When cache_compiler actually has to fetch (no existing feather, so
    the code path that downloads + extracts a CSV runs), the default
    keep_csv=True must leave the extracted CSV on disk alongside the
    feather. rebuild=True forces the fetch path so this test isn't
    dependent on tmp_path being empty at start.

    AEMO zips contain CSV files with an uppercase .CSV extension —
    NEMOSIS handles this internally with [cC][sS][vV] globs and the
    test does the same."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        # no keep_csv kwarg — exercises the default
    )
    csv_files = list(nemosis_fixture.glob("*DISPATCHPRICE*.[Cc][Ss][Vv]"))
    assert csv_files, "default keep_csv=True should retain the fetched CSV"


def test_keep_csv_false_removes_fetched_csv(nemosis_fixture):
    """Mirror of the above with the override — verifies the opt-out
    path still works (the source-side delete in _dynamic_data_fetch_loop
    fires only when keep_csv is False)."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        keep_csv=False,
    )
    csv_files = list(nemosis_fixture.glob("*DISPATCHPRICE*.[Cc][Ss][Vv]"))
    assert not csv_files, "keep_csv=False should remove the fetched CSV"


def test_existing_feather_means_no_csv_is_fetched(nemosis_fixture):
    """If the feather is already in the cache, cache_compiler must take
    the "already compiled" short-circuit and not fetch a CSV — keep_csv
    is only about retaining a CSV we actually downloaded, not about
    creating one out of thin air. Pre-populate empty feather files at
    the expected filenames (in caching_mode the existence check skips
    the read), call cache_compiler without rebuild, and verify no CSV
    appeared."""
    # April + May because NEMOSIS uses a 1-day buffer-back, so a query
    # starting 2018-05-01 also scans the 2018-04 archive.
    for month in ("201804", "201805"):
        (nemosis_fixture / f"PUBLIC_DVD_DISPATCHPRICE_{month}010000.feather").touch()

    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        # default keep_csv=True — would matter if a CSV was fetched
    )

    csv_files = list(nemosis_fixture.glob("*DISPATCHPRICE*.[Cc][Ss][Vv]"))
    assert not csv_files, (
        "keep_csv=True should NOT cause a CSV to be created when the "
        "feather already exists — the CSV branch must not run at all"
    )


@pytest.mark.parametrize("fformat", ["feather", "parquet"])
def test_write_to_format_cleans_up_partial_file_on_failure(tmp_path, monkeypatch, fformat):
    """A mid-write failure (e.g. disk full) used to leave a partial,
    unreadable feather/parquet on disk. Subsequent runs would then trip
    on the corrupt file. _write_to_format now removes the partial file
    in its except branch before re-raising the original exception."""
    target = tmp_path / f"x.{fformat}"
    df = pd.DataFrame({"a": [1, 2, 3]})

    method = "to_feather" if fformat == "feather" else "to_parquet"

    def fake_write(self, path, **kwargs):
        # Simulate a partial write — bytes hit disk before the writer errors.
        with open(path, "wb") as f:
            f.write(b"partial-bytes-from-failed-write")
        raise IOError("simulated disk full mid-write")

    monkeypatch.setattr(pd.DataFrame, method, fake_write)

    with pytest.raises(IOError, match="simulated disk full"):
        data_fetch_methods._write_to_format(df, fformat, str(target), {})

    assert not target.exists(), (
        f"partial {fformat} file should have been cleaned up after the write failure"
    )
