"""Tests for `cache_compiler` — the public sibling of `dynamic_data_compiler`
that writes typed feather / parquet files to disk for downstream consumers.

Verifies (a) the round-trip through cache → dynamic_data_compiler preserves
typed columns, (b) `select_columns` narrows the cached file itself, not
just the returned frame, (c) cache directory is auto-created when missing,
(d) keep_csv / keep_zip defaults and opt-in paths, and (e) partial cache
files are cleaned up when a feather/parquet write fails mid-flight.
"""
import pandas as pd
import pytest

from nemosis import cache_compiler, data_fetch_methods, defaults, dynamic_data_compiler
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
    assert list(target.glob("*.parquet")), "cache should be populated"


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


def test_keep_csv_true_retains_csv(nemosis_fixture):
    """When cache_compiler actually has to fetch (no existing parquet, so
    the code path that downloads + extracts a CSV runs), `keep_csv=True`
    must leave the extracted CSV on disk alongside the parquet.
    rebuild=True forces the fetch path so this test isn't dependent on
    tmp_path being empty at start.

    AEMO zips contain CSV files with an uppercase .CSV extension —
    NEMOSIS handles this internally with [cC][sS][vV] globs and the
    test does the same."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        keep_csv=True,
    )
    csv_files = list(nemosis_fixture.glob("*DISPATCHPRICE*.[Cc][Ss][Vv]"))
    assert csv_files, "keep_csv=True should retain the fetched CSV"


def test_keep_csv_default_is_false(nemosis_fixture):
    """Default behaviour — keep_csv=False removes the extracted CSV
    after the typed parquet is written, leaving only the parquet in
    the cache. Users who want raw retention opt in via keep_csv=True."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        # no keep_csv kwarg — exercises the default
    )
    csv_files = list(nemosis_fixture.glob("*DISPATCHPRICE*.[Cc][Ss][Vv]"))
    assert not csv_files, f"default keep_csv=False should remove CSV; found: {csv_files}"


def test_existing_parquet_means_no_csv_is_fetched(nemosis_fixture):
    """If the parquet is already in the cache, cache_compiler must take
    the "already compiled" short-circuit and not fetch a CSV — keep_csv
    is only about retaining a CSV we actually downloaded, not about
    creating one out of thin air. Pre-populate empty parquet files at
    the expected filenames (in caching_mode the existence check skips
    the read), call cache_compiler without rebuild, and verify no CSV
    appeared."""
    # April + May because NEMOSIS uses a 1-day buffer-back, so a query
    # starting 2018-05-01 also scans the 2018-04 archive.
    for month in ("201804", "201805"):
        (nemosis_fixture / f"PUBLIC_DVD_DISPATCHPRICE_{month}010000.parquet").touch()

    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        # default keep_csv=True — would matter if a CSV was fetched
    )

    csv_files = list(nemosis_fixture.glob("*DISPATCHPRICE*.[Cc][Ss][Vv]"))
    assert not csv_files, (
        "keep_csv=True should NOT cause a CSV to be created when the "
        "parquet already exists — the CSV branch must not run at all"
    )


def test_keep_zip_default_is_true(nemosis_fixture):
    """Default behaviour — keep_zip=True retains the downloaded zip
    after extracting the CSV, so subsequent runs (cache rebuilds,
    format changes, slow connections per #56) can re-extract without
    re-downloading."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        # no keep_zip kwarg — exercises the default
    )
    zip_files = list(nemosis_fixture.glob("*.zip"))
    assert zip_files, "default keep_zip=True should retain the downloaded zip"


def test_keep_zip_true_retains_zip(nemosis_fixture):
    """keep_zip=True (the default, per #56's slow-internet use case)
    leaves the AEMO archive zip on disk after extraction so subsequent
    runs can re-extract without re-downloading. Exercised here with
    an explicit True to lock the contract independent of the default."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        keep_zip=True,
    )
    zip_files = list(nemosis_fixture.glob("*.zip"))
    assert zip_files, "keep_zip=True should retain the downloaded zip"


def test_keep_zip_false_removes_zip(nemosis_fixture):
    """keep_zip=False is the explicit opt-out for callers who want a
    lean cache (no compressed archives on disk). The downloaded zip
    must be cleaned up after extracting the CSV."""
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        keep_zip=False,
    )
    zip_files = list(nemosis_fixture.glob("*.zip"))
    assert not zip_files, f"keep_zip=False should remove zips; found: {zip_files}"


def test_cached_zip_extracts_without_network(nemosis_fixture, monkeypatch):
    """The #56 benefit in action: with keep_zip=True on a first call,
    a subsequent call that needs the same CSV but finds the parquet
    missing should re-extract from the cached zip locally without
    hitting nemweb. Proves it by breaking the AEMO URL after the first
    call — if the second call tries to fetch, it would 404 against the
    bad URL."""
    # First call — populate cache with zip retained
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        keep_zip=True,
    )
    parquet_files = list(nemosis_fixture.glob("*.parquet"))
    zip_files = list(nemosis_fixture.glob("*.zip"))
    assert parquet_files and zip_files

    # Delete the parquet so the loop has to call _download_data again,
    # but leave the zip in place so the lower-level download_to_path
    # short-circuits on the cached file.
    for f in parquet_files:
        f.unlink()

    # Point the URL at a dead address — any actual network call would fail.
    monkeypatch.setattr(defaults, "aemo_mms_url", "http://127.0.0.1:1/dead/{}/{}/{}/{}.zip")

    # Should succeed using the cached zip, no network required.
    cache_compiler(
        start_time=START, end_time=END,
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        rebuild=True,
        keep_zip=True,
    )

    # Parquet rebuilt from the cached zip
    assert list(nemosis_fixture.glob("*.parquet")), "cache should be rebuilt from cached zip"


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
