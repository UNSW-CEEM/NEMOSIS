"""Tests for `cache_compiler` — the public sibling of `dynamic_data_compiler`
that writes typed feather / parquet files to disk for downstream consumers.

Verifies (a) the round-trip through cache → dynamic_data_compiler preserves
typed columns and (b) `select_columns` narrows the cached file itself, not
just the returned frame.
"""
import pandas as pd
import pytest

from nemosis import cache_compiler, dynamic_data_compiler


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
