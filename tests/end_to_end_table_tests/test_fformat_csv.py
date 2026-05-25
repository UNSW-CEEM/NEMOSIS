"""Coverage for `dynamic_data_compiler(fformat="csv")`.

The CSV path is materially different from feather/parquet — it skips the
cache-read/cache-write branch entirely and re-parses the raw AEMO CSV on
every call. Feather/parquet coverage lives in `test_cache_compiler.py`;
this file locks in that the CSV branch doesn't silently break.

`cache_compiler` only accepts feather/parquet, so CSV is exclusively a
`dynamic_data_compiler` concern.
"""
from nemosis import dynamic_data_compiler


def test_csv_fformat_round_trip(nemosis_fixture):
    data = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00",
        end_time="2018/05/01 00:30:00",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        fformat="csv",
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP", "INTERVENTION"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )
    assert not data.empty
    assert data["SETTLEMENTDATE"].dtype == "datetime64[ns]"
    assert data["REGIONID"].dtype == "object"
    assert not all(data.dtypes == "object")


def test_keep_csv_false_removes_csv(nemosis_fixture):
    """With `fformat="csv"` and `keep_csv=False`, the raw CSV is fetched,
    used to produce the return frame, and then deleted.

    Note: the downloaded .zip is retained by default (see #56 — Matt's
    use case is slow internet, so caching the compressed archive
    avoids re-downloading on subsequent runs). Users who want a truly
    empty cache can pass keep_zip=False — covered by
    test_keep_zip_false_removes_zip in
    tests/end_to_end_table_tests/test_cache_compiler.py.
    """
    dynamic_data_compiler(
        start_time="2018/05/01 00:00:00",
        end_time="2018/05/01 00:30:00",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        fformat="csv",
        keep_csv=False,
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP"],
    )
    csv_files = list(nemosis_fixture.glob("*.[Cc][Ss][Vv]"))
    assert csv_files == [], f"keep_csv=False should remove the CSV; found: {csv_files}"
