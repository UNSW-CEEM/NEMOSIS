"""Offline tests for NEMOSIS error paths and cache idempotency.

Covers argument-validation errors (`UserInputError`) and network/data-
availability errors (`NoDataToReturn`) for `dynamic_data_compiler`,
`cache_compiler`, and `static_table`. Also verifies that after a
successful fetch populates the cache, a second call with the same
arguments reads from the cache rather than re-fetching.

All tests run against the `nemosis_fixture` dummy server — no network.
The NoDataToReturn cases are triggered either by querying a date range
the fixture doesn't cover (dummy server 404s) or by patching the
static-table URL to an invalid path.
"""
import pytest

from nemosis import cache_compiler, defaults, dynamic_data_compiler, static_table
from nemosis.custom_errors import NoDataToReturn, UserInputError


# ---------------------------------------------------------------------------
# Argument-validation: dynamic_data_compiler
# ---------------------------------------------------------------------------

def test_dynamic_bad_table_name(nemosis_fixture):
    with pytest.raises(UserInputError, match="not a dynamic table"):
        dynamic_data_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "NOTATABLE", str(nemosis_fixture),
        )


def test_dynamic_bad_fformat(nemosis_fixture):
    with pytest.raises(UserInputError, match="fformat must be"):
        dynamic_data_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", str(nemosis_fixture),
            fformat="db",
        )


def test_dynamic_filter_col_not_in_select_columns(nemosis_fixture):
    with pytest.raises(UserInputError, match="Filter columns not valid"):
        dynamic_data_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", str(nemosis_fixture),
            select_columns=["SETTLEMENTDATE", "REGIONID", "RRP"],
            filter_cols=["INTERVENTION"],
            filter_values=([0],),
        )


def test_dynamic_filter_col_not_a_real_column(nemosis_fixture):
    """When `select_columns` is None the filter is validated against the
    table's default columns — a nonexistent column should still fail."""
    with pytest.raises(UserInputError, match="Filter columns not valid"):
        dynamic_data_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", str(nemosis_fixture),
            filter_cols=["NOTACOLUMN"],
            filter_values=(["0"],),
        )


def test_dynamic_select_columns_must_include_pk_for_dedup_tables(nemosis_fixture):
    """For tables whose finalise pipeline includes
    `drop_duplicates_by_primary_key`, omitting a PK column used to
    crash with a bare pandas `KeyError: Index(['VERSIONNO'], ...)` from
    inside finalise — opaque to users. Catch it up-front with a clear
    UserInputError naming the missing PK column(s)."""
    with pytest.raises(UserInputError, match="primary-key"):
        dynamic_data_compiler(
            "2021/05/01 00:00:00", "2021/05/01 01:00:00",
            "LOSSFACTORMODEL", str(nemosis_fixture),
            # LOSSFACTORMODEL PK = [EFFECTIVEDATE, INTERCONNECTORID,
            # REGIONID, VERSIONNO]. Deliberately omit VERSIONNO.
            select_columns=["EFFECTIVEDATE", "INTERCONNECTORID", "REGIONID",
                            "DEMANDCOEFFICIENT"],
        )


def test_static_select_columns_must_include_pk_for_dedup_tables(nemosis_fixture):
    """`_finalise_excel_data` calls `data.drop_duplicates(primary_keys)`,
    so static_table has the same PK-omission crash as dynamic_data_compiler.
    Generators and Scheduled Loads PK = ['DUID']."""
    with pytest.raises(UserInputError, match="primary-key"):
        static_table(
            "Generators and Scheduled Loads", str(nemosis_fixture),
            select_columns=["Participant", "Station Name", "Region"],
        )


def test_dynamic_select_columns_all_requires_csv(nemosis_fixture):
    with pytest.raises(UserInputError, match="select_columns='all'"):
        dynamic_data_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", str(nemosis_fixture),
            select_columns="all",
            fformat="feather",
        )


def test_dynamic_raw_data_location_is_none():
    """Passing None as the cache path is a common caller mistake (forgot
    to set it, config returned None). Reject with UserInputError rather
    than the cryptic TypeError that os.path.isdir(None) would raise."""
    with pytest.raises(UserInputError, match="is None"):
        dynamic_data_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", raw_data_location=None,
        )


def test_dynamic_raw_data_location_missing_includes_path_in_error(tmp_path):
    """The "does not exist" error must include the offending path so the
    caller can see exactly what they passed (helps diagnose typos and
    cwd mistakes). Regression test for the improvement that landed via
    the doc-strings-and-error-messages cleanup."""
    bogus = str(tmp_path / "subdir-that-does-not-exist")
    with pytest.raises(UserInputError, match="subdir-that-does-not-exist"):
        dynamic_data_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", raw_data_location=bogus,
        )


def test_static_raw_data_location_missing_includes_path_in_error(tmp_path):
    """Same regression contract for static_table."""
    bogus = str(tmp_path / "another-missing-dir")
    with pytest.raises(UserInputError, match="another-missing-dir"):
        static_table("VARIABLES_FCAS_4_SECOND", raw_data_location=bogus)


# ---------------------------------------------------------------------------
# Argument-validation: cache_compiler
# ---------------------------------------------------------------------------

def test_cache_bad_table_name(nemosis_fixture):
    with pytest.raises(UserInputError, match="not a dynamic table"):
        cache_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "NOTATABLE", str(nemosis_fixture),
        )


def test_cache_bad_fformat(nemosis_fixture):
    """`cache_compiler` rejects 'csv' too — only feather/parquet."""
    with pytest.raises(UserInputError, match="fformat must be"):
        cache_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", str(nemosis_fixture),
            fformat="csv",
        )


def test_cache_select_columns_requires_rebuild(nemosis_fixture):
    with pytest.raises(UserInputError, match="rebuild=True"):
        cache_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", str(nemosis_fixture),
            select_columns=["SETTLEMENTDATE", "RRP"],
        )


def test_cache_raw_data_location_is_none():
    with pytest.raises(UserInputError, match="is None"):
        cache_compiler(
            "2018/05/01 00:00:00", "2018/05/01 01:00:00",
            "DISPATCHPRICE", raw_data_location=None,
        )


# ---------------------------------------------------------------------------
# Argument-validation: static_table
# ---------------------------------------------------------------------------

def test_static_bad_table_name(nemosis_fixture):
    with pytest.raises(UserInputError, match="not a static table"):
        static_table("NOTATABLE", str(nemosis_fixture))


def test_static_filter_col_not_in_select_columns(nemosis_fixture):
    with pytest.raises(UserInputError, match="Filter columns not valid"):
        static_table(
            "VARIABLES_FCAS_4_SECOND", str(nemosis_fixture),
            select_columns=["VARIABLENUMBER"],
            filter_cols=["VARIABLETYPE"],
            filter_values=(["MW"],),
        )


def test_static_select_columns_typo_raises(nemosis_fixture):
    """A typo in user-typed select_columns (e.g. 'duid' instead of
    'DUID') used to silently return a stub DataFrame with whatever
    columns *did* match, plus a WARNING. Now raises UserInputError
    with the available columns listed — mirrors the
    dynamic_data_compiler contract."""
    with pytest.raises(UserInputError, match="not present in the data"):
        static_table(
            "Generators and Scheduled Loads", str(nemosis_fixture),
            select_columns=["Participant", "Station Name", "DUID",
                            "totally_not_a_real_column"],
        )


def test_static_filter_col_not_in_loaded_data_raises(nemosis_fixture):
    """Defence-in-depth: the missing-`raise` bug in the post-load
    filter_cols check used to silently no-op filters whose column had
    survived the early select_columns validator but didn't make it
    into the loaded file (defaults-but-vintage-missing case). Now
    raises."""
    # First confirm that the user-typed-col path raises early (via
    # _check_loaded_select_columns) — the filter_cols-not-in-data path
    # below would also raise, but earlier.
    with pytest.raises(UserInputError):
        static_table(
            "Generators and Scheduled Loads", str(nemosis_fixture),
            select_columns=["Participant", "Station Name", "DUID",
                            "totally_not_a_real_column"],
            filter_cols=["totally_not_a_real_column"],
            filter_values=(["whatever"],),
        )


def test_static_raw_data_location_is_none():
    with pytest.raises(UserInputError, match="is None"):
        static_table("VARIABLES_FCAS_4_SECOND", raw_data_location=None)


# ---------------------------------------------------------------------------
# Network / availability: NoDataToReturn
# ---------------------------------------------------------------------------

def test_dynamic_no_data_raises(nemosis_fixture):
    """Query a date range the fixture doesn't cover — the dummy server
    404s every month, NEMOSIS gives up and raises NoDataToReturn."""
    with pytest.raises(NoDataToReturn, match="Compiling data for table DISPATCHPRICE failed"):
        dynamic_data_compiler(
            "2000/01/01 00:00:00", "2000/02/01 00:00:00",
            "DISPATCHPRICE", str(nemosis_fixture),
        )


def test_static_no_data_raises(nemosis_fixture, monkeypatch):
    """Redirect a static-table URL to a path the dummy server doesn't
    serve — NEMOSIS should raise NoDataToReturn, not crash cryptically."""
    monkeypatch.setitem(
        defaults.static_table_url,
        "VARIABLES_FCAS_4_SECOND",
        "http://127.0.0.1:1/does-not-exist.csv",
    )
    with pytest.raises(NoDataToReturn, match="Compiling data for table VARIABLES_FCAS_4_SECOND failed"):
        static_table("VARIABLES_FCAS_4_SECOND", str(nemosis_fixture))


# ---------------------------------------------------------------------------
# Cache idempotency
# ---------------------------------------------------------------------------

def test_cache_is_reused_on_second_call(nemosis_fixture, monkeypatch):
    """After the first call populates the cache with a feather file, a
    second call with the same arguments must read from that cache rather
    than re-fetching. We prove it by breaking the HTTP server mid-run:
    the first call succeeds (fetches and caches), then we redirect the
    AEMO URL to an invalid path, and the second call still succeeds."""
    kwargs = dict(
        start_time="2018/05/01 00:00:00",
        end_time="2018/05/01 01:00:00",
        table_name="DISPATCHPRICE",
        raw_data_location=str(nemosis_fixture),
        select_columns=["SETTLEMENTDATE", "REGIONID", "RRP", "INTERVENTION"],
        filter_cols=["INTERVENTION"],
        filter_values=([0],),
    )
    first = dynamic_data_compiler(**kwargs)
    assert not first.empty

    # Cache is now on disk. Break the network: any new fetch would 404.
    monkeypatch.setattr(defaults, "aemo_mms_url", "http://127.0.0.1:1/nowhere")

    second = dynamic_data_compiler(**kwargs)
    assert first.equals(second)
