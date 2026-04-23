"""Tests for AEMO's static (time-independent) tables.

These four tables share one test file because they have no date dimension —
each is downloaded once as a snapshot and cached verbatim. The fixtures are
frozen copies of what AEMO was serving when build.py last ran.

  - ELEMENTS_FCAS_4_SECOND      — CSV, scraped from a directory index
  - VARIABLES_FCAS_4_SECOND     — CSV, direct download
  - Generators and Scheduled Loads — XLS workbook, direct download
  - FCAS Providers              — different tab of the same XLS workbook
"""
from nemosis import static_table


def test_elements_fcas_returns_non_empty_frame(nemosis_fixture):
    data = static_table("ELEMENTS_FCAS_4_SECOND", str(nemosis_fixture))
    assert not data.empty


def test_variables_fcas_returns_non_empty_frame(nemosis_fixture):
    data = static_table("VARIABLES_FCAS_4_SECOND", str(nemosis_fixture))
    assert not data.empty


def test_generators_and_scheduled_loads_returns_non_empty_frame(nemosis_fixture):
    data = static_table("Generators and Scheduled Loads", str(nemosis_fixture))
    assert not data.empty


def test_fcas_providers_returns_non_empty_frame(nemosis_fixture):
    # "FCAS Providers" reads a different sheet of the same XLS workbook as
    # "Generators and Scheduled Loads". NEMOSIS's static_downloader_map has
    # no entry for "FCAS Providers", so the first fetch errors on an empty
    # cache — prime it by fetching the sibling table.
    static_table("Generators and Scheduled Loads", str(nemosis_fixture))
    data = static_table("FCAS Providers", str(nemosis_fixture))
    assert not data.empty


# ---------------------------------------------------------------------------
# Filter exercises — confirm `filter_cols`/`filter_values` actually narrow
# the result. `static_table` requires `select_columns` whenever `filter_cols`
# is passed (the filter cols must be a subset of the selected cols).
# ---------------------------------------------------------------------------

def test_elements_fcas_filter_narrows(nemosis_fixture):
    cols = ["ELEMENTNUMBER", "EMSNAME", "ELEMENTTYPE"]
    full = static_table("ELEMENTS_FCAS_4_SECOND", str(nemosis_fixture), select_columns=cols)
    filtered = static_table(
        "ELEMENTS_FCAS_4_SECOND", str(nemosis_fixture),
        select_columns=cols, filter_cols=["ELEMENTTYPE"], filter_values=[["GEN"]],
    )
    assert 0 < len(filtered) < len(full)
    assert set(filtered["ELEMENTTYPE"]) == {"GEN"}


def test_variables_fcas_filter_narrows(nemosis_fixture):
    cols = ["VARIABLENUMBER", "VARIABLETYPE"]
    full = static_table("VARIABLES_FCAS_4_SECOND", str(nemosis_fixture), select_columns=cols)
    filtered = static_table(
        "VARIABLES_FCAS_4_SECOND", str(nemosis_fixture),
        select_columns=cols, filter_cols=["VARIABLETYPE"], filter_values=[["MW"]],
    )
    assert 0 < len(filtered) < len(full)
    assert set(filtered["VARIABLETYPE"]) == {"MW"}


def test_generators_filter_narrows(nemosis_fixture):
    cols = ["Participant", "Region", "DUID"]
    full = static_table("Generators and Scheduled Loads", str(nemosis_fixture), select_columns=cols)
    filtered = static_table(
        "Generators and Scheduled Loads", str(nemosis_fixture),
        select_columns=cols, filter_cols=["Region"], filter_values=[["SA1"]],
    )
    assert 0 < len(filtered) < len(full)
    assert set(filtered["Region"]) == {"SA1"}


def test_fcas_providers_filter_narrows(nemosis_fixture):
    static_table("Generators and Scheduled Loads", str(nemosis_fixture))
    cols = ["Participant", "Region", "DUID", "Bid Type"]
    full = static_table("FCAS Providers", str(nemosis_fixture), select_columns=cols)
    filtered = static_table(
        "FCAS Providers", str(nemosis_fixture),
        select_columns=cols, filter_cols=["Region"], filter_values=[["SA1"]],
    )
    assert 0 < len(filtered) < len(full)
    assert set(filtered["Region"]) == {"SA1"}
