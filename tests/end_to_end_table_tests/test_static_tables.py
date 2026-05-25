"""Tests for AEMO's static (time-independent) tables.

These tables have no date dimension — each is downloaded once as a
snapshot and cached verbatim. The fixtures are frozen copies of what
AEMO was serving when build.py last ran.

  - ELEMENTS_FCAS_4_SECOND      — CSV, scraped from a directory index
  - VARIABLES_FCAS_4_SECOND     — CSV, direct download
  - Generators and Scheduled Loads — XLS workbook, direct download

"FCAS Providers" was a fourth static table sharing the registration XLS
workbook with "Generators and Scheduled Loads". AEMO has since emptied
the underlying sheet and migrated the data to a weekly archive on
nemweb — see issue #92. The handler now raises early; coverage is the
deprecation-error tests below.
"""
import pytest

from nemosis import static_table
from nemosis.custom_errors import UserInputError


def test_elements_fcas_returns_non_empty_frame(nemosis_fixture):
    data = static_table("ELEMENTS_FCAS_4_SECOND", str(nemosis_fixture))
    assert not data.empty


def test_variables_fcas_returns_non_empty_frame(nemosis_fixture):
    data = static_table("VARIABLES_FCAS_4_SECOND", str(nemosis_fixture))
    assert not data.empty


def test_generators_and_scheduled_loads_returns_non_empty_frame(nemosis_fixture):
    data = static_table("Generators and Scheduled Loads", str(nemosis_fixture))
    assert not data.empty


def test_fcas_providers_raises_deprecation_error(nemosis_fixture):
    """FCAS Providers was deprecated when AEMO migrated the data out of
    the registration XLS workbook (issue #92). The handler raises
    immediately, regardless of cache state, with a pointer to the new
    nemweb endpoint."""
    with pytest.raises(UserInputError, match="no longer available"):
        static_table("FCAS Providers", str(nemosis_fixture))


def test_fcas_providers_deprecation_message_points_to_new_endpoint(nemosis_fixture):
    """The deprecation error must include the new AEMO endpoint URL so
    users have somewhere to go. This test locks the URL into the
    message; updating where the data lives requires updating this test."""
    with pytest.raises(UserInputError) as excinfo:
        static_table("FCAS Providers", str(nemosis_fixture))
    msg = str(excinfo.value)
    assert "ANCILLARY_SERVICES_REPORTS" in msg
    assert "issue #92" in msg


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


# FCAS Providers filter test removed — the table is deprecated (see
# issue #92), so there's no longer a happy path to filter against.
# The deprecation-error tests above cover the new behaviour.
