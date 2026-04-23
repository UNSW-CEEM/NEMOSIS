"""Tests for DUDETAILSUMMARY.

Effective-date config table summarising DUDETAIL into start/end date
ranges per DUID (more compact than DUDETAIL for participant lookups).
search_type="end" — fetches archives only up to end_time.

Fixtured eras: 2021-05; DUID ∈ {AGLHAL, HDWF2}.
"""
from nemosis import defaults, dynamic_data_compiler


def test_dudetailsummary_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="DUDETAILSUMMARY",
        raw_data_location=str(nemosis_fixture),
        select_columns=["DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "REGIONID"],
    )

    assert not data.empty
    assert set(data["DUID"]) <= {"AGLHAL", "HDWF2"}


def test_narrow_window_one_row_per_duid(nemosis_fixture, monkeypatch):
    """`filter_on_start_date` should collapse to one effective row per DUID
    at any single instant in time. Grouping by PK minus START_DATE must
    show no duplicates — otherwise two versions of the same DUID are
    leaking through the effective-date filter."""
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    pk = defaults.table_primary_keys["DUDETAILSUMMARY"]
    group_cols = [c for c in pk if c != "START_DATE"]

    data = dynamic_data_compiler(
        start_time="2021/05/20 23:00:00",
        end_time="2021/05/20 23:05:00",
        table_name="DUDETAILSUMMARY",
        raw_data_location=str(nemosis_fixture),
        select_columns=pk,
    )

    assert not data.empty
    assert not data.duplicated(group_cols).any(), data[data.duplicated(group_cols, keep=False)]
