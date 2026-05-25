"""Tests for PARTICIPANT.

Effective-date config table: registered NEM participants. search_type="all".

Fixtured eras: 2018-05, 2021-05;
PARTICIPANTID ∈ {AGLE, INFIGENH, ERMPOWER}.

ERMPOWER is fixtured specifically to exercise `filter_on_last_changed`
across a wide window — its LASTCHANGED moves between 2018 and 2021
archives, whereas AGLE and INFIGENH sit on 2013 LASTCHANGED values in
both archives and so each appear exactly once even in a multi-year query.
"""
from nemosis import defaults, dynamic_data_compiler


FIXTURED_PIDS = {"AGLE", "INFIGENH", "ERMPOWER"}


def test_participant_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="PARTICIPANT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["PARTICIPANTID", "PARTICIPANTCLASSID", "NAME", "LASTCHANGED"],
    )

    assert not data.empty
    assert set(data["PARTICIPANTID"]) <= FIXTURED_PIDS


def test_narrow_window_returns_one_row_per_participantid(nemosis_fixture, monkeypatch):
    """5-minute window: `filter_on_last_changed` surfaces exactly the
    currently-effective row for each participant — no history."""
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 00:05:00",
        table_name="PARTICIPANT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["PARTICIPANTID", "NAME", "LASTCHANGED"],
    )

    counts = data.groupby("PARTICIPANTID").size()
    assert (counts == 1).all(), counts.to_dict()


def test_wide_window_surfaces_lastchanged_history(nemosis_fixture, monkeypatch):
    """Multi-year window: ERMPOWER has distinct LASTCHANGED values in the
    2018-05 and 2021-05 archives, so both rows survive `filter_on_last_changed`.
    AGLE/INFIGENH never changed — still 1 row each. The contrast confirms
    the filter is surfacing history, not collapsing to most-recent-per-PID."""
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2018/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2018/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="PARTICIPANT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["PARTICIPANTID", "NAME", "LASTCHANGED"],
    )

    counts = data.groupby("PARTICIPANTID").size().to_dict()
    assert counts.get("ERMPOWER", 0) > 1, counts
    assert counts.get("AGLE") == 1, counts
    assert counts.get("INFIGENH") == 1, counts
