"""Tests for DUDETAIL.

Effective-date config table: registration details per dispatchable unit
(connection point, registered capacity, station ID, etc.).
search_type="all".

Fixtured eras: 2021-05; DUID ∈ {AGLHAL, HDWF2}.
"""
from nemosis import defaults, dynamic_data_compiler


def test_dudetail_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="DUDETAIL",
        raw_data_location=str(nemosis_fixture),
        select_columns=["DUID", "EFFECTIVEDATE", "VERSIONNO", "CONNECTIONPOINTID", "REGISTEREDCAPACITY"],
    )

    assert not data.empty
    assert set(data["DUID"]) <= {"AGLHAL", "HDWF2"}
