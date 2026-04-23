"""Tests for SPDREGIONCONSTRAINT.

Effective-date config table: tells SPD which regions feed into which
generic constraints, and with what factor. search_type="all".

Fixtured eras: 2021-05; REGIONID ∈ {SA1, NSW1}.
"""
from nemosis import defaults, dynamic_data_compiler


def test_spd_region_constraint_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="SPDREGIONCONSTRAINT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["REGIONID", "GENCONID", "EFFECTIVEDATE", "VERSIONNO", "FACTOR"],
    )

    assert not data.empty
    assert set(data["REGIONID"]) <= {"SA1", "NSW1"}
