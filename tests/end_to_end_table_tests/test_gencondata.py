"""Tests for GENCONDATA.

Effective-date config table: master list of generic constraints
(constraint type, RHS, weight, description). search_type="all".

Fixtured eras: 2021-05; GENCONID ∈ {#NSW1-QLD1_RAMP_I_F}.
"""
from nemosis import defaults, dynamic_data_compiler


def test_gencondata_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="GENCONDATA",
        raw_data_location=str(nemosis_fixture),
        select_columns=["GENCONID", "EFFECTIVEDATE", "VERSIONNO", "CONSTRAINTTYPE", "CONSTRAINTVALUE"],
    )

    assert not data.empty
    assert set(data["GENCONID"]) == {"#NSW1-QLD1_RAMP_I_F"}
