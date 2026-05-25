"""Tests for LOSSFACTORMODEL.

Effective-date config table: per-region loss factors for each
interconnector. search_type="all".

Fixtured eras: 2021-05; INTERCONNECTORID ∈ {VIC1-NSW1}.
"""
from nemosis import defaults, dynamic_data_compiler


def test_loss_factor_model_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="LOSSFACTORMODEL",
        raw_data_location=str(nemosis_fixture),
        select_columns=["INTERCONNECTORID", "REGIONID", "EFFECTIVEDATE", "VERSIONNO", "DEMANDCOEFFICIENT"],
    )

    assert not data.empty
    assert set(data["INTERCONNECTORID"]) == {"VIC1-NSW1"}
