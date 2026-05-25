"""Tests for SPDCONNECTIONPOINTCONSTRAINT.

Effective-date config table: tells SPD which connection points feed
into which generic constraints, and with what factor. search_type="all".

Filtered by GENCONID rather than CONNECTIONPOINTID since we don't have a
known connection-point shortlist; picked a low-volume constraint
("S>>MKRB_NIL_WEMWP4", ~20 rows in 2021-05) with an EFFECTIVEDATE before
the test window so NEMOSIS's `filter_on_effective_date` keeps it in.

Fixtured eras: 2021-05; GENCONID ∈ {S>>MKRB_NIL_WEMWP4}.
"""
from nemosis import defaults, dynamic_data_compiler


def test_spd_connection_point_constraint_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="SPDCONNECTIONPOINTCONSTRAINT",
        raw_data_location=str(nemosis_fixture),
        select_columns=["CONNECTIONPOINTID", "GENCONID", "EFFECTIVEDATE", "VERSIONNO", "BIDTYPE", "FACTOR"],
    )

    assert not data.empty
    assert set(data["GENCONID"]) == {"S>>MKRB_NIL_WEMWP4"}
