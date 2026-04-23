"""Tests for INTERCONNECTOR.

Effective-date config table: one row per interconnector, `LASTCHANGED`
records the last config change. NEMOSIS classifies it as search_type
"all" — by default it would iterate every month from July 2009 through
end_time. The test narrows that window by monkeypatching
`nem_data_model_start_time` so only a single fixture month is probed.

Fixtured eras: 2021-05; INTERCONNECTORID ∈ {VIC1-NSW1}.
"""
import pandas as pd

from nemosis import defaults, dynamic_data_compiler


def test_interconnector_returns_fixtured_row(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="INTERCONNECTOR",
        raw_data_location=str(nemosis_fixture),
        select_columns=["INTERCONNECTORID", "REGIONFROM", "REGIONTO", "LASTCHANGED"],
    )

    assert not data.empty
    assert set(data["INTERCONNECTORID"]) == {"VIC1-NSW1"}
