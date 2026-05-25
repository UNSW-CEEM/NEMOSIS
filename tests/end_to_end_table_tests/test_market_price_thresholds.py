"""Tests for MARKET_PRICE_THRESHOLDS.

Effective-date config table: NEM market price floor and cap, plus
admin-price thresholds. Tiny — 12 rows total — so no row filter is
applied at fixture-build time. search_type="all".

Fixtured eras: 2021-05.
"""
from nemosis import defaults, dynamic_data_compiler


def test_market_price_thresholds_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="MARKET_PRICE_THRESHOLDS",
        raw_data_location=str(nemosis_fixture),
        select_columns=["EFFECTIVEDATE", "VERSIONNO", "VOLL", "MARKETPRICEFLOOR"],
    )

    assert not data.empty
