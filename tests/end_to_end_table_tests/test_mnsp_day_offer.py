"""Tests for MNSP_DAYOFFER.

Daily bid offers from MNSP interconnectors (price bands, daily totals;
keyed on LINKID). MMS monthly archive at every era — the table kept
its name across the Aug-2024 archive rename, unlike MNSP_PEROFFER, so
coverage extends to 2024-09.

Fixtured eras: 2018-05, 2021-05, 2024-09; LINKID ∈ {BLNKTAS, BLNKVIC}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/05/01 00:00:00",
    "2024/09/01 00:00:00",
])
def test_day_offers_returned_for_fixtured_links(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="MNSP_DAYOFFER",
        raw_data_location=str(nemosis_fixture),
        select_columns=[
            "SETTLEMENTDATE", "OFFERDATE", "VERSIONNO",
            "PARTICIPANTID", "LINKID", "PRICEBAND1",
        ],
    )

    assert not data.empty
    assert set(data["LINKID"]) <= {"BLNKTAS", "BLNKVIC"}
