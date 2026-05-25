"""Tests for MNSP_PEROFFER.

Per-period bid offers from MNSP interconnectors (keyed on LINKID
rather than DUID). MMS monthly archive at every era NEMOSIS can
currently read. After ~April 2021 AEMO began writing MNSP_BIDOFFERPERIOD
data (different column names) into the PUBLIC_DVD_MNSP_PEROFFER archives
and then at Aug-2024 renamed the archive stub outright — NEMOSIS doesn't
handle either change, so coverage stops at 2021-02.

Fixtured eras: 2018-05, 2021-02; LINKID ∈ {BLNKTAS, BLNKVIC}.
"""
import pandas as pd
import pytest

from nemosis import dynamic_data_compiler


@pytest.mark.parametrize("era_start", [
    "2018/05/01 00:00:00",
    "2021/02/01 00:00:00",
])
def test_offers_returned_for_fixtured_links(nemosis_fixture, era_start):
    start = pd.to_datetime(era_start, format="%Y/%m/%d %H:%M:%S")
    end = start + pd.Timedelta(hours=1)

    data = dynamic_data_compiler(
        start_time=start.strftime("%Y/%m/%d %H:%M:%S"),
        end_time=end.strftime("%Y/%m/%d %H:%M:%S"),
        table_name="MNSP_PEROFFER",
        raw_data_location=str(nemosis_fixture),
        select_columns=[
            "SETTLEMENTDATE", "OFFERDATE", "VERSIONNO",
            "PARTICIPANTID", "LINKID", "PERIODID", "BANDAVAIL1",
        ],
    )

    assert not data.empty
    assert set(data["LINKID"]) <= {"BLNKTAS", "BLNKVIC"}
