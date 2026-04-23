"""Tests for MNSP_INTERCONNECTOR.

Effective-date config table: per-MNSP-link routing into a parent
"interconnector" abstraction (e.g. BLNKTAS → T-V-MNSP1). search_type="all"
— test narrows the scan window via monkeypatched `nem_data_model_start_time`.

Filtered on LINKID rather than INTERCONNECTORID because the
INTERCONNECTORIDs in this table are the MNSP-side abstractions
(T-V-MNSP1, V-S-MNSP1, N-Q-MNSP1), not the regulated AC interconnectors
in our INTERCONNECTORS constant.

Fixtured eras: 2021-05; LINKID ∈ {BLNKTAS, BLNKVIC}.
"""
from nemosis import defaults, dynamic_data_compiler


def test_mnsp_interconnector_returns_fixtured_rows(nemosis_fixture, monkeypatch):
    monkeypatch.setattr(defaults, "nem_data_model_start_time", "2021/05/01 00:00:00")

    data = dynamic_data_compiler(
        start_time="2021/05/01 00:00:00",
        end_time="2021/05/01 01:00:00",
        table_name="MNSP_INTERCONNECTOR",
        raw_data_location=str(nemosis_fixture),
        select_columns=["LINKID", "INTERCONNECTORID", "EFFECTIVEDATE", "VERSIONNO"],
    )

    assert not data.empty
    assert set(data["LINKID"]) <= {"BLNKTAS", "BLNKVIC"}
