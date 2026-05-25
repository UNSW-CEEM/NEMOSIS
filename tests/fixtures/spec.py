"""Declarative spec of what the NEMOSIS test fixtures cover.

The fixture build script reads this module and is otherwise self-contained —
if you want to add an era or a table to the test matrix, edit here and re-run
the build script.

Adding a new era:
  1. Add a dated entry to ERAS.
  2. Append the era key to each table's `eras` list in DYNAMIC_TABLES.
  3. Run `python tests/fixtures/build.py` to download, filter, and write.
  4. Commit the new files under tests/fixtures/data/.
"""
from datetime import date


# Each era is a single calendar month pinned by its first day.
# Dates are chosen to straddle known AEMO schema / format transitions so
# the tests exercise every historical variant NEMOSIS needs to handle.
ERAS = {
    "2018-05": date(2018, 5, 1),   # pre-5-min-trading baseline
    "2020-01": date(2020, 1, 1),   # year boundary, pre-5-min, monthly bidding format
    "2021-02": date(2021, 2, 1),   # last month the MMS archive still publishes bidding
    "2021-05": date(2021, 5, 1),   # first month of daily BIDMOVE_COMPLETE layout
    "2021-10": date(2021, 10, 1),  # 5MS cutover for TRADINGPRICE/TRADINGINTERCONNECT (extra_eras only)
    "2022-01": date(2022, 1, 1),   # year boundary, 5-min dispatch, 30-min trading, bidding gap
    "2022-06": date(2022, 6, 1),   # arbitrary post-5MS month for trading tables (5MS itself cut over 2021-10)
    "2024-08": date(2024, 8, 1),   # PUBLIC_DVD_ → PUBLIC_ARCHIVE# cutover; `into` straddles the format change
    "2024-09": date(2024, 9, 1),   # first safely-past-cutover month; used by bidding tables (see note)
    "2025-01": date(2025, 1, 1),   # year boundary, PUBLIC_ARCHIVE# format, post-bidding-gap
    "recent":  date(2026, 3, 15),  # inside AEMO's current-data scrape window (see note)
}
# Year-boundary eras (2020-01, 2022-01, 2025-01) exist for the boundary test
# matrix in tests/end_to_end_table_tests/_boundaries.py — they exercise
# NEMOSIS's Dec→Jan stitch and the date generator's `month == 1` buffer-wrap
# branch. Each is added only to the dynamic time-series tables that have
# AEMO data in that month (no bidding tables for 2022-01, no MNSP_PEROFFER
# past 2021-02 due to issue #68, etc.).
# Note on "recent": AEMO's current-data pages (bidmove_complete, daily reports,
# etc.) only retain ~a few months. Any date we pick here must be recent enough
# to still be served at fixture-build time. When the commit ages out, update
# this date and rerun `build.py --rebuild` for the scrape-pattern tables.


# Entities kept when filtering rows. Diverse enough to exercise table
# behaviour, small enough that fixture files stay under a few hundred KB each.
DUIDS = ["AGLHAL", "HDWF2"]                 # OCGT + wind farm
REGIONS = ["SA1", "NSW1"]
INTERCONNECTORS = ["VIC1-NSW1"]
CONSTRAINTS = ["DATASNAP_DFS_Q_CLST"]       # CONSTRAINTID, used by DISPATCHCONSTRAINT
GENCON_IDS = ["#NSW1-QLD1_RAMP_I_F"]        # GENCONID, used by GENCONDATA / SPD* tables
SPDCPC_GENCON_IDS = ["S>>MKRB_NIL_WEMWP4"]  # smaller-volume GENCONID for SPDCPC, EFFECTIVEDATE pre-May-2021
PARTICIPANTS = ["AGLE", "INFIGENH", "ERMPOWER"]   # AGL SA + Infigen (Hornsdale) + ERM Power (LASTCHANGED moves between 2018/2021)
MNSP_LINKS = ["BLNKTAS", "BLNKVIC"]         # Basslink, both directions


# Dynamic tables. Each entry says which eras to fixture and how to filter
# rows down. The build script figures out the URL / download path from the
# table name and era date — the spec itself stays path-agnostic.
#
# Starting with a representative cross-section covering the main patterns:
#   - DISPATCHPRICE:        MMS monthly archive, region filter
#   - DISPATCHLOAD:         MMS monthly archive, DUID filter
#   - BIDPEROFFER_D:        MMS before 2021-04, BIDMOVE_COMPLETE scrape after
#   - DAILY_REGION_SUMMARY: scrape-pattern, current-data page only
# More tables get added as each one lands end-to-end.
DYNAMIC_TABLES = {
    # Five-minute dispatch tables — the workhorse set.
    "DISPATCHPRICE":             {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2024-08", "2025-01"], "filter": {"REGIONID": REGIONS}},
    "DISPATCHLOAD":              {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2024-08", "2025-01"], "filter": {"DUID": DUIDS}},
    "DISPATCH_UNIT_SCADA":       {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2024-08", "2025-01"], "filter": {"DUID": DUIDS}},
    "DISPATCHREGIONSUM":         {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2024-08", "2025-01"], "filter": {"REGIONID": REGIONS}},
    "DISPATCHINTERCONNECTORRES": {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2024-08", "2025-01"], "filter": {"INTERCONNECTORID": INTERCONNECTORS}},
    "DISPATCHCONSTRAINT":        {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2024-08", "2025-01"], "filter": {"CONSTRAINTID": CONSTRAINTS}},

    # Trading tables. TRADINGPRICE/TRADINGINTERCONNECT switched from 30-min to
    # 5-min at 2021-10-01 (the 5MS reform cutover); TRADINGLOAD/TRADINGREGIONSUM
    # were discontinued at that point and only have pre-2022 eras.
    #
    # The `extra_eras` entry for 2021-10 is a build-only era used by the
    # dedicated stride-transition test in test_trading_price.py. It is not
    # listed in `eras`, so the boundary tests skip it — the 5MS cutover
    # month is covered by that dedicated test instead.
    "TRADINGPRICE":        {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2022-06", "2024-08", "2025-01"], "extra_eras": ["2021-10"], "filter": {"REGIONID": REGIONS}},
    "TRADINGINTERCONNECT": {"eras": ["2018-05", "2020-01", "2021-05", "2022-01", "2022-06", "2024-08", "2025-01"], "extra_eras": ["2021-10"], "filter": {"INTERCONNECTORID": INTERCONNECTORS}},
    # TRADINGLOAD/TRADINGREGIONSUM were discontinued before 2022 — 2022-01 returns 404.
    "TRADINGLOAD":         {"eras": ["2018-05", "2020-01", "2021-05"], "filter": {"DUID": DUIDS}},
    "TRADINGREGIONSUM":    {"eras": ["2018-05", "2020-01", "2021-05"], "filter": {"REGIONID": REGIONS}},

    # Bidding — MMS archive all the way through. AEMO stopped publishing these
    # monthly files between March 2021 and July 2024 (see README), so 2022-01
    # is unavailable and post-2021-02 coverage jumps to 2024-09.
    #
    # Non-bidding tables use 2024-08 to straddle the PUBLIC_DVD_ → PUBLIC_ARCHIVE#
    # filename cutover. Bidding tables stay on 2024-09: 2024-07's bid archive
    # sits inside the publishing gap and a 2024-08-era prev-month buffer would
    # 404 at test time.
    "BIDPEROFFER_D": {"eras": ["2018-05", "2020-01", "2021-02", "2024-09", "2025-01"], "filter": {"DUID": DUIDS}},
    "BIDDAYOFFER_D": {"eras": ["2018-05", "2020-01", "2021-02", "2024-09", "2025-01"], "filter": {"DUID": DUIDS}},

    # MNSP bidding — same shape as BIDPEROFFER_D / BIDDAYOFFER_D but keyed on
    # LINKID (MNSP interconnectors bid as directional links).
    #
    # MNSP_PEROFFER has coverage gaps NEMOSIS can't currently read past:
    #   - From ~April 2021 AEMO began writing MNSP_BIDOFFERPERIOD data into
    #     the PUBLIC_DVD_MNSP_PEROFFER_* archives with different column names
    #     (TRADINGDATE / OFFERDATETIME instead of SETTLEMENTDATE / OFFERDATE).
    #   - At the Aug-2024 archive format switch AEMO also renamed the file
    #     stem to MNSP_BIDOFFERPERIOD outright.
    # NEMOSIS's defaults.names knows neither of these transitions, so we cap
    # MNSP_PEROFFER coverage at 2021-02 (the last clean MMS month) — same
    # story as BIDPEROFFER_D but without the 2024-09 recovery era.
    #
    # MNSP_DAYOFFER kept its column names across both boundaries, so the
    # standard bidding era set applies — same bidding-gap shape as BIDPEROFFER_D
    # / BIDDAYOFFER_D above (no 2022-01, recovery at 2024-09).
    "MNSP_PEROFFER": {"eras": ["2018-05", "2020-01", "2021-02"],                       "filter": {"LINKID": MNSP_LINKS}},
    "MNSP_DAYOFFER": {"eras": ["2018-05", "2020-01", "2021-05", "2024-09", "2025-01"], "filter": {"LINKID": MNSP_LINKS}},

    # Scrape-only tables — all live on AEMO's rolling current-data pages.
    "DAILY_REGION_SUMMARY":   {"eras": ["recent"], "filter": {"REGIONID": REGIONS}},
    "NEXT_DAY_DISPATCHLOAD":  {"eras": ["recent"], "filter": {"DUID": DUIDS}},
    "INTERMITTENT_GEN_SCADA": {"eras": ["recent"], "filter": {"DUID": DUIDS}},

    # Rooftop PV — introduced mid-2019.
    "ROOFTOP_PV_ACTUAL": {"eras": ["2020-01", "2021-05", "2022-01", "2024-08", "2025-01"], "filter": {"REGIONID": REGIONS}},

    # Effective-date config tables. These publish sparsely — records are
    # configuration changes, not time-series observations — and NEMOSIS's
    # "all" search_type would normally iterate every month from July 2009
    # onward. Tests narrow the scan by monkeypatching
    # `defaults.nem_data_model_start_time` so only a single era month is
    # probed. `keep_full_month: True` tells build.py to skip the usual
    # first-3 / last-2 day time trim, since a sparse table can easily end
    # up with no rows in that window. All eras pinned to 2021-05 — these
    # tables don't have format transitions that warrant multi-era coverage.
    "INTERCONNECTOR":               {"eras": ["2021-05"], "filter": {"INTERCONNECTORID": INTERCONNECTORS}, "keep_full_month": True},
    "MNSP_INTERCONNECTOR":          {"eras": ["2021-05"], "filter": {"LINKID": MNSP_LINKS},               "keep_full_month": True},
    "INTERCONNECTORCONSTRAINT":     {"eras": ["2021-05"], "filter": {"INTERCONNECTORID": INTERCONNECTORS}, "keep_full_month": True},
    "LOSSMODEL":                    {"eras": ["2021-05"], "filter": {"INTERCONNECTORID": INTERCONNECTORS}, "keep_full_month": True},
    "LOSSFACTORMODEL":              {"eras": ["2021-05"], "filter": {"INTERCONNECTORID": INTERCONNECTORS}, "keep_full_month": True},
    "DUDETAIL":                     {"eras": ["2021-05"], "filter": {"DUID": DUIDS},                       "keep_full_month": True},
    "DUDETAILSUMMARY":              {"eras": ["2021-05"], "filter": {"DUID": DUIDS},                       "keep_full_month": True},
    "PARTICIPANT":                  {"eras": ["2018-05", "2021-05"], "filter": {"PARTICIPANTID": PARTICIPANTS}, "keep_full_month": True},  # 2018 pair gives a wide LASTCHANGED span
    "GENCONDATA":                   {"eras": ["2021-05"], "filter": {"GENCONID": GENCON_IDS},              "keep_full_month": True},
    "SPDREGIONCONSTRAINT":          {"eras": ["2021-05"], "filter": {"REGIONID": REGIONS},                 "keep_full_month": True},
    "SPDINTERCONNECTORCONSTRAINT":  {"eras": ["2021-05"], "filter": {"INTERCONNECTORID": INTERCONNECTORS}, "keep_full_month": True},
    "SPDCONNECTIONPOINTCONSTRAINT": {"eras": ["2021-05"], "filter": {"GENCONID": SPDCPC_GENCON_IDS},       "keep_full_month": True},
    # MARKET_PRICE_THRESHOLDS has no group column (only 12 rows total) so no row filter is applied.
    "MARKET_PRICE_THRESHOLDS":      {"eras": ["2021-05"], "filter": {},                                    "keep_full_month": True},
}


# Static tables have no time dimension; each is downloaded once as current state.
# The build script snapshots them to the fixture tree.
STATIC_TABLES = [
    "ELEMENTS_FCAS_4_SECOND",
    "VARIABLES_FCAS_4_SECOND",
    "Generators and Scheduled Loads",
]
