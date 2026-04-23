"""Offline validation of `processing_info_maps.search_type` against AEMO's
actual storage patterns.

For each table classified in `search_type`, the classification determines how
NEMOSIS iterates monthly archives and whether it deduplicates on read:

  start_to_end  each monthly archive covers its own month's rows only —
                no overlap between months (with narrow exceptions where
                AEMO publishes a day or two into the next month).
                Library concatenates directly.
  all           each monthly archive is a cumulative snapshot (for true
                registration tables like PARTICIPANT) or a change-log
                (for constraint tables like GENCONDATA). In either case
                the library deduplicates via `drop_duplicates_by_primary_key`.
  end           the single end-time archive is cumulative back to table
                origin, so only one fetch is needed.

This file reads raw AEMO CSVs from the fixture zips directly (no
`dynamic_data_compiler`, no HTTP server) and checks AEMO's stored data
actually behaves the way each classification assumes. A mis-classified table
or a wrong `table_primary_keys` entry surfaces here even when no per-table
test covers the affected table.

Scrape-pattern tables (DAILY_REGION_SUMMARY, NEXT_DAY_DISPATCHLOAD,
INTERMITTENT_GEN_SCADA) are excluded — their per-day file layout doesn't
fit the cross-month semantics probed here, and their classification is
exercised by their per-table end-to-end tests.
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from nemosis import defaults
from nemosis.processing_info_maps import search_type
from nemosis.query_wrappers import drop_duplicates_by_primary_key


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "data"
MMS_DATA_DIR = (
    "Data_Archive/Wholesale_Electricity/MMSDM/{year}/"
    "MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA"
)

_SCRAPE_TABLES = {"DAILY_REGION_SUMMARY", "NEXT_DAY_DISPATCHLOAD", "INTERMITTENT_GEN_SCADA"}
_PARKED = {"FCAS_4_SECOND"}   # upstream AEMO outage — issue #64
_NON_MMS = _SCRAPE_TABLES | _PARKED

_TIME_COLS = {"SETTLEMENTDATE", "INTERVAL_DATETIME", "TIMESTAMP", "RUN_DATETIME"}

# `start_to_end` tables whose AEMO archives genuinely overlap at month
# boundaries (the published archive for month N extends a day or two into
# month N+1). Excluded from the disjointness check — this is AEMO's format,
# not a classification bug. NEMOSIS handles the overlap via the SETTLEMENTDATE
# range filter applied after fetch.
_ARCHIVES_OVERLAP_INTO_NEXT_MONTH = {"ROOFTOP_PV_ACTUAL"}


# ---------------------------------------------------------------------------
# Fixture zip helpers
# ---------------------------------------------------------------------------

def _mms_zip_path(table: str, year: int, month: int) -> Path | None:
    """Find an MMS fixture zip for `table` at `year-month`. Tries both the
    pre-2024-08 `PUBLIC_DVD_` filename and the post-cutover `PUBLIC_ARCHIVE#`
    one. Returns None if neither exists."""
    base = FIXTURE_ROOT / MMS_DATA_DIR.format(year=year, month=month)
    for name in (
        f"PUBLIC_DVD_{table}_{year}{month:02d}010000.zip",
        f"PUBLIC_ARCHIVE#{table}#FILE01#{year}{month:02d}010000.zip",
    ):
        if (base / name).exists():
            return base / name
    return None


def _read_mms_zip(path: Path) -> pd.DataFrame:
    """Extract the single CSV from an MMS archive zip and return just the
    D-row content as a DataFrame. AEMO CSVs have `C,...` header metadata,
    one `I,...` row with column names, and many `D,...` data rows. The
    first 4 fields of I/D rows are structural (row-type, market section,
    table name, version) — this helper strips them so the frame has only
    the actual table columns."""
    with zipfile.ZipFile(path) as z:
        csv_name = next(n for n in z.namelist() if n.endswith(".CSV"))
        content = z.read(csv_name).decode()

    lines = content.splitlines()
    header = next(l for l in lines if l.startswith("I,"))
    data_rows = [l for l in lines if l.startswith("D,")]
    csv_body = header + "\n" + "\n".join(data_rows)
    df = pd.read_csv(io.StringIO(csv_body))
    return df.iloc[:, 4:]


def _fixtured_months(table: str) -> list[tuple[int, int]]:
    """Every (year, month) pair for which a fixture zip exists for `table`,
    sorted chronologically. Used to pick consecutive-month pairs for
    storage-pattern checks."""
    months: list[tuple[int, int]] = []
    mms_root = FIXTURE_ROOT / "Data_Archive/Wholesale_Electricity/MMSDM"
    if not mms_root.exists():
        return months
    for year_dir in sorted(mms_root.iterdir()):
        for month_dir in sorted(year_dir.iterdir()):
            parts = month_dir.name.split("_")
            if len(parts) != 3 or parts[0] != "MMSDM":
                continue
            try:
                year, month = int(parts[1]), int(parts[2])
            except ValueError:
                continue
            if _mms_zip_path(table, year, month) is not None:
                months.append((year, month))
    return months


def _consecutive_pair(months: list[tuple[int, int]]) -> tuple | None:
    """Pick the first consecutive-month pair in `months`, or None if
    none of the fixtured months are adjacent."""
    for (y1, m1), (y2, m2) in zip(months, months[1:]):
        if (y2, m2) == ((y1 + 1, 1) if m1 == 12 else (y1, m1 + 1)):
            return (y1, m1), (y2, m2)
    return None


def _shared_pk_columns(table: str, *dfs: pd.DataFrame) -> list[str]:
    """PK columns present in every supplied frame. Drops any PK column
    that's missing from older archives (handles AEMO schema drift like
    BIDPEROFFER_D's `DIRECTION`, added mid-history)."""
    full_pk = defaults.table_primary_keys[table]
    return [c for c in full_pk if all(c in df.columns for df in dfs)]


# ---------------------------------------------------------------------------
# Table groupings by classification
# ---------------------------------------------------------------------------

_START_TO_END_TABLES = sorted(
    t for t, st in search_type.items()
    if st == "start_to_end" and t not in _NON_MMS
)
_ALL_TABLES = sorted(t for t, st in search_type.items() if st == "all")
_END_TABLES = sorted(t for t, st in search_type.items() if st == "end")


# ---------------------------------------------------------------------------
# Metadata invariants — classification must be consistent with schema.
# Pure unit tests: no fixture needed, just library config.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", _START_TO_END_TABLES)
def test_start_to_end_table_has_a_time_column(table):
    """`start_to_end` implies each row has a unique timestamp, so the table
    must have a SETTLEMENTDATE/INTERVAL_DATETIME/TIMESTAMP/RUN_DATETIME
    column. Missing it suggests misclassification (should probably be
    `all` instead)."""
    cols = set(defaults.table_columns[table])
    assert cols & _TIME_COLS, (
        f"{table} classified as start_to_end but has no time column "
        f"(looked for {_TIME_COLS})"
    )


@pytest.mark.parametrize("table", _ALL_TABLES)
def test_all_table_has_no_time_column(table):
    """`all` tables are effective-date config/change-log tables, not time
    series. A time column here would suggest the table should probably
    be classified as `start_to_end`."""
    cols = set(defaults.table_columns[table])
    leaked = cols & _TIME_COLS
    assert not leaked, (
        f"{table} classified as 'all' but has time column(s) {leaked} — "
        f"may be misclassified (should likely be start_to_end)"
    )


# ---------------------------------------------------------------------------
# Storage-pattern validation — read raw archives and check AEMO's actual
# behaviour matches what the classification assumes.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", _START_TO_END_TABLES)
def test_start_to_end_archives_are_disjoint_across_months(table):
    """`start_to_end` tables should have no overlapping rows between
    consecutive monthly archives — each row lands in exactly one month's
    file, so concatenation gives zero PK duplicates."""
    if table in _ARCHIVES_OVERLAP_INTO_NEXT_MONTH:
        pytest.skip(f"{table}: AEMO archives legitimately overlap at month boundary")
    months = _fixtured_months(table)
    pair = _consecutive_pair(months)
    if pair is None:
        pytest.skip(
            f"{table}: fixture lacks a consecutive-month pair "
            f"(have {[f'{y}-{m:02d}' for y, m in months]})"
        )
    (y1, m1), (y2, m2) = pair
    df1 = _read_mms_zip(_mms_zip_path(table, y1, m1))
    df2 = _read_mms_zip(_mms_zip_path(table, y2, m2))
    pk = _shared_pk_columns(table, df1, df2)
    assert pk, f"{table}: no PK columns shared between {y1}-{m1:02d} and {y2}-{m2:02d} frames"

    combined = pd.concat([df1, df2])
    dupes = combined.duplicated(pk).sum()
    assert dupes == 0, (
        f"{table}: concat of {y1}-{m1:02d} and {y2}-{m2:02d} has "
        f"{dupes} PK duplicates on {pk} — either archives aren't "
        f"disjoint (should be 'all'?) or the primary-key config is wrong"
    )


@pytest.mark.parametrize("table", _ALL_TABLES)
def test_all_dedup_collapses_cleanly(table):
    """The core invariant for `all`-type tables: after fetching multiple
    monthly archives and running `drop_duplicates_by_primary_key`, the
    result must have no PK duplicates. This catches wrong `table_primary_keys`
    configs (where the PK isn't actually unique) and classification
    mismatches (where the dedup step fails to converge). Covers both
    true snapshot tables like PARTICIPANT (where archives overlap heavily)
    and change-log tables like GENCONDATA (where archives don't overlap
    much but dedup still needs to produce clean output)."""
    months = _fixtured_months(table)
    if len(months) < 2:
        pytest.skip(f"{table}: need 2 fixtured months")
    (y1, m1), (y2, m2) = months[0], months[-1]
    df1 = _read_mms_zip(_mms_zip_path(table, y1, m1))
    df2 = _read_mms_zip(_mms_zip_path(table, y2, m2))
    combined = pd.concat([df1, df2])
    pk = _shared_pk_columns(table, df1, df2)
    assert pk, f"{table}: no PK columns shared between {y1}-{m1:02d} and {y2}-{m2:02d} frames"

    start_time = pd.Timestamp(f"{y1}-{m1:02d}-01")
    deduped = drop_duplicates_by_primary_key(combined, start_time, table)
    assert not deduped.duplicated(pk).any(), (
        f"{table}: drop_duplicates_by_primary_key left PK duplicates on {pk}"
    )


def test_participant_all_archives_do_overlap():
    """Canary for the `all`-type snapshot pattern. PARTICIPANT is a true
    registration list — every registered participant appears in every
    monthly archive with identical content. If concat shows zero PK
    overlap across two distant months, either AEMO stopped publishing
    snapshots or the PK config is wrong. We probe PARTICIPANT specifically
    because other `all` tables are change-logs that don't overlap much
    over a 2-month window."""
    df1 = _read_mms_zip(_mms_zip_path("PARTICIPANT", 2018, 5))
    df2 = _read_mms_zip(_mms_zip_path("PARTICIPANT", 2021, 5))
    pk = _shared_pk_columns("PARTICIPANT", df1, df2)
    combined = pd.concat([df1, df2])
    dupes = combined.duplicated(pk).sum()
    assert dupes > 0, (
        f"PARTICIPANT: concat of 2018-05 and 2021-05 has 0 PK duplicates on "
        f"{pk} — snapshot pattern no longer holds, library's `all` assumption "
        f"is wrong, or the PK definition is off"
    )


@pytest.mark.parametrize("table", _END_TABLES)
def test_end_archive_contains_historical_rows(table):
    """`end` tables are classified such that only the end-time archive is
    fetched. That only works if each archive is cumulative back to the
    table's origin. Probe: the 2021-05 archive should contain rows with
    effective dates well earlier than the archive month itself."""
    zip_path = _mms_zip_path(table, 2021, 5)
    if zip_path is None:
        pytest.skip(f"{table}: no 2021-05 fixture")
    df = _read_mms_zip(zip_path)
    date_col = defaults.primary_date_columns[table]
    df[date_col] = pd.to_datetime(df[date_col], format="%Y/%m/%d %H:%M:%S")
    min_date = df[date_col].min()
    assert min_date < pd.Timestamp("2020-01-01"), (
        f"{table} 2021-05 archive min {date_col}={min_date} — expected "
        f"historical rows from well before 2020 (cumulative semantics)"
    )