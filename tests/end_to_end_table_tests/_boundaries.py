"""Boundary-test helpers for the end-to-end table tests.

A "boundary" here is the calendar boundary between two monthly archive
files — midnight on day 1 of an era month. NEMOSIS stores raw AEMO data
one file per month (mostly), so any query whose window touches a month
boundary has to stitch two files together. These helpers probe that stitch.

Two cases per (table, era), both anchored on midnight of day 1:

  at      [day1 00:00, day1 01:00]
          The window opens exactly on the boundary. Every returned row
          lives in the era-month file, but NEMOSIS must still reach back
          into the previous month's file (the uniform 1-day buffer-back)
          in case that file holds rows dated into the new month. Fails if
          the buffer-back doesn't fire when it's needed.

  before  [last-day 23:00, day1 00:00]
          The window closes exactly on the boundary; every returned row
          lives in the *previous* month's file. Fails if AEMO's
          previous-month file doesn't run right up to day1 00:00 — i.e. if
          a table's month overhang ever shifts from calendar-day to
          market-day partitioning — and confirms NEMOSIS never needs a
          *forward* buffer.

Between them the two cases cover every boundary failure mode we know of:
under-fetch (`at`), end-of-archive convention drift (`before`), and filter
off-by-one at both window edges (row count + first/last are checked on
both, so the lower edge is pinned by `at` and the upper edge by `before`).

Why only midnight, why only these two: an earlier version of this helper
probed a 9-cell matrix (3 window shapes x 3 times-of-day). Working through
the actual failure modes showed 7 of the 9 cells were re-testing plain
filter correctness — a property of the filter function, worth probing
once, not nine times. See `testing_and_maintenance.md` for the wider
test-suite design.

Public API (unchanged — consumers don't need editing):
  boundary_cases(table)       -> list[BoundaryCase]
  assert_boundary_shape(...)  -> None
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

_FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))
import spec  # noqa: E402


# Tables this helper does NOT cover. Why each is excluded:
#
#   BIDDAYOFFER_D, MNSP_DAYOFFER  — daily stride, market-day-keyed rows
#                                   that don't fit the per-interval shape.
#   BIDPEROFFER_D                 — bids are partitioned by trading day
#                                   (04:05 -> 04:00) not calendar day, so
#                                   the `at` window's row count needs a
#                                   trading-day-aware expectation. Covered
#                                   directly in test_bid_per_offer_d.py.
#   MNSP_PEROFFER                 — multi-dimensional rows (PERIODID etc.)
#                                   per (LINKID, SETTLEMENTDATE) plus the
#                                   schema-drift bug (issue #68).
_HELPER_DOES_NOT_COVER = {
    "BIDDAYOFFER_D",
    "MNSP_DAYOFFER",
    "BIDPEROFFER_D",
    "MNSP_PEROFFER",
}

FLAVOURS = ("at", "before")


# ===========================================================================
# BoundaryCase data model
# ===========================================================================

@dataclass(frozen=True)
class BoundaryCase:
    """One concrete (era x flavour) test case for a given table.

    `start`/`end`/`stride_minutes` are the literal query inputs; the
    `expected_*` fields are what the returned frame should look like *per
    entity* (per region, per DUID — whichever column the table filters on).
    """
    table: str
    era_key: str
    flavour: str

    start: datetime
    end: datetime
    stride_minutes: int

    expected_count: int
    expected_first: datetime
    expected_last: datetime

    @property
    def id(self) -> str:
        return f"{self.era_key}-{self.flavour}"

    @property
    def start_str(self) -> str:
        return self.start.strftime("%Y/%m/%d %H:%M:%S")

    @property
    def end_str(self) -> str:
        return self.end.strftime("%Y/%m/%d %H:%M:%S")


# ===========================================================================
# Stride per (table, era)
# ===========================================================================

# AEMO's 5-Minute Settlement reform went live on 2021-10-01 — at that point
# TRADINGPRICE & TRADINGINTERCONNECT switched from 30-min to 5-min rows.
_TRADING_5MIN_FROM = date(2021, 10, 1)


def stride_for(table: str, era_date: date) -> int:
    """Return the per-row stride in minutes for `table` in `era_date`'s era."""
    if table in ("TRADINGPRICE", "TRADINGINTERCONNECT"):
        return 5 if era_date >= _TRADING_5MIN_FROM else 30
    if table in ("TRADINGLOAD", "TRADINGREGIONSUM", "ROOFTOP_PV_ACTUAL"):
        return 30
    # All dispatch tables. BIDPEROFFER_D / MNSP_PEROFFER are skipped by the
    # matrix (see `_HELPER_DOES_NOT_COVER`) so are never passed here.
    return 5


# ===========================================================================
# Public API
# ===========================================================================

def boundary_cases(table: str) -> list[BoundaryCase]:
    """Generate the `at` and `before` case for every era listed in
    `spec.DYNAMIC_TABLES[table]["eras"]`.

    Returns an empty list for tables in `_HELPER_DOES_NOT_COVER`. The
    `recent` scrape era is skipped — it has no monthly file boundary.
    """
    if table in _HELPER_DOES_NOT_COVER:
        return []

    cases = []
    for era_key in spec.DYNAMIC_TABLES[table]["eras"]:
        if era_key == "recent":
            continue
        era_date = spec.ERAS[era_key]
        stride = stride_for(table, era_date)
        for flavour in FLAVOURS:
            cases.append(_build_case(table, era_key, era_date, stride, flavour))
    return cases


def assert_boundary_shape(
    data: pd.DataFrame,
    case: BoundaryCase,
    *,
    date_col: str,
    entities_col: str,
    expected_entities: set,
) -> None:
    """Assert `data` matches what `case` predicts, per entity."""
    # A datetime column coming back as object/string is a silent-failure
    # risk: == and sorted() happen to work on AEMO's date strings, so a
    # dtype regression wouldn't otherwise surface. Guarded on non-empty
    # because an empty frame's selected column can legitimately be object.
    if not data.empty:
        assert data[date_col].dtype == "datetime64[ns]", (
            f"{case.id}: {date_col} dtype is {data[date_col].dtype}, "
            f"expected datetime64[ns]"
        )

    actual_entities = set(data[entities_col])
    assert actual_entities == expected_entities, (
        f"{case.id}: entity set {actual_entities!r} != {expected_entities!r}"
    )

    expected_stride = timedelta(minutes=case.stride_minutes)
    for entity in sorted(expected_entities):
        rows = data[data[entities_col] == entity].sort_values(date_col)
        timestamps = rows[date_col].tolist()

        assert len(timestamps) == case.expected_count, (
            f"{case.id} [{entity}]: got {len(timestamps)} rows, "
            f"expected {case.expected_count}"
        )
        assert timestamps[0] == case.expected_first, (
            f"{case.id} [{entity}]: first {timestamps[0]} != {case.expected_first}"
        )
        assert timestamps[-1] == case.expected_last, (
            f"{case.id} [{entity}]: last {timestamps[-1]} != {case.expected_last}"
        )
        assert len(set(timestamps)) == len(timestamps), (
            f"{case.id} [{entity}]: duplicate timestamps in returned frame"
        )
        diffs = {b - a for a, b in zip(timestamps, timestamps[1:])}
        assert diffs == {expected_stride}, (
            f"{case.id} [{entity}]: irregular stride {diffs}, "
            f"expected {{{expected_stride}}}"
        )


# ===========================================================================
# Case builder
# ===========================================================================

def _build_case(
    table: str, era_key: str, era_date: date, stride: int, flavour: str,
) -> BoundaryCase:
    """Build one BoundaryCase. Both flavours are 1-hour windows touching
    midnight of day 1; period-ending convention means the first returned
    row sits one stride after `start` and the last sits exactly on `end`."""
    midnight = datetime.combine(era_date, time(0, 0))
    if flavour == "at":
        start, end = midnight, midnight + timedelta(hours=1)
    elif flavour == "before":
        start, end = midnight - timedelta(hours=1), midnight
    else:
        raise ValueError(f"unknown flavour {flavour!r}")

    return BoundaryCase(
        table=table, era_key=era_key, flavour=flavour,
        start=start, end=end, stride_minutes=stride,
        expected_count=60 // stride,
        expected_first=start + timedelta(minutes=stride),
        expected_last=end,
    )
