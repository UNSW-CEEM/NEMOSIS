"""Boundary-test helpers — readable rewrite of `_boundaries.py`.

Same external API: `boundary_cases(table)` returns a list of `BoundaryCase`,
`assert_boundary_shape(...)` checks a returned frame against a case.

This file is laid out top-down: the two public entry points come first,
followed by helpers in roughly the order they're called.

----------------------------------------------------------------------------
Vocabulary used throughout this file:

  era       A single calendar month chosen as a test anchor — listed in
            `spec.ERAS`. Picked to straddle known AEMO format transitions
            (e.g. 2021-05 is the first 5-min-NEM month) or to sit on a
            year boundary (2020-01, 2022-01, 2025-01) for the year-wrap
            coverage in this matrix. Each era covered by a table
            contributes its own (flavour × T) sub-matrix.

  T         A time-of-day on day 1 of the era month. The matrix probes
            three values per table — see "Three time-of-day probe points"
            below. Used to anchor each flavour's window.

  stride    The per-row interval, in minutes, of the table at the era in
            question. 5 for dispatch tables, 30 for trading tables before
            2021-10 and for ROOFTOP_PV_ACTUAL throughout. `stride_for(table,
            era_date)` returns the right value, including the 30→5 switch
            TRADINGPRICE/TRADINGINTERCONNECT made on 2021-10-01.

  boundary  The calendar boundary between (era_month − 1) and era_month —
            i.e. midnight on day 1 of the era. At a January era this IS
            the year boundary, which is how Dec→Jan stitch and the date-
            generator's `month == 1` buffer-wrap branch get exercised
            without a separate flavour family.

  flavour   One of three window shapes (`at`, `before`, `into`) — see
            below for the full definition of each.

----------------------------------------------------------------------------
Three flavours, anchored on T on day 1 of the era month:

  - at      [day1@T, day1@T + 1h]
              Forward 1h from T. At T == 00:00 this exercises NEMOSIS's
              buffer-back branch in `year_and_month_gen` (and buffer-wrap to
              December of the prior year at January eras). At T == 04:00 /
              04:05 it's an intra-day stride/clip check on day 1.

  - before  [day1@T - 1h, day1@T]
              Backward 1h ending at T. Mirror of `at`. At T == 00:00 the
              window lands entirely in the previous month (1h before midnight
              IS last-day@23:00) and ends exactly at the boundary. At higher
              T values the window is intra-day on day 1.

  - into    [last-day@23:00, day1@T]
              Variable-length window from the last hour of the previous day
              into day 1 of the era month. The start is anchored on the
              previous day regardless of T, so for T > 00:00 the window
              genuinely straddles the boundary and ends at / just past the
              market-day-end on day 1.

Three time-of-day probe points: 00:00, 04:00, and 04:05 for 5-min-stride
tables (or 04:30 for 30-min tables, since :05 isn't a valid stride point).

----------------------------------------------------------------------------
Internally this version trades brevity for explicitness:
  * `boundary_cases` uses `itertools.product` so the matrix walk is one
    flat loop instead of three nested ones.
  * Each flavour has its own builder function that computes window and
    expected values inline with plain arithmetic — no shared `pd.date_range`
    helper to context-switch into. The per-stride math is duplicated three
    times (once per flavour) on purpose; each instance is two lines.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from itertools import product
from pathlib import Path

import pandas as pd

_FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))
import spec  # noqa: E402


# ===========================================================================
# Public API
# ===========================================================================

def boundary_cases(table: str) -> list[BoundaryCase]:
    """Generate every (era × flavour × T) case for `table`, based on the
    eras listed in `spec.DYNAMIC_TABLES[table]["eras"]`.

    Skipped:
      * the `recent` scrape era (handled by separate scrape-pattern tests)
      * tables in `_HELPER_DOES_NOT_COVER` (return empty list)
      * `flavour=into, T=00:00` cases (identical to `flavour=before, T=00:00`)
    """
    if table in _HELPER_DOES_NOT_COVER:
        return []

    # Pre-compute the (era_key, era_date, stride, T) axis for this table.
    # This is the only place where stride/era logic lives. Everything below
    # this point treats it as opaque coordinates.
    matrix = []
    for era_key in spec.DYNAMIC_TABLES[table]["eras"]:
        if era_key == "recent":
            continue
        era_date = spec.ERAS[era_key]
        stride = stride_for(table, era_date)
        for T in time_points_for_stride(stride):
            matrix.append((era_key, era_date, stride, T))

    # Cartesian product flattens the (era × T) axis against flavours, giving
    # one explicit triple per case in a single flat loop. Per-flavour dispatch
    # is an inline if-chain so the reader doesn't have to chase a dict lookup.
    cases = []
    for (era_key, era_date, stride, T), flavour in product(matrix, FLAVOURS):
        if flavour == "into" and T == time(0, 0):
            continue   # duplicate of `before` at T=00:00

        if flavour == "at":
            case = _build_at_case(table, era_key, era_date, stride, T)
        elif flavour == "before":
            case = _build_before_case(table, era_key, era_date, stride, T)
        else:  # "into"
            case = _build_into_case(table, era_key, era_date, stride, T)

        cases.append(case)
    return cases


def assert_boundary_shape(
    data: pd.DataFrame,
    case: BoundaryCase,
    *,
    date_col: str,
    entities_col: str,
    expected_entities: set,
) -> None:
    """Assert `data` matches what `case` predicts. Reads top-down as a
    checklist of properties the returned frame must have."""
    # Dtype checks — guard against a regression where the datetime column
    # comes back as object/string. `sorted()` and `==` on the timestamp
    # list below happen to work on strings for AEMO's date format, so
    # downstream breakage would be silent without this.
    if not data.empty:
        assert data[date_col].dtype == "datetime64[ns]", (
            f"{case.id}: {date_col} dtype is {data[date_col].dtype}, "
            f"expected datetime64[ns]"
        )
        assert data[entities_col].dtype == "object", (
            f"{case.id}: {entities_col} dtype is {data[entities_col].dtype}, "
            f"expected object"
        )

    _assert_entity_set_matches(data, case, entities_col, expected_entities)

    for entity in sorted(expected_entities):
        per_entity = data[data[entities_col] == entity].sort_values(date_col)
        timestamps = per_entity[date_col].tolist()

        _assert_row_count(timestamps, case, entity)
        if case.expected_count == 0:
            continue
        _assert_first_and_last_timestamps(timestamps, case, entity)
        _assert_no_duplicate_timestamps(timestamps, case, entity)
        _assert_stride_is_regular(timestamps, case, entity)


# ===========================================================================
# Configuration constants
# ===========================================================================

FLAVOURS = ("at", "before", "into")


# Tables this helper does NOT cover. Why each is excluded:
#
#   BIDDAYOFFER_D, MNSP_DAYOFFER  — daily stride, market-day-keyed rows
#                                   that don't fit the per-interval shape.
#   BIDPEROFFER_D                 — bids are partitioned by trading day
#                                   (04:05 → 04:00) not calendar day, so
#                                   a query in calendar 00:00–04:00 lands
#                                   in the previous trading-day archive
#                                   and the helper's row-count assumptions
#                                   don't hold.
#   MNSP_PEROFFER                 — multi-dimensional rows (PERIODID etc.)
#                                   per (LINKID, SETTLEMENTDATE) plus the
#                                   schema-drift bug (issue #68) caps it
#                                   at low coverage anyway.
#
# DISPATCHCONSTRAINT was previously excluded on the theory that a
# constraint may not bind every interval. Probing every era fixture
# showed the pinned CONSTRAINTID `DATASNAP_DFS_Q_CLST` binds all 288
# intervals on day 1 across 2018–2025, so the matrix shape assumptions
# DO hold for this specific fixture.
_HELPER_DOES_NOT_COVER = {
    "BIDDAYOFFER_D",
    "MNSP_DAYOFFER",
    "BIDPEROFFER_D",
    "MNSP_PEROFFER",
}


# ===========================================================================
# BoundaryCase data model
# ===========================================================================

@dataclass(frozen=True)
class BoundaryCase:
    """One concrete (era × flavour × T) test case for a given table.

    Fields fall into three groups:
      * inputs   — which slot in the matrix this case represents
      * query    — the literal datetimes & stride passed to NEMOSIS
      * expected — what the returned frame should look like *per entity*
                   (per region, per DUID, etc — whichever column the table
                    is filtered on)
    """
    # --- inputs ---
    table: str
    era_key: str
    era_date: date
    flavour: str
    time_point: time

    # --- query ---
    start: datetime
    end: datetime
    stride_minutes: int

    # --- expected per-entity frame ---
    expected_count: int
    expected_first: datetime
    expected_last: datetime

    @property
    def id(self) -> str:
        return f"{self.era_key}-T{self.time_point.strftime('%H%M')}-{self.flavour}"

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
    if table in ("TRADINGLOAD", "TRADINGREGIONSUM"):
        return 30
    if table == "ROOFTOP_PV_ACTUAL":
        return 30
    # All dispatch tables — BIDPEROFFER_D / MNSP_PEROFFER are skipped by the
    # matrix (see `_HELPER_DOES_NOT_COVER`) so `stride_for` is never called
    # for them.
    return 5


def time_points_for_stride(stride: int) -> list[time]:
    """Three probe times per day. Midnight, market-day-end (04:00), and just
    past it. The post-end probe is 04:05 for 5-min strides; for 30-min
    tables we use 04:30 since :05 isn't a valid stride point there.

    All returned T values must be stride-aligned — the flavour builders rely
    on `window_minutes // stride` producing an exact count, which only holds
    when the window endpoints sit on stride boundaries. Enforced here so a
    future contributor adding an off-stride T value fails loud, not silent.
    """
    if stride == 30:
        points = [time(0, 0), time(4, 0), time(4, 30)]
    else:
        points = [time(0, 0), time(4, 0), time(4, 5)]
    for T in points:
        assert (T.hour * 60 + T.minute) % stride == 0, (
            f"time point {T} not aligned to {stride}-min stride"
        )
    return points


# ===========================================================================
# Per-flavour case builders
# ===========================================================================
# Each function builds one BoundaryCase from the matrix coordinates, computing
# the window and expected first/last/count inline. Period-ending convention:
# the first returned row sits at start + stride minutes (the first stride
# boundary strictly after start); the last sits at end (which always lands
# on a stride boundary in our matrix).

def _build_at_case(
    table: str, era_key: str, era_date: date, stride: int, T: time,
) -> BoundaryCase:
    """`at` flavour: 1h window starting at T on day 1 of the era month."""
    start = datetime.combine(era_date, T)
    end = start + timedelta(hours=1)
    return BoundaryCase(
        table=table, era_key=era_key, era_date=era_date,
        flavour="at", time_point=T,
        start=start, end=end, stride_minutes=stride,
        expected_count=60 // stride,
        expected_first=start + timedelta(minutes=stride),
        expected_last=end,
    )


def _build_before_case(
    table: str, era_key: str, era_date: date, stride: int, T: time,
) -> BoundaryCase:
    """`before` flavour: 1h window ending at T on day 1 of the era month."""
    end = datetime.combine(era_date, T)
    start = end - timedelta(hours=1)
    return BoundaryCase(
        table=table, era_key=era_key, era_date=era_date,
        flavour="before", time_point=T,
        start=start, end=end, stride_minutes=stride,
        expected_count=60 // stride,
        expected_first=start + timedelta(minutes=stride),
        expected_last=end,
    )


def _build_into_case(
    table: str, era_key: str, era_date: date, stride: int, T: time,
) -> BoundaryCase:
    """`into` flavour: window from last-day@23:00 of previous month to T on
    day 1 of the era month. Window length varies with T:
      T = 00:00  →  1h        (= `before` at T=00:00; deduplicated upstream)
      T = 04:00  →  5h
      T = 04:05  →  5h 5min   (5-min-stride tables)
      T = 04:30  →  5h 30min  (30-min-stride tables)
    """
    start = datetime.combine(era_date, time(0, 0)) - timedelta(hours=1)  # last-day@23:00
    end = datetime.combine(era_date, T)
    window_minutes = int((end - start).total_seconds() // 60)
    return BoundaryCase(
        table=table, era_key=era_key, era_date=era_date,
        flavour="into", time_point=T,
        start=start, end=end, stride_minutes=stride,
        expected_count=window_minutes // stride,
        expected_first=start + timedelta(minutes=stride),
        expected_last=end,
    )


# ===========================================================================
# Assertion sub-checks
# ===========================================================================

def _assert_entity_set_matches(data, case, entities_col, expected):
    actual = set(data[entities_col])
    assert actual == expected, (
        f"{case.id}: entity set {actual!r} != expected {expected!r}"
    )


def _assert_row_count(timestamps, case, entity):
    assert len(timestamps) == case.expected_count, (
        f"{case.id} [{entity}]: got {len(timestamps)} rows, "
        f"expected {case.expected_count}"
    )


def _assert_first_and_last_timestamps(timestamps, case, entity):
    assert timestamps[0] == case.expected_first, (
        f"{case.id} [{entity}]: first {timestamps[0]} != "
        f"expected {case.expected_first}"
    )
    assert timestamps[-1] == case.expected_last, (
        f"{case.id} [{entity}]: last {timestamps[-1]} != "
        f"expected {case.expected_last}"
    )


def _assert_no_duplicate_timestamps(timestamps, case, entity):
    assert len(set(timestamps)) == len(timestamps), (
        f"{case.id} [{entity}]: duplicate timestamps in returned frame"
    )


def _assert_stride_is_regular(timestamps, case, entity):
    """Every consecutive pair of timestamps should differ by exactly
    `case.stride_minutes` — no gaps, no irregular spacing. A case with
    fewer than 2 timestamps can't probe stride, so treat it as a
    matrix-construction error rather than silently passing."""
    assert case.expected_count >= 2, (
        f"{case.id}: stride regularity needs >=2 rows, got expected_count="
        f"{case.expected_count} — case builder is dropping a stride point"
    )
    consecutive_diffs = {b - a for a, b in zip(timestamps, timestamps[1:])}
    expected = timedelta(minutes=case.stride_minutes)
    assert consecutive_diffs == {expected}, (
        f"{case.id} [{entity}]: irregular stride {consecutive_diffs}, "
        f"expected single {expected}"
    )
