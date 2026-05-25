"""Boundary-test helpers shared across per-table test files.

Generates a parametrised matrix of (era × flavour × time-of-day) cases for
each dynamic time-series table, and provides an assertion helper that
checks shape, count, first/last timestamps, and absence of gaps or
duplicates across the boundary.

Three flavours, each anchored on T (a time-of-day) on day 1 of the era month:

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

Time-of-day axis: three probe points. Calendar midnight (00:00), market-day
end (04:00), and just past market-day end (04:05 for 5-min-stride tables;
04:30 for 30-min-stride tables since :05 isn't a valid stride point there).
Daily-stride bid tables (BIDDAYOFFER_D, MNSP_DAYOFFER) aren't handled here
— they have different row-count semantics and live in their own per-table
tests.

Year-wrap coverage falls out for free: the boundary at a January era IS the
year boundary, so flavours at eras 2020-01, 2022-01, and 2025-01 exercise
the Dec→Jan stitch and the date-generator's `month == 1` buffer-wrap branch
without needing a separate flavour family.

----------------------------------------------------------------------------
Worked example — what `boundary_cases("DISPATCHPRICE")` generates at the
2020-01 era (one of six eras for that table):

    flavour=at      T=00:00  query [2020-01-01 00:00, 2020-01-01 01:00]
                                   12 rows; buffer-back triggered → fetches Dec 2019
    flavour=at      T=04:00  query [2020-01-01 04:00, 2020-01-01 05:00]
                                   12 rows; intra-day on Jan 1
    flavour=at      T=04:05  query [2020-01-01 04:05, 2020-01-01 05:05]
                                   12 rows; intra-day, off-stride start
    flavour=before  T=00:00  query [2019-12-31 23:00, 2020-01-01 00:00]
                                   12 rows; entirely Dec, ends at boundary
    flavour=before  T=04:00  query [2020-01-01 03:00, 2020-01-01 04:00]
                                   12 rows; intra-day on Jan 1
    flavour=before  T=04:05  query [2020-01-01 03:05, 2020-01-01 04:05]
                                   12 rows; intra-day, off-stride start
    flavour=into    T=04:00  query [2019-12-31 23:00, 2020-01-01 04:00]
                                   60 rows; straddles year boundary
    flavour=into    T=04:05  query [2019-12-31 23:00, 2020-01-01 04:05]
                                   61 rows; straddles year boundary

  (`into` at T=00:00 is identical to `before` at T=00:00 — skipped.)

  8 cases per era × 6 eras = 48 cases for DISPATCHPRICE.
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


# ===========================================================================
# Stride per (table, era)
# ===========================================================================

# TRADINGPRICE & TRADINGINTERCONNECT switched 30-min → 5-min when AEMO's
# 5-Minute Settlement reform went live on 2021-10-01.
_TRADING_5MIN_FROM = date(2021, 10, 1)


def stride_for(table: str, era_date: date) -> int:
    """Return the per-row stride in minutes for `table` in `era_date`'s era."""
    if table in ("TRADINGPRICE", "TRADINGINTERCONNECT"):
        return 5 if era_date >= _TRADING_5MIN_FROM else 30
    if table in ("TRADINGLOAD", "TRADINGREGIONSUM"):
        return 30
    if table == "ROOFTOP_PV_ACTUAL":
        return 30
    # All dispatch tables, BIDPEROFFER_D, MNSP_PEROFFER.
    return 5


# Tables this helper does NOT cover (boundary tests live in the per-table
# files instead, or are skipped entirely):
#
#   BIDDAYOFFER_D, MNSP_DAYOFFER  — daily stride, market-day-keyed rows
#                                   that don't fit the per-interval shape
#   BIDPEROFFER_D                 — bids are partitioned by trading day
#                                   (04:05 → 04:00) not calendar day, so
#                                   a query in calendar 00:00–04:00 lands
#                                   in the previous trading-day archive
#                                   and the helper's row-count assumptions
#                                   don't hold
#   MNSP_PEROFFER                 — multi-dimensional rows (PERIODID etc.)
#                                   per (LINKID, SETTLEMENTDATE) plus the
#                                   schema-drift bug (issue #68) caps it
#                                   at low coverage anyway
#   DISPATCHCONSTRAINT            — a constraint isn't binding every
#                                   interval, so per-interval row counts
#                                   are non-deterministic
_HELPER_DOES_NOT_COVER = {
    "BIDDAYOFFER_D",
    "MNSP_DAYOFFER",
    "BIDPEROFFER_D",
    "MNSP_PEROFFER",
    "DISPATCHCONSTRAINT",
}


def _time_points_for_stride(stride: int) -> list[time]:
    """Three probe times per day. Midnight, market-day-end (04:00), and just
    past it. The post-end probe is 04:05 for 5-min strides; for 30-min
    tables we use 04:30 since :05 isn't a valid stride point there."""
    if stride == 30:
        return [time(0, 0), time(4, 0), time(4, 30)]
    return [time(0, 0), time(4, 0), time(4, 5)]


# ===========================================================================
# Window construction — one explicit function per flavour
# ===========================================================================

def _at_window(era_date: date, T: time) -> tuple[datetime, datetime]:
    """`at` flavour: forward 1h from T on day 1 of the era month."""
    anchor = datetime.combine(era_date, T)
    return anchor, anchor + timedelta(hours=1)


def _before_window(era_date: date, T: time) -> tuple[datetime, datetime]:
    """`before` flavour: backward 1h ending at T on day 1 of the era month."""
    anchor = datetime.combine(era_date, T)
    return anchor - timedelta(hours=1), anchor


def _into_window(era_date: date, T: time) -> tuple[datetime, datetime]:
    """`into` flavour: from last-day@23:00 to T on day 1 of the era month."""
    last_day_23 = datetime.combine(era_date, time(0, 0)) - timedelta(hours=1)
    anchor = datetime.combine(era_date, T)
    return last_day_23, anchor


_FLAVOUR_BUILDERS = {
    "at":     _at_window,
    "before": _before_window,
    "into":   _into_window,
}


# ===========================================================================
# Case generation
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
    flavour: str               # "at" | "before" | "into"
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


def boundary_cases(table: str) -> list[BoundaryCase]:
    """Generate every (era × flavour × T) case for `table`, based on the
    eras listed in `spec.DYNAMIC_TABLES[table]["eras"]`.

    Skipped:
      * the `recent` scrape era (handled by separate scrape-pattern tests)
      * daily-stride bid tables (see `_DAILY_TABLES`)
      * `flavour=into, T=00:00` cases (identical to `flavour=before, T=00:00`)
    """
    if table in _HELPER_DOES_NOT_COVER:
        return []

    eras = spec.DYNAMIC_TABLES[table]["eras"]
    cases: list[BoundaryCase] = []

    for era_key in eras:
        if era_key == "recent":
            continue
        era_date = spec.ERAS[era_key]
        stride = stride_for(table, era_date)

        for T in _time_points_for_stride(stride):
            for flavour, build_window in _FLAVOUR_BUILDERS.items():
                if _is_duplicate_into_at_midnight(flavour, T):
                    continue
                start, end = build_window(era_date, T)
                first, last, n = _expected_period_endings(start, end, stride)
                cases.append(BoundaryCase(
                    table=table,
                    era_key=era_key,
                    era_date=era_date,
                    flavour=flavour,
                    time_point=T,
                    start=start,
                    end=end,
                    stride_minutes=stride,
                    expected_count=n,
                    expected_first=first,
                    expected_last=last,
                ))
    return cases


def _is_duplicate_into_at_midnight(flavour: str, T: time) -> bool:
    """At T == 00:00, `into` and `before` produce the same window
    ([last-day@23:00, day1@00:00]) — `into`'s start (last-day@23:00) IS
    `before`'s start (anchor − 1h, where anchor = midnight). Skip the
    duplicate so the matrix doesn't double-count it."""
    return flavour == "into" and T == time(0, 0)


def _expected_period_endings(
    start: datetime, end: datetime, stride: int,
) -> tuple[datetime, datetime, int]:
    """Compute the period-ending timestamps NEMOSIS should return for a
    query [start, end] on a `stride`-minute table.

    AEMO settlement timestamps are period-ending: a row labelled 04:05
    represents the interval 04:00–04:05. For a query starting at 04:00,
    the first matching row is 04:05; for a query starting at 04:05, the
    first matching row is 04:10 (because 04:05 is at-or-before the start,
    not after it).

    We model that with a pandas date_range over [start, end] at the table's
    stride and discard any timestamp equal to `start`. Assumes `start` and
    `end` are both stride-aligned, which holds for every case in our matrix
    (T values are always 00:00, 04:00, 04:05, or 04:30).

    Returns (first_timestamp, last_timestamp, count). When count == 0
    (window narrower than one stride), first/last are returned as `start`
    as a harmless placeholder — assertion code skips first/last checks
    when count is zero.
    """
    all_points = pd.date_range(start=start, end=end, freq=f"{stride}min")
    period_endings = all_points[all_points > pd.Timestamp(start)]
    if len(period_endings) == 0:
        return start, start, 0
    return (
        period_endings[0].to_pydatetime(),
        period_endings[-1].to_pydatetime(),
        len(period_endings),
    )


# ===========================================================================
# Assertion — split into named sub-checks
# ===========================================================================

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
    `case.stride_minutes` — no gaps, no irregular spacing."""
    consecutive_diffs = {b - a for a, b in zip(timestamps, timestamps[1:])}
    if not consecutive_diffs:
        return  # only one row — nothing to compare
    expected = timedelta(minutes=case.stride_minutes)
    assert consecutive_diffs == {expected}, (
        f"{case.id} [{entity}]: irregular stride {consecutive_diffs}, "
        f"expected single {expected}"
    )
