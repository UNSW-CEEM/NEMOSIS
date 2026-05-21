# Testing and maintenance

This document covers how the NEMOSIS test suite is structured, how to run it, and how to extend or
maintain it as AEMO's data formats evolve.

## Overview

The end-to-end suite runs against a local dummy HTTP server (defined in `tests/conftest.py`) that
serves pre-filtered AEMO data committed to `tests/fixtures/data/`. The server is a bare
`SimpleHTTPServer` rooted at the fixture tree, which mirrors AEMO's URL paths verbatim — at test
time NEMOSIS's hostname is swapped for the local server and everything else is real.

The whole suite is fast (under a minute) and network-free.

## Running tests

```console
$ uv run pytest tests/ --ignore=tests/test_performance_stats.py
```

`test_performance_stats.py` is the last legacy network-hitting file. It covers the
`custom_tables` module (VWAP, capacity factors, plant stats) which hasn't been ported offline
yet — most of its assertions run on synthetic DataFrames and are trivially portable, but a
few depend on local plant-stats files (`E:\plants_stats_test_data\`) that exist only on one
developer's machine. CI uses the same invocation (see `.github/workflows/cicd.yml`).

To run a single file:

```console
$ uv run pytest tests/end_to_end_table_tests/test_dispatch_price.py -v
```

## Suite layout

```
tests/
  conftest.py                      # Dummy HTTP server fixture
  fixtures/
    spec.py                        # What to download for each table
    build.py                       # Downloads + filters + writes fixture files
    data/                          # Committed fixture tree (mirrors AEMO URLs)
  end_to_end_table_tests/
    _boundaries.py                 # Shared boundary-test helper
    test_<table>.py                # One file per AEMO table
    DECISIONS.md                   # Internal design notes (transitional)
  test_filters.py                  # Pure unit tests
  test_date_generators.py
  test_query_wrappers.py
  test_errors.py                   # Argument validation + NoDataToReturn + cache idempotency
  test_processing_info_maps.py     # Cross-table validation of search_type classification
  test_performance_stats.py        # Legacy custom_tables tests — ignored in CI
```

Each test file in `end_to_end_table_tests/` covers one AEMO table. Files start with a module
docstring explaining the table's shape and which eras the fixture covers.

## The fixture system

`tests/fixtures/data/` is a verbatim mirror of AEMO's URL tree. For example:

```
http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2018/...
        ↓
tests/fixtures/data/Data_Archive/Wholesale_Electricity/MMSDM/2018/...
```

This means the dummy server doesn't need any clever routing — it just serves static files at the
matching paths.

Fixtures are filtered down (~50× smaller than raw AEMO archives) by:

- **Row filter**: each table keeps only two DUIDs / two REGIONIDs / one INTERCONNECTORID. The
  selection lives in `tests/fixtures/spec.py` (`DUIDS`, `REGIONS`, etc.).
- **Time filter**: only the first 3 and last 2 days of each month, controlled by
  `KEEP_FIRST_DAYS` / `KEEP_LAST_DAYS` in `build.py`.
- **Section filter** (scrape-pattern files only): for files like `PUBLIC_DAILY` that bundle many
  logical tables in one CSV, the build script keeps only the section NEMOSIS reads and strips the
  D-rows from the rest, preserving the I-row markers NEMOSIS counts against.

Total fixture tree: ~12 MB across ~200 files.

## Eras

An era is a single month (or day, for scrape-pattern tables) chosen because it straddles a known
AEMO format change or sits on a year boundary. Each table's fixture covers one or more eras. The
full list lives in `tests/fixtures/spec.py::ERAS`:

| Era       | Purpose                                                                   |
|-----------|---------------------------------------------------------------------------|
| `2018-05` | Pre-5-min-trading baseline                                                |
| `2020-01` | Year boundary, pre-5-min, monthly bidding                                 |
| `2021-02` | Last viable MMS month for bidding before the 3-year gap                   |
| `2021-05` | First month of the new 5-min NEM and daily bidding layouts                |
| `2022-01` | Year boundary, 5-min dispatch, 30-min trading legacy                      |
| `2022-06` | Legacy era (kept to avoid fixture churn — could be retired)               |
| `2024-09` | First month after `PUBLIC_DVD_` → `PUBLIC_ARCHIVE#` rename                |
| `2025-01` | Year boundary, `PUBLIC_ARCHIVE#` format, post-bidding-gap                 |
| `recent`  | Pinned to a date inside AEMO's rolling current-data window                |

The three Jan-1 eras (2020-01, 2022-01, 2025-01) exist primarily for the boundary tests —
year wraps are exercised by anchoring boundary cases on Jan 1.

## Boundary tests

`_boundaries.py` generates two test cases per (table, era) for every dynamic time-series table,
both anchored on midnight of day 1 of the era month — the natural fence between monthly fixture
files. Each case asserts entity-set match, exact per-entity row count, first/last timestamp, no
duplicates, and regular stride.

**Vocabulary:**

- **Era** — a tagged anchor month (e.g. `2021-05`).
- **Stride** — the table's native interval, in minutes (5, 30, etc.).
- **Boundary** — midnight on day 1 of the era month.
- **Flavour** — how the 1-hour query window sits against the boundary:
  - `at` — `[day1 00:00, day1 01:00]`. Every returned row is in the era-month file, but NEMOSIS
    must still reach into the previous month's file (the uniform buffer-back) in case it holds
    rows dated into the new month. Catches buffer-back failing to fire.
  - `before` — `[last-day 23:00, day1 00:00]`. Every returned row is in the *previous* month's
    file. Catches the previous-month file not running right up to `day1 00:00` — e.g. if a
    table's month overhang shifts from calendar-day to market-day partitioning — and confirms
    NEMOSIS never needs a *forward* buffer.

Together the two flavours cover every boundary failure mode known: under-fetch (`at`),
end-of-archive convention drift (`before`), and filter off-by-one at both window edges. An
earlier version probed a 9-cell matrix (3 window shapes × 3 times-of-day); 7 of the 9 cells
turned out to be re-testing plain filter correctness, which is a property of the filter and
only needs probing once.

Year wraps are exercised for free: the boundary at a Jan-1 era *is* the year boundary, so the
`at`/`before` cases at 2020-01 / 2022-01 / 2025-01 cover the Dec→Jan file stitch.

**Tables not covered by the helper** (with reasons, in `_boundaries.py::_HELPER_DOES_NOT_COVER`):

- `BIDDAYOFFER_D`, `MNSP_DAYOFFER` — daily stride, not per-interval.
- `BIDPEROFFER_D` — partitioned by AEMO trading day (04:05 → 04:00 next day) not calendar day;
  covered directly in `test_bid_per_offer_d.py` instead.
- `MNSP_PEROFFER` — multi-dimensional rows + known bug (see Known quirks).

These tables keep their per-era smoke tests.

## Adding a test for a new table

1. **Pick eras** the table should cover. Refer to the era table above. Most dynamic time-series
   tables use the dispatch set: `2018-05`, `2020-01`, `2021-05`, `2022-01`, `2024-09`, `2025-01`
   (the three Jan-1 eras give year-boundary coverage for the matrix).
2. **Add the table to `tests/fixtures/spec.py::DYNAMIC_TABLES`** with its filter spec — which
   eras, which row filter columns and values, which columns to keep, etc.
3. **Run `uv run python tests/fixtures/build.py`** to download, filter, and write the fixture
   files into `tests/fixtures/data/`.
4. **Write `test_<table_lower>.py`** in `tests/end_to_end_table_tests/`, following the style of
   an existing file. Use the `nemosis_fixture` fixture (from `conftest.py`) to get a fresh cache
   directory. If the table is suitable for the boundary tests, parametrise on
   `boundary_cases("<TABLE_NAME>")` and assert with `assert_boundary_shape(...)`.
5. **Run** `uv run pytest tests/end_to_end_table_tests/test_<table>.py -v` and iterate.

## Refreshing fixtures

Most fixtures are stable and don't need refreshing — the eras are anchored to historical months
that AEMO doesn't change. The exception is the `recent` era, which is pinned to a date inside
AEMO's rolling current-data window (`/Reports/Current/...`). That window only retains the last
few months of data, so the `recent` fixtures age out periodically.

The committed fixtures keep working indefinitely — staleness only bites when you next try to
*rebuild* (e.g. adding a new table that uses the `recent` era, or refiltering an existing one).
At that point AEMO will return 404s for the scrape-pattern tables, which is the signal to refresh.

To refresh:

1. Update `spec.ERAS["recent"]` to a date inside AEMO's current window (within the last ~2
   months is safe).
2. Re-run `uv run python tests/fixtures/build.py`.
3. Commit the updated fixtures.

Tables that use the `recent` era: `DAILY_REGION_SUMMARY`, `NEXT_DAY_DISPATCHLOAD`,
`INTERMITTENT_GEN_SCADA`.

## Adding a new era

When AEMO changes a data format, add an era to capture the transition:

1. Pick a single month (or a Jan-1 month if you also want year-boundary coverage) that straddles
   the change.
2. Add it to `spec.ERAS`.
3. Add the era key to the affected tables' `eras` lists in `spec.DYNAMIC_TABLES`.
4. Re-run `uv run python tests/fixtures/build.py` to fetch the new fixtures.
5. If the format change affects the table's row count, columns, or stride, adjust the test file
   accordingly (era-parametrise where behaviour differs).

## Known quirks

These have bitten contributors before. Worth a read before adding a test.

- **`filter_cols` must be a subset of `select_columns`** when calling `dynamic_data_compiler`.
- **`select_columns` for tables wrapped by `drop_duplicates_by_primary_key`** (MNSP_PEROFFER,
  MNSP_DAYOFFER, the effective-date config tables) must include every column in
  `defaults.table_primary_keys[table]`, or NEMOSIS errors with a `KeyError` on the missing dedupe
  columns. SPDCONNECTIONPOINTCONSTRAINT is a particularly easy one to trip on — its primary key
  includes BIDTYPE.
- **Prev-month / prev-day buffering** in NEMOSIS's date generator only fires when `start_time`
  lands on a natural boundary (first of month, midnight). Tests that probe interior points don't
  see those extra buffer requests.
- **`filter_on_effective_date` runs before `most_recent_records_before_start_time`** and drops any
  row with `EFFECTIVEDATE >= end_time`. Effective-date fixture entities therefore need at least
  one record dated *before* the test's `start_time`, or the test sees an empty frame.
- **Effective-date config tables** have `search_type = "all"` in NEMOSIS, which by default scans
  every month from `nem_data_model_start_time` (2009-07) through `end_time` — ~180 HTTP requests
  per call. Tests narrow this to a single month by monkeypatching
  `defaults.nem_data_model_start_time` to the era date, and opt out of the build-time time trim
  via `keep_full_month: True` in `spec.py`.
- **Year-straddle queries aren't covered.** A true year-boundary crossing for a non-Jan era —
  e.g. `[2024/08/15, 2025/01/15]` — would exercise `year_and_month_gen`'s `start_year < end_year`
  branch with many months of iteration. The fixture doesn't include Sep/Oct/Nov 2024, so such
  queries 404 on intervening months. If a future contributor needs this coverage, extend the
  fixture to include the intervening months (cost: fixture size) or add a unit test against
  `year_and_month_gen` directly.
- **Mid-month queries return empty frames.** The fixture builder keeps only the first 3 and last
  2 days of each era month (see `KEEP_FIRST_DAYS` / `KEEP_LAST_DAYS` in `build.py`). A test that
  queries, say, `[2021/05/15, 2021/05/16]` will get back a non-erroring empty DataFrame from every
  covered table — NEMOSIS's own code path is fine, the fixture just doesn't have those rows. If
  you're writing a test whose window lands in the kept-days gap, either anchor it to day 1–3 / the
  last 2 days of the era, or `assert not data.empty` explicitly so the empty case fails loudly
  rather than sliding past a soft subset check.
- **`MNSP_PEROFFER` is effectively unreadable past ~April 2021**. AEMO began writing
  `MNSP_BIDOFFERPERIOD` data into the existing `PUBLIC_DVD_MNSP_PEROFFER_*` archives, then
  renamed the archive stub outright in Aug-2024. NEMOSIS's defaults know neither transition.
  Pre-existing bug, tracked in the test file and `spec.py`. `MNSP_DAYOFFER` is unaffected.

## Parked tables

- **FCAS_4_SECOND** — AEMO's Causer Pays archive is empty at every URL NEMOSIS tries (existing
  upstream issue #64). No fixture, no test.
- **FCAS_4s_SCADA_MAP** — built by `custom_tables.py::fcas4s_scada_match`, which consumes
  `FCAS_4_SECOND` values to find lowest-error element↔DUID matches. Blocked on the same upstream
  outage. A standalone fixture would mean fabricating the 4-second values the algorithm exists to
  match — no meaningful test.
