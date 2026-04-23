"""Download and filter real AEMO data into compact local test fixtures.

Reads the spec in tests/fixtures/spec.py, downloads every (table, era) pair
from AEMO, filters rows down to the entities in the spec, and writes the
result under tests/fixtures/data/ in a directory tree that mirrors the URL
paths on nemweb.com.au and aemo.com.au. The dummy HTTP server serves that
tree verbatim, so every URL AEMO exposes has a 1:1 local counterpart.

Usage:
    python tests/fixtures/build.py              # build missing fixtures only
    python tests/fixtures/build.py --rebuild    # re-download and overwrite all

This is intended to be run rarely — whenever an era or table is added to the
spec — and the resulting files committed to the repo.
"""
from __future__ import annotations

import argparse
import io
import logging
import sys
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = Path(__file__).parent / "data"
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from nemosis import defaults  # noqa: E402
import spec  # noqa: E402

USR_AGENT = {"User-Agent": "NEMOSIS-fixture-builder"}
MMS_ARCHIVE_CUTOVER = date(2024, 8, 1)   # PUBLIC_DVD_ → PUBLIC_ARCHIVE# on this month

# Temporal filter applied to every row whose table has a primary date column.
# Keeping only the first and last few days of the month shrinks fixtures ~6x
# while preserving the start-of-month / end-of-month / month-straddle boundaries
# the tests care about.
KEEP_FIRST_DAYS = 3
KEEP_LAST_DAYS = 2

log = logging.getLogger("fixtures")


# ---------------------------------------------------------------------------
# Network / IO helpers
# ---------------------------------------------------------------------------

def http_get(url: str) -> requests.Response:
    log.info("GET %s", url)
    r = requests.get(url.replace("#", "%23"), headers=USR_AGENT, timeout=180)
    r.raise_for_status()
    return r


def write_file(target: Path, contents: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(contents)
    log.info("wrote %s (%s bytes)", target.relative_to(FIXTURE_ROOT), len(contents))


def read_single_csv_from_zip(zip_bytes: bytes) -> tuple[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        name = z.namelist()[0]
        return name, z.read(name)


def pack_csv_as_zip(csv_name: str, csv_bytes: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(csv_name, csv_bytes)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# MMS CSV filtering
# ---------------------------------------------------------------------------
# AEMO MMS CSVs have the shape:
#   row 0:    C,NEMP.WORLD,...                (summary — preserved as-is)
#   row 1:    I,TABLE,SUBCAT,VERSION,col,col  (column header — preserved as-is)
#   rows 2-N: D,TABLE,SUBCAT,VERSION,val,val  (data rows — filtered)
#   row N+1:  C,"END OF REPORT",n             (footer — preserved as-is)

def filter_mms_csv(
    csv_bytes: bytes,
    filters: dict[str, list],
    date_column: str | None,
    era_date: date,
) -> bytes:
    lines = csv_bytes.decode("utf-8").splitlines(keepends=True)
    summary, footer = lines[0], lines[-1]
    body = "".join(lines[1:-1])

    df = pd.read_csv(io.StringIO(body), dtype=str, keep_default_na=False)
    for col, allowed in filters.items():
        if col not in df.columns:
            raise KeyError(f"filter column {col!r} not found in {list(df.columns)}")
        df = df[df[col].isin([str(v) for v in allowed])]

    if date_column and date_column in df.columns:
        df = _keep_first_and_last_days(df, date_column, era_date)

    out = io.StringIO()
    out.write(summary)
    df.to_csv(out, index=False, lineterminator="\n")
    out.write(footer)
    return out.getvalue().encode("utf-8")


def _keep_first_and_last_days(df: pd.DataFrame, col: str, era_date: date) -> pd.DataFrame:
    """Keep rows from the first KEEP_FIRST_DAYS and last KEEP_LAST_DAYS of the
    era's month. Also keep rows from adjacent months if they happen to be
    present — MMS archives sometimes contain records from the trailing edge of
    the neighbouring month, and tests exercising that behaviour want them."""
    parsed = pd.to_datetime(df[col], errors="coerce")
    same_month = (parsed.dt.year == era_date.year) & (parsed.dt.month == era_date.month)
    days_in_month = (parsed + pd.offsets.MonthEnd(0)).dt.day
    early = same_month & (parsed.dt.day <= KEEP_FIRST_DAYS)
    late = same_month & (parsed.dt.day > days_in_month - KEEP_LAST_DAYS)
    other_month = ~same_month   # preserve any cross-month rows verbatim
    return df[early | late | other_month]


# ---------------------------------------------------------------------------
# URL & path construction (MMS dynamic tables)
# ---------------------------------------------------------------------------

def mms_filename(table: str, era_date: date, chunk: int) -> str:
    year, month = era_date.year, f"{era_date.month:02d}"
    if era_date >= MMS_ARCHIVE_CUTOVER:
        return f"PUBLIC_ARCHIVE#{table}#FILE{chunk:02d}#{year}{month}010000.zip"
    return f"{defaults.names[table]}_{year}{month}010000.zip"


def mms_fixture_path(table: str, era_date: date, chunk: int) -> Path:
    year, month = era_date.year, f"{era_date.month:02d}"
    return (
        FIXTURE_ROOT
        / "Data_Archive/Wholesale_Electricity/MMSDM"
        / str(year)
        / f"MMSDM_{year}_{month}"
        / "MMSDM_Historical_Data_SQLLoader/DATA"
        / mms_filename(table, era_date, chunk)
    )


def mms_source_url(table: str, era_date: date, chunk: int) -> str:
    return defaults.aemo_mms_url.format(
        era_date.year,
        era_date.year,
        f"{era_date.month:02d}",
        mms_filename(table, era_date, chunk).removesuffix(".zip"),
    )


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def build_mms_month(table: str, month: date, *, rebuild: bool) -> None:
    """Download, filter, and write every chunk of one (table, month) MMS archive."""
    cfg = spec.DYNAMIC_TABLES[table]
    filters = cfg["filter"]
    # Effective-date config tables publish sparsely — the default first-3 /
    # last-2 days trim would often leave the fixture empty. They opt out with
    # `keep_full_month: True`, meaning we only apply the row (column-value)
    # filter, never the time filter.
    date_column = None if cfg.get("keep_full_month") else defaults.primary_date_columns.get(table)
    chunked = month >= MMS_ARCHIVE_CUTOVER

    chunk = 1
    while True:
        target = mms_fixture_path(table, month, chunk)
        if target.exists() and not rebuild:
            log.info("skip (cached) %s", target.relative_to(FIXTURE_ROOT))
        else:
            url = mms_source_url(table, month, chunk)
            try:
                zip_bytes = http_get(url).content
            except requests.HTTPError:
                if chunk == 1:
                    raise
                log.info("no chunk %d for %s %s — stopping", chunk, table, month)
                return
            csv_name, csv_bytes = read_single_csv_from_zip(zip_bytes)
            filtered = filter_mms_csv(csv_bytes, filters, date_column, month)
            write_file(target, pack_csv_as_zip(csv_name, filtered))

        if not chunked:
            return
        chunk += 1


def previous_month(month: date) -> date:
    if month.month == 1:
        return date(month.year - 1, 12, 1)
    return date(month.year, month.month - 1, 1)


def build_static_variables_fcas(*, rebuild: bool) -> None:
    """Direct CSV download from aemo.com.au — no scraping, no filtering."""
    url = defaults.static_table_url["VARIABLES_FCAS_4_SECOND"]
    target = FIXTURE_ROOT / url.split("://", 1)[1].split("/", 1)[1]
    if target.exists() and not rebuild:
        log.info("skip (cached) %s", target.relative_to(FIXTURE_ROOT))
        return
    write_file(target, http_get(url).content)


def build_static_generators_xls(*, rebuild: bool) -> None:
    """Direct XLS download from aemo.com.au — covers both 'Generators and
    Scheduled Loads' and 'FCAS Providers', which share the same workbook."""
    url = defaults.static_table_url["Generators and Scheduled Loads"]
    target = FIXTURE_ROOT / url.split("://", 1)[1].split("/", 1)[1]
    if target.exists() and not rebuild:
        log.info("skip (cached) %s", target.relative_to(FIXTURE_ROOT))
        return
    write_file(target, http_get(url).content)


def build_static_elements_fcas(*, rebuild: bool) -> None:
    """Scrape-pattern: download the newest Elements CSV and write a fake
    directory index alongside it so the live scraper logic still works."""
    index_url = defaults.static_table_url["ELEMENTS_FCAS_4_SECOND"]
    fixture_dir = FIXTURE_ROOT / index_url.split("://", 1)[1].split("/", 1)[1]

    soup = BeautifulSoup(http_get(index_url).text, "html.parser")
    links = [a.text for a in soup.find_all("a") if a.text.endswith(".csv")]
    if not links:
        raise RuntimeError("no Elements_FCAS CSV links found on AEMO index page")
    filename = links[-1]   # mirror downloader.download_elements_file's choice

    target = fixture_dir / filename
    if target.exists() and not rebuild:
        log.info("skip (cached) %s", target.relative_to(FIXTURE_ROOT))
    else:
        write_file(target, http_get(index_url + filename).content)

    elements_path = index_url.split("://", 1)[1].split("/", 1)[1]
    write_file(fixture_dir / "index.html",
               make_index_html([filename], elements_path).encode("utf-8"))


def make_index_html(filenames: list[str], url_prefix: str) -> str:
    """Minimal HTML directory listing. Real AEMO pages are messy tables of
    server-generated links with absolute-path hrefs like /Reports/Current/X/Y.zip;
    NEMOSIS concatenates those onto the domain URL, so our fake index must use
    the same absolute-path form (not ./relative) for the resulting URL to point
    at the right file on our server."""
    anchors = "\n".join(f'<a href="/{url_prefix.strip("/")}/{n}">{n}</a>' for n in filenames)
    return f"<html><body>\n{anchors}\n</body></html>"


# ---------------------------------------------------------------------------
# Scrape-pattern (BIDMOVE_COMPLETE, PUBLIC_DAILY, PUBLIC_NEXT_DAY_*)
# ---------------------------------------------------------------------------
# These tables live on AEMO's rolling "current data" index pages — each page
# lists daily zips for the last few months. NEMOSIS finds them by scraping
# the page for a link whose href contains a date-stamped filename stub.
# Build-time complication: dates that age out of the window stop being served,
# so we pick one date that's currently on the page when build runs (ERAS.recent)
# and also fetch the day before, which NEMOSIS's date generator buffers for.

# Each AEMO scrape file may contain several logical tables in one CSV,
# delimited by 'I' header rows. NEMOSIS only reads specific sections — we
# filter those by column-value and strip the D-rows of everything else,
# which preserves the I-row structure NEMOSIS's section-counter relies on.
#   sections_to_keep: section-index → filter dict
SCRAPE_FILES = {
    "Reports/Current/Daily_Reports/": {
        "stub_template": "PUBLIC_DAILY_{ymd}",
        "sections_to_keep": {1: {"REGIONID": spec.REGIONS}},
    },
    "Reports/Current/NEXT_DAY_DISPATCH/": {
        "stub_template": "PUBLIC_NEXT_DAY_DISPATCH_{ymd}",
        "sections_to_keep": {0: {"DUID": spec.DUIDS}},
    },
    "Reports/Current/Next_Day_Intermittent_Gen_Scada/": {
        "stub_template": "PUBLIC_NEXT_DAY_INTERMITTENT_GEN_SCADA_{ymd}",
        "sections_to_keep": {0: {"DUID": spec.DUIDS}},
    },
}

# Which NEMOSIS table maps to which scrape file.
TABLE_SCRAPE_FILE = {
    "DAILY_REGION_SUMMARY":  "Reports/Current/Daily_Reports/",
    "NEXT_DAY_DISPATCHLOAD": "Reports/Current/NEXT_DAY_DISPATCH/",
    "INTERMITTENT_GEN_SCADA": "Reports/Current/Next_Day_Intermittent_Gen_Scada/",
}


def build_scrape_day(table: str, day: date, *, rebuild: bool) -> None:
    """Download, filter, and write one day's scrape-pattern fixture.

    Also rewrites the directory's fake index.html to list every fixture
    file currently present (so a later build for a different day adds a
    link rather than replacing the page).
    """
    path = TABLE_SCRAPE_FILE[table]
    cfg = SCRAPE_FILES[path]
    ymd = f"{day.year}{day.month:02d}{day.day:02d}"
    stub = cfg["stub_template"].format(ymd=ymd)
    fixture_dir = FIXTURE_ROOT / path

    existing_match = next(
        (p for p in fixture_dir.glob("*") if stub in p.name and p.suffix != ".html"),
        None,
    )
    if existing_match and not rebuild:
        log.info("skip (cached) %s", existing_match.relative_to(FIXTURE_ROOT))
    else:
        href, zip_bytes = _fetch_matching_link(path, stub)
        filename = href.split("/")[-1]
        filtered_zip = _filter_multisection_zip(zip_bytes, cfg["sections_to_keep"])
        write_file(fixture_dir / filename, filtered_zip)

    _rewrite_index(fixture_dir)


def _fetch_matching_link(path: str, stub: str) -> tuple[str, bytes]:
    """Mirror NEMOSIS's scraper: hit the index, find an anchor whose href
    contains the stub, download it. Returns (href, zip-bytes)."""
    index_url = defaults.nem_web_domain_url.rstrip("/") + "/" + path
    soup = BeautifulSoup(http_get(index_url).text, "html.parser")
    domain = defaults.nem_web_domain_url.rstrip("/")
    for link in soup.find_all("a"):
        href = link.get("href") or ""
        if stub not in href:
            continue
        if href.startswith("http"):
            zip_url = href
        elif href.startswith("/"):
            zip_url = domain + href          # absolute path on the domain
        else:
            zip_url = index_url.rstrip("/") + "/" + href.lstrip("./")
        return href, http_get(zip_url).content
    raise RuntimeError(f"no link matching {stub!r} on {index_url}")


def _rewrite_index(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    filenames = sorted(
        p.name for p in directory.iterdir() if p.is_file() and p.suffix != ".html"
    )
    url_prefix = directory.relative_to(FIXTURE_ROOT).as_posix()
    write_file(directory / "index.html",
               make_index_html(filenames, url_prefix).encode("utf-8"))


# ---------------------------------------------------------------------------
# Multi-section AEMO CSV parsing (for BIDMOVE_COMPLETE, PUBLIC_DAILY, etc.)
# ---------------------------------------------------------------------------
# Several AEMO zip archives contain a single CSV holding multiple logical
# tables — each delimited by an 'I' header row, ending with a 'C' footer.
# We split at I-rows, filter each section independently (so BIDDAYOFFER_D
# gets filtered by DUID, and so does BIDPEROFFER_D alongside it).

def _filter_multisection_zip(zip_bytes: bytes, sections_to_keep: dict[int, dict]) -> bytes:
    """Filter sections listed in `sections_to_keep` by their column-values,
    and drop all D rows from every other section. I rows stay untouched so
    NEMOSIS's section-counting (_find_start_row_nth_table) still matches up.
    """
    csv_name, csv_bytes = read_single_csv_from_zip(zip_bytes)
    text = csv_bytes.decode("utf-8")
    lines = text.splitlines(keepends=True)
    summary = lines[0]
    footer = lines[-1] if lines[-1].startswith("C,") else ""
    body = lines[1:-1] if footer else lines[1:]

    sections = _split_mms_sections(body)
    for idx, section in enumerate(sections):
        if idx in sections_to_keep:
            section.update(_filter_one_section(section, sections_to_keep[idx]))
        else:
            section["data"] = []    # strip — NEMOSIS never reads this section

    rebuilt = summary
    for section in sections:
        rebuilt += section["header"]
        rebuilt += "".join(section["data"])
    rebuilt += footer
    return pack_csv_as_zip(csv_name, rebuilt.encode("utf-8"))


def _split_mms_sections(body_lines: list[str]) -> list[dict]:
    sections: list[dict] = []
    current: dict | None = None
    for line in body_lines:
        if line.startswith("I,"):
            if current is not None:
                sections.append(current)
            current = {"header": line, "data": []}
        elif line.startswith("D,") and current is not None:
            current["data"].append(line)
    if current is not None:
        sections.append(current)
    return sections


def _filter_one_section(section: dict, filters: dict[str, list]) -> dict:
    if not section["data"]:
        return section
    df = pd.read_csv(
        io.StringIO(section["header"] + "".join(section["data"])),
        dtype=str, keep_default_na=False,
    )
    for col, allowed in filters.items():
        if col in df.columns:
            df = df[df[col].isin([str(v) for v in allowed])]
    out = io.StringIO()
    df.to_csv(out, index=False, lineterminator="\n")
    lines = out.getvalue().splitlines(keepends=True)
    return {"header": lines[0], "data": lines[1:]}


def previous_day(day: date) -> date:
    from datetime import timedelta
    return day - timedelta(days=1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild", action="store_true",
                        help="re-download and overwrite existing fixtures")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-5s %(message)s",
    )

    failed: list[tuple[str, str, str]] = []

    for table, cfg in spec.DYNAMIC_TABLES.items():
        # `extra_eras` are built but not picked up by `boundary_cases` — used
        # for standalone tests whose windows don't fit the matrix's shape
        # (e.g. straddling a stride-transition month like 2021-10).
        for era_key in cfg["eras"] + cfg.get("extra_eras", []):
            era_date = spec.ERAS[era_key]
            try:
                if _is_scrape_only(table, era_date):
                    build_scrape_day(table, era_date, rebuild=args.rebuild)
                    # NEMOSIS buffers one day back for scrape tables.
                    try:
                        build_scrape_day(table, previous_day(era_date), rebuild=args.rebuild)
                    except (requests.HTTPError, RuntimeError) as e:
                        log.info("prev-day buffer unavailable for %s @ %s (%s)",
                                 table, era_key, e)
                else:
                    build_mms_month(table, era_date, rebuild=args.rebuild)
                    try:
                        build_mms_month(table, previous_month(era_date), rebuild=args.rebuild)
                    except requests.HTTPError as e:
                        log.info("prev-month buffer unavailable for %s @ %s (%s)",
                                 table, era_key, e)
            except (requests.HTTPError, RuntimeError) as e:
                failed.append((table, era_key, str(e)))
                log.error("FAILED %s @ %s — %s", table, era_key, e)

    for name in spec.STATIC_TABLES:
        builder = {
            "ELEMENTS_FCAS_4_SECOND": build_static_elements_fcas,
            "VARIABLES_FCAS_4_SECOND": build_static_variables_fcas,
            "Generators and Scheduled Loads": build_static_generators_xls,
        }[name]
        try:
            builder(rebuild=args.rebuild)
        except requests.HTTPError as e:
            failed.append((name, "static", str(e)))
            log.error("FAILED %s — %s", name, e)

    if failed:
        log.error("%d fixture(s) failed — fix the spec or retry:", len(failed))
        for table, era, err in failed:
            log.error("  %s @ %s: %s", table, era, err)
        sys.exit(1)


def _is_scrape_only(table: str, era_date: date) -> bool:
    """Some AEMO tables are only published on rolling current-data pages, not
    in the MMS monthly archive. (BIDPEROFFER_D and BIDDAYOFFER_D use MMS —
    NEMOSIS's dead `BIDDING` code path would go through the Bidmove_Complete
    scrape, but no table is actually wired to it.)"""
    return table in ("DAILY_REGION_SUMMARY", "NEXT_DAY_DISPATCHLOAD", "INTERMITTENT_GEN_SCADA")


if __name__ == "__main__":
    main()
