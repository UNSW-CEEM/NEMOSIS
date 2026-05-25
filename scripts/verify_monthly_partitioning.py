"""Independently verify the partitioning convention of BIDPEROFFER_D vs.
DISPATCH_UNIT_SCADA at month boundaries, using raw AEMO MMSDM archives.

The bid tests claim:
  - BIDPEROFFER_D is partitioned by AEMO trading day (04:05 -> 04:00 next
    day), so the monthly archive for month M holds rows whose
    INTERVAL_DATETIME runs from day 1 04:05 to (M+1) day 1 04:00.
  - DISPATCH_UNIT_SCADA (and tables like it) is partitioned by calendar
    day, so the archive for month M holds rows whose SETTLEMENTDATE runs
    from day 1 00:05 to (M+1) day 1 00:00 (or close to it).

This script downloads the 2021-01 archive for both tables, plus BIDDAYOFFER_D
for completeness, and prints the actual min/max timestamps + the rows around
each boundary. No nemosis or test-fixture imports.

Run: uv run python scripts/verify_monthly_partitioning.py
"""
from __future__ import annotations

import io
import sys
import urllib.request
import urllib.error
import zipfile

import pandas as pd


BASE = "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"

# Pick a single archive month that exists in the legacy DVD format for all
# three tables, so the URL shape is uniform.
YEAR = 2021
MONTH = 1

TABLES = [
    # (name, timestamp column)
    ("BIDPEROFFER_D",       "INTERVAL_DATETIME"),
    ("DISPATCH_UNIT_SCADA", "SETTLEMENTDATE"),
    ("BIDDAYOFFER_D",       "SETTLEMENTDATE"),
]

DUID = "HDWF2"


def url_for(table: str, year: int, month: int) -> str:
    ym = f"{year}{month:02d}"
    folder = f"{BASE}/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA"
    return f"{folder}/PUBLIC_DVD_{table}_{ym}010000.zip"


def fetch_zip(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "verify-partitioning/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def load_csv(blob: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        with zf.open(members[0]) as fh:
            df = pd.read_csv(fh, skiprows=1, low_memory=False)
    return df[df.iloc[:, 0] == "D"].copy()


def report(table: str, ts_col: str, df: pd.DataFrame) -> None:
    df = df[df["DUID"] == DUID].copy()
    df[ts_col] = pd.to_datetime(df[ts_col])
    df = df.sort_values(ts_col)

    tmin, tmax = df[ts_col].min(), df[ts_col].max()
    print(f"=== {table}  (DUID={DUID}, archive {YEAR}-{MONTH:02d}) ===")
    print(f"  rows:        {len(df)}")
    print(f"  {ts_col} min: {tmin}")
    print(f"  {ts_col} max: {tmax}")

    # Show the first 3 and last 3 distinct timestamps so the boundary shape
    # is visible.
    distinct = df[ts_col].drop_duplicates().sort_values()
    print(f"  first 3 ts:  {[str(t) for t in distinct.head(3).tolist()]}")
    print(f"  last 3 ts:   {[str(t) for t in distinct.tail(3).tolist()]}")
    print()


def main() -> int:
    for table, ts_col in TABLES:
        url = url_for(table, YEAR, MONTH)
        print(f"-> {url}")
        blob = fetch_zip(url)
        df = load_csv(blob)
        report(table, ts_col, df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
