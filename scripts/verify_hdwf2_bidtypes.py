"""Independently verify the HDWF2 BIDTYPE set used in
tests/end_to_end_table_tests/test_bid_day_offer_d.py.

Pulls BIDDAYOFFER_D MMS archive zips straight from nemweb.com.au, parses the
CSVs with stdlib + pandas, and prints the distinct BIDTYPE set for HDWF2 on
each fixtured trading day. No nemosis or test-fixture imports.

Run: uv run python scripts/verify_hdwf2_bidtypes.py
"""
from __future__ import annotations

import io
import sys
import urllib.request
import urllib.error
import zipfile
from dataclasses import dataclass

import pandas as pd


BASE = "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"

# (settlement_date_we_expect, archive_year, archive_month)
# BIDDAYOFFER_D for trading day D is published in the MMSDM monthly archive
# for that month. We also try the following month as a fallback because the
# AEMO snapshot stamping convention has shifted over the years.
CASES = [
    ("2018-04-30", 2018, 4),
    ("2021-01-31", 2021, 1),
    ("2024-08-31", 2024, 8),
]


def candidate_urls(year: int, month: int) -> list[str]:
    """Return the URLs we should try for a given archive month, newest format
    first then the legacy DVD format."""
    ym = f"{year}{month:02d}"
    folder = f"{BASE}/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA"
    archive_name = f"PUBLIC_ARCHIVE%23BIDDAYOFFER_D%23FILE01%23{ym}010000.zip"
    legacy_name = f"PUBLIC_DVD_BIDDAYOFFER_D_{ym}010000.zip"
    return [f"{folder}/{legacy_name}", f"{folder}/{archive_name}"]


def fetch_zip(url: str) -> bytes | None:
    req = urllib.request.Request(url, headers={"User-Agent": "verify-hdwf2/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            if r.status != 200:
                return None
            return r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def load_csv_from_zip(blob: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise RuntimeError(f"unexpected zip contents: {members}")
        with zf.open(members[0]) as fh:
            # MMS CSV layout: row 0 is a "C" comment, row 1 is the "I" row
            # (column names), data rows start with "D", trailing "C" row at
            # the end. Skipping the first row makes the I row the header.
            df = pd.read_csv(fh, skiprows=1, low_memory=False)
    df = df[df.iloc[:, 0] == "D"].copy()
    return df


@dataclass
class Result:
    settlement_date: str
    url: str
    bidtypes: set[str]
    row_count: int


def verify_one(settlement_date: str, year: int, month: int) -> Result:
    last_err = None
    for url in candidate_urls(year, month):
        blob = fetch_zip(url)
        if blob is None:
            last_err = f"404: {url}"
            continue
        df = load_csv_from_zip(blob)
        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"])
        target = pd.Timestamp(settlement_date)
        sub = df[(df["DUID"] == "HDWF2") & (df["SETTLEMENTDATE"] == target)]
        return Result(
            settlement_date=settlement_date,
            url=url,
            bidtypes=set(sub["BIDTYPE"].unique()),
            row_count=len(sub),
        )
    raise RuntimeError(f"no archive found for {year}-{month:02d} ({last_err})")


def main() -> int:
    expected = {
        "ENERGY", "RAISEREG", "LOWERREG",
        "RAISE5MIN", "RAISE60SEC", "LOWER5MIN", "LOWER60SEC",
    }

    print(f"Expected (test claims):      {sorted(expected)}\n")
    all_match = True
    for settlement_date, year, month in CASES:
        r = verify_one(settlement_date, year, month)
        match = r.bidtypes == expected
        all_match &= match
        flag = "OK" if match else "MISMATCH"
        print(f"[{flag}] HDWF2 on {r.settlement_date}  ({r.row_count} rows)")
        print(f"        url: {r.url}")
        print(f"        actual:   {sorted(r.bidtypes)}")
        if not match:
            print(f"        missing:  {sorted(expected - r.bidtypes)}")
            print(f"        extra:    {sorted(r.bidtypes - expected)}")
        print()
    return 0 if all_match else 1


if __name__ == "__main__":
    sys.exit(main())
