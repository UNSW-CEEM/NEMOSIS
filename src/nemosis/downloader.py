import os
import logging
import requests
from bs4 import BeautifulSoup
import zipfile
import io
from urllib.parse import urljoin, urldefrag

import pandas as pd
from cachetools import cached, TTLCache

from . import defaults, custom_errors

logger = logging.getLogger(__name__)

# Module-level requests.Session for connection reuse across the many
# small downloads NEMOSIS makes when scanning monthly archives. The
# session also centralises the User-Agent so every request looks like a
# current Windows Chrome — AEMO's `aemo.com.au` host filters out older
# UAs as suspected scrapers, and `nemweb.com.au` is happy either way.
# Chrome 130 was the current stable when this was written; bump
# periodically to stay plausible.
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    )
})


# ---------------------------------------------------------------------------
# Parent-directory HTML cache (powers the missing-file pre-check below)
# ---------------------------------------------------------------------------
#
# Scanning historical NEMOSIS data hits many monthly archive paths that
# don't yet exist — each one normally costs a ~200-500ms 404 round-trip
# to nemweb. Nemweb serves a browsable HTML index for each archive
# directory, so we can fetch the parent listing once and answer many
# missing-file questions from it without further network traffic.
#
# 1-hour TTL is long enough to amortise across a multi-year scan, short
# enough that re-running an hour later sees fresh state. The cache is
# exposed at module scope so tests can clear it between cases — see the
# autouse `_clear_downloader_html_cache` fixture in tests/conftest.py.
_html_cache: TTLCache = TTLCache(maxsize=2**10, ttl=60 * 60)

# Nemweb's directory-listed archive trees. Other AEMO endpoints (e.g.
# the hashed PUBLIC_ARCHIVE# files under aemo_mms_url) don't have a
# browsable parent, so the pre-check sits this out for them.
_NEMWEB_HTML_PRECHECK_PREFIXES = (
    "https://www.nemweb.com.au/Data_Archive/",
    "https://www.nemweb.com.au/Reports/",
)


@cached(_html_cache)
def download_html(url):
    """Fetch `url` and return its body as text. Cached at module scope
    for ~1 hour — see `_html_cache` for the rationale."""
    r = session.get(url)
    r.raise_for_status()
    return r.text


def download_html_as_soup(url):
    """BeautifulSoup-parsed view of `url`'s HTML body. Backed by the
    same TTL cache as `download_html`."""
    return BeautifulSoup(download_html(url), "html.parser")


def _pre_check_file_is_missing(file_url):
    """Return True if `file_url`'s parent directory listing is reachable
    and the file is NOT linked from it, False if it IS linked, or None
    when no parent listing is available (non-nemweb URL, non-archive
    file extension, parent fetch failed, etc).

    A True answer lets the caller skip a guaranteed-404 round-trip.
    A None answer means the caller should fall through to a real
    request and let any 404 surface the normal way.
    """
    if not any(file_url.startswith(prefix) for prefix in _NEMWEB_HTML_PRECHECK_PREFIXES):
        return None
    if not (file_url.upper().endswith(".ZIP") or file_url.upper().endswith(".CSV")):
        return None

    parent_url = urljoin(file_url, "./")
    try:
        soup = download_html_as_soup(parent_url)
    except Exception:
        # Parent listing unreachable — fall through to a real request.
        return None

    target = urldefrag(file_url)[0]
    for a_tag in soup.find_all("a", href=True):
        absolute = urljoin(parent_url, a_tag["href"])
        if urldefrag(absolute)[0] == target:
            return False
    return True


def run(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=True):
    """
    Returns True if a network fetch occurred, False if a cached zip
    was reused, or None if the attempt failed (in which case a warning
    has already been emitted). Callers use this to log honestly about
    whether AEMO was contacted (see _download_data).
    """

    url = defaults.aemo_mms_url
    # Add the year and month information to the generic AEMO data url
    url_formatted = format_aemo_url(url, year, month, filename_stub)

    # Perform the download, unzipping saving of the file
    try:
        return download_unzip_csv(url_formatted, down_load_to, keep_zip=keep_zip)
    except Exception as e:
        if chunk == 1:
            logger.warning(f"{filename_stub} not downloaded ({e})")
        return None


def run_bid_tables(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=True):
    if day is None:
        return run(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=keep_zip)
    else:
        try:
            filename_stub = "BIDMOVE_COMPLETE_{year}{month}{day}".format(year=year, month=month, day=day)
            download_url = _get_current_url(
                filename_stub,
                defaults.current_data_page_urls["BIDDING"])
            return _download_and_unpack_bid_move_complete_files(
                download_url, down_load_to, keep_zip=keep_zip
            )
        except Exception as e:
            logger.warning(f"{filename_stub} not downloaded ({e})")
            return None


def run_next_day_region_tables(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=True):
    try:
        filename_stub = "PUBLIC_DAILY_{year}{month}{day}".format(year=year, month=month, day=day)
        download_url = _get_current_url(
            filename_stub,
            defaults.current_data_page_urls["DAILY_REGION_SUMMARY"])
        return _download_and_unpack_next_region_tables(
            download_url, down_load_to, keep_zip=keep_zip
        )
    except Exception as e:
        logger.warning(f"{filename_stub} not downloaded ({e})")
        return None


def run_next_dispatch_tables(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=True):
    try:
        filename_stub = "PUBLIC_NEXT_DAY_DISPATCH_{year}{month}{day}".format(year=year, month=month, day=day)
        download_url = _get_current_url(
            filename_stub,
            defaults.current_data_page_urls["NEXT_DAY_DISPATCHLOAD"])
        return _download_and_unpack_next_dispatch_load_files_complete_files(download_url, down_load_to, keep_zip=keep_zip)
    except Exception as e:
        logger.warning(f"{filename_stub} not downloaded ({e})")
        return None


def run_intermittent_gen_scada(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=True):
    try:
        download_url = _get_current_url(
            filename_stub,
            defaults.current_data_page_urls["INTERMITTENT_GEN_SCADA"])
        return _download_and_unpack_intermittent_gen_scada_file(download_url, down_load_to, keep_zip=keep_zip)
    except Exception as e:
        logger.warning(f"{filename_stub} not downloaded ({e})")
        return None


def _get_current_url(filename_stub, current_page_url):
    sub_url = _get_matching_link(
        url=defaults.nem_web_domain_url + current_page_url,
        stub_link=filename_stub)
    return defaults.nem_web_domain_url + sub_url


def _download_and_unpack_bid_move_complete_files(
    download_url, down_load_to, keep_zip=True
):
    zip_local_path, downloaded = download_to_dir(download_url, down_load_to)
    try:
        with zipfile.ZipFile(zip_local_path) as zipped_file:

            file_name = zipped_file.namelist()[
                0
            ]  # Just one file so we can pull it out of the list using 0
            start_row_second_table = _find_start_row_nth_table(
                zipped_file, file_name, 2
            )
            csv_file = zipped_file.open(file_name)
            BIDDAYOFFER_D = pd.read_csv(
                csv_file, header=1, nrows=start_row_second_table - 3, dtype=str
            )
            BIDDAYOFFER_D.to_csv(
                os.path.join(
                    down_load_to,
                    "PUBLIC_DVD_BIDDAYOFFER_D_" + file_name[24:32] + ".csv",
                ),
                index=False,
            )
            csv_file = zipped_file.open(file_name)
            BIDPEROFFER_D = pd.read_csv(
                csv_file, header=start_row_second_table - 1, dtype=str
            )[:-1]
            BIDPEROFFER_D.to_csv(
                os.path.join(
                    down_load_to,
                    "PUBLIC_DVD_BIDPEROFFER_D_" + file_name[24:32] + ".csv",
                ),
                index=False,
            )
    finally:
        if downloaded and not keep_zip and os.path.isfile(zip_local_path):
            os.unlink(zip_local_path)
    return downloaded


def _download_and_unpack_next_region_tables(
    download_url, down_load_to, keep_zip=True
):
    zip_local_path, downloaded = download_to_dir(download_url, down_load_to)
    try:
        with zipfile.ZipFile(zip_local_path) as zipped_file:

            file_name = zipped_file.namelist()[
                0
            ]  # Just one file so we can pull it out of the list using 0
            start_row_second_table = _find_start_row_nth_table(
                zipped_file, file_name, 2
            )
            start_row_third_table = _find_start_row_nth_table(
                zipped_file, file_name, 3
            )
            csv_file = zipped_file.open(file_name)
            DAILY_REGION_SUMMARY = pd.read_csv(
                csv_file, header=start_row_second_table - 1,
                nrows=start_row_third_table - start_row_second_table - 1, dtype=str
            )
            DAILY_REGION_SUMMARY.to_csv(
                os.path.join(
                    down_load_to,
                    "PUBLIC_DAILY_REGION_SUMMARY_" + file_name[13:21] + ".csv",
                ),
                index=False,
            )
    finally:
        if downloaded and not keep_zip and os.path.isfile(zip_local_path):
            os.unlink(zip_local_path)
    return downloaded


def _download_and_unpack_next_dispatch_load_files_complete_files(
    download_url, down_load_to, keep_zip=True
):
    zip_local_path, downloaded = download_to_dir(download_url, down_load_to)
    try:
        with zipfile.ZipFile(zip_local_path) as zipped_file:

            file_name = zipped_file.namelist()[
                0
            ]  # Just one file so we can pull it out of the list using 0
            start_row_second_table = _find_start_row_nth_table(
                zipped_file, file_name, 2
            )
            csv_file = zipped_file.open(file_name)
            NEXT_DAY_DISPATCHLOAD = pd.read_csv(
                csv_file, header=1, nrows=start_row_second_table - 3, dtype=str
            )
            NEXT_DAY_DISPATCHLOAD.to_csv(
                os.path.join(
                    down_load_to,
                    "PUBLIC_NEXT_DAY_DISPATCHLOAD_" + file_name[25:33] + ".csv",
                ),
                index=False,
            )
    finally:
        if downloaded and not keep_zip and os.path.isfile(zip_local_path):
            os.unlink(zip_local_path)
    return downloaded


def _download_and_unpack_intermittent_gen_scada_file(
    download_url, down_load_to, keep_zip=True
):
    zip_local_path, downloaded = download_to_dir(download_url, down_load_to)
    try:
        with zipfile.ZipFile(zip_local_path) as zipped_file:

            file_name = zipped_file.namelist()[
                0
            ]  # Just one file so we can pull it out of the list using 0
            start_row_second_table = _find_start_row_nth_table(
                zipped_file, file_name, 1
            )
            csv_file = zipped_file.open(file_name)
            data = pd.read_csv(
                csv_file, header=1, dtype=str
            )[:-1]
            data.to_csv(
                os.path.join(
                    down_load_to,
                    "PUBLIC_NEXT_DAY_INTERMITTENT_GEN_SCADA_" + file_name[39:47] + ".csv",
                ),
                index=False,
            )
    finally:
        if downloaded and not keep_zip and os.path.isfile(zip_local_path):
            os.unlink(zip_local_path)
    return downloaded


def _find_start_row_nth_table(sub_folder_zipfile, file_name, n):
    row = 0
    table_start_rows_found = 0
    with sub_folder_zipfile.open(file_name) as f:
        for line in f:
            row += 1
            if str(line)[2] == "I":
                table_start_rows_found += 1
                table_start_row = row
                if table_start_rows_found == n:
                    return table_start_row
    raise custom_errors.DataFormatError(
        "The data in table BIDMOVE_COMPLETE was not in the expected format. \n"
        + "Please contact the NEMOSIS package maintainers."
    )



def run_fcas4s(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=True):
    """
    Returns True if a network fetch occurred, False if a cached zip
    was reused, or None if both fallback URLs failed (in which case a
    warning has already been emitted unless the CSV was already on
    disk from a prior call).
    """

    # Add the year and month information to the generic AEMO data url
    url_formatted_latest = defaults.fcas_4_url.format(year, month, day, index)
    url_formatted_hist = defaults.fcas_4_url_hist.format(
        year, year, month, year, month, day, index
    )
    # Perform the download, unzipping saving of the file
    try:
        return download_unzip_csv(url_formatted_latest, down_load_to, keep_zip=keep_zip)
    except Exception:
        try:
            return download_unzip_csv(url_formatted_hist, down_load_to, keep_zip=keep_zip)
        except Exception as e:
            # FCAS csvs are bundled in 30 minute bundles
            # Check if the csv exists before warning
            file_check = os.path.join(down_load_to, filename_stub + ".csv")
            if not os.path.isfile(file_check):
                logger.warning(f"{filename_stub} not downloaded {(e)}")
            return None

def download_to_dir(url, down_load_to, force_redo=False):
    """
    Download a file into `down_load_to`, deriving the filename from
    the URL. Returns a (path, downloaded) tuple: `path` is the file's
    absolute path, `downloaded` is True if this call actually fetched
    over the network and False if the file was already in place and
    re-used.

    Streams the response so large files don't have to fit in memory.
    """
    # Post-2024-07 AEMO archive files are stored on nemweb with literal
    # `%23` in their on-disk filenames (not `#`). To match, the URL must
    # contain `%2523` so nemweb decodes it once to `%23` and finds the
    # file. A single `%23` would decode to `#` and 400. Pre-Aug-2024
    # PUBLIC_DVD_* filenames don't contain `#`, so the replace is a
    # no-op for the older path. See issue #74.
    url = url.replace('#', '%2523')
    filename = url.split('/')[-1].split('?')[0]
    path = os.path.join(down_load_to, filename)
    downloaded = download_to_path(url, path, force_redo=force_redo)
    return path, downloaded


def download_to_path(url, path_and_name, force_redo=False):
    """
    Download a file from `url` to `path_and_name`. Returns True if a
    new network fetch occurred, False if the destination already
    existed and was re-used.

    Idempotent — if the destination already exists, the function
    short-circuits unless `force_redo=True`. On a write failure
    mid-stream, the partial output file is removed before the
    exception propagates.
    """
    # See `download_to_dir` for why this is `%2523` and not `%23`.
    # Repeated here because `download_to_path` is also called directly
    # (e.g. from `download_csv`); the replace is idempotent (`%2523`
    # contains no `#`) so double-encoding via `download_to_dir` is safe.
    url = url.replace('#', '%2523')
    if os.path.isfile(path_and_name) and not force_redo:
        return False

    if _pre_check_file_is_missing(url):
        # The parent directory's HTML listing told us this file doesn't
        # exist. Synthesise a 404 HTTPError so the caller's existing 404
        # handling (added in PR #85) fires uniformly whether the answer
        # came from the cache or the wire.
        synthetic = requests.Response()
        synthetic.status_code = 404
        synthetic.reason = "Not Found (cached parent directory listing)"
        synthetic.url = url
        raise requests.HTTPError(
            f"404 Client Error: {synthetic.reason} for url: {url}",
            response=synthetic,
        )

    with session.get(url, stream=True) as response:
        response.raise_for_status()
        try:
            with open(path_and_name, 'wb') as file:
                for chunk in response.iter_content(chunk_size=2**13):
                    file.write(chunk)
        except Exception as e:
            logger.error(f"Failed to write file to {path_and_name}: {e}")
            if os.path.isfile(path_and_name):
                os.unlink(path_and_name)
            raise
    return True


def download_unzip_csv(url, down_load_to, keep_zip=True):
    """
    Download a zipped csv from a URL, extract its contents into
    `down_load_to`, and (per `keep_zip`) retain the zip on disk.

    `keep_zip=True` (default) keeps the compressed archive on disk
    after extraction, so subsequent calls (e.g. cache rebuilds,
    format changes, slow connections per #56) can re-extract
    without re-downloading.

    `keep_zip=False` cleans up the zip after extracting, leaving
    only the extracted CSV in the cache directory. Cleanup only
    touches zips this call actually downloaded — pre-existing zips
    (from a previous call, or another concurrent process) are left
    alone.

    Returns True if a network fetch occurred, False if a cached zip
    already on disk was reused. Callers use this to log honestly
    about whether AEMO was contacted.
    """
    zip_local_path, downloaded = download_to_dir(url, down_load_to)
    try:
        with zipfile.ZipFile(zip_local_path) as z:
            z.extractall(down_load_to)
    finally:
        if downloaded and not keep_zip and os.path.isfile(zip_local_path):
            os.unlink(zip_local_path)
    return downloaded


def download_csv(url, path_and_name):
    """
    This function downloads a csv using a url,
    and saves it to a specified location.
    """
    download_to_path(url, path_and_name)


def download_elements_file(url, path_and_name):
    soup = download_html_as_soup(url)
    links = soup.find_all("a")
    last_file_name = links[-1].text
    link = url + last_file_name

    download_to_path(link, path_and_name)
    

def download_xlsx(url, path_and_name):
    """
    Download an Excel (.xlsx) file from a URL and save it to the
    specified path. Used for AEMO's NEM Registration and Exemption List.
    """
    download_to_path(url, path_and_name)


def format_aemo_url(url, year, month, filename_stub):
    """
    This fills in the missing information in the AEMO URL
    so data for the right month, year and file name are
    downloaded
    """
    year = str(year)
    return url.format(year, year, month, filename_stub)

def status_code_return(url):
    r = session.get(url)
    return r.status_code


def _get_matching_link(url, stub_link):
    soup = download_html_as_soup(url)
    links = [link.get("href") for link in soup.find_all("a")]
    for link in links:
        if stub_link in link:
            return link
    logger.warning(f"{stub_link} not downloaded because no match for {stub_link} was found on {url}")
