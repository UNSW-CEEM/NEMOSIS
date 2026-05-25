import os
import logging
import requests
from bs4 import BeautifulSoup
import zipfile
import io
import pandas as pd

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


def run(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=False):
    """This function"""

    url = defaults.aemo_mms_url
    # Add the year and month information to the generic AEMO data url
    url_formatted = format_aemo_url(url, year, month, filename_stub)

    # Perform the download, unzipping saving of the file
    try:
        download_unzip_csv(url_formatted, down_load_to, keep_zip=keep_zip)
    except Exception as e:
        if chunk == 1:
            logger.warning(f"{filename_stub} not downloaded ({e})")


def run_bid_tables(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=False):
    if day is None:
        run(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=keep_zip)
    else:
        try:
            filename_stub = "BIDMOVE_COMPLETE_{year}{month}{day}".format(year=year, month=month, day=day)
            download_url = _get_current_url(
                filename_stub,
                defaults.current_data_page_urls["BIDDING"])
            _download_and_unpack_bid_move_complete_files(
                download_url, down_load_to, keep_zip=keep_zip
            )
        except Exception as e:
            logger.warning(f"{filename_stub} not downloaded ({e})")


def run_next_day_region_tables(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=False):
    try:
        filename_stub = "PUBLIC_DAILY_{year}{month}{day}".format(year=year, month=month, day=day)
        download_url = _get_current_url(
            filename_stub,
            defaults.current_data_page_urls["DAILY_REGION_SUMMARY"])
        _download_and_unpack_next_region_tables(
            download_url, down_load_to, keep_zip=keep_zip
        )
    except Exception as e:
        logger.warning(f"{filename_stub} not downloaded ({e})")


def run_next_dispatch_tables(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=False):
    try:
        filename_stub = "PUBLIC_NEXT_DAY_DISPATCH_{year}{month}{day}".format(year=year, month=month, day=day)
        download_url = _get_current_url(
            filename_stub,
            defaults.current_data_page_urls["NEXT_DAY_DISPATCHLOAD"])
        _download_and_unpack_next_dispatch_load_files_complete_files(download_url, down_load_to, keep_zip=keep_zip)
    except Exception as e:
        logger.warning(f"{filename_stub} not downloaded ({e})")


def run_intermittent_gen_scada(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=False):
    try:
        download_url = _get_current_url(
            filename_stub,
            defaults.current_data_page_urls["INTERMITTENT_GEN_SCADA"])
        _download_and_unpack_intermittent_gen_scada_file(download_url, down_load_to, keep_zip=keep_zip)
    except Exception as e:
        logger.warning(f"{filename_stub} not downloaded ({e})")


def _get_current_url(filename_stub, current_page_url):
    sub_url = _get_matching_link(
        url=defaults.nem_web_domain_url + current_page_url,
        stub_link=filename_stub)
    return defaults.nem_web_domain_url + sub_url


def _download_and_unpack_bid_move_complete_files(
    download_url, down_load_to, keep_zip=False
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


def _download_and_unpack_next_region_tables(
    download_url, down_load_to, keep_zip=False
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


def _download_and_unpack_next_dispatch_load_files_complete_files(
    download_url, down_load_to, keep_zip=False
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


def _download_and_unpack_intermittent_gen_scada_file(
    download_url, down_load_to, keep_zip=False
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



def run_fcas4s(year, month, day, chunk, index, filename_stub, down_load_to, keep_zip=False):
    """This function"""

    # Add the year and month information to the generic AEMO data url
    url_formatted_latest = defaults.fcas_4_url.format(year, month, day, index)
    url_formatted_hist = defaults.fcas_4_url_hist.format(
        year, year, month, year, month, day, index
    )
    # Perform the download, unzipping saving of the file
    try:
        download_unzip_csv(url_formatted_latest, down_load_to, keep_zip=keep_zip)
    except Exception:
        try:
            download_unzip_csv(url_formatted_hist, down_load_to, keep_zip=keep_zip)
        except Exception as e:
            # FCAS csvs are bundled in 30 minute bundles
            # Check if the csv exists before warning
            file_check = os.path.join(down_load_to, filename_stub + ".csv")
            if not os.path.isfile(file_check):
                logger.warning(f"{filename_stub} not downloaded {(e)}")

def download_to_dir(url, down_load_to, force_redo=False):
    """
    Download a file into `down_load_to`, deriving the filename from
    the URL. Returns a (path, downloaded) tuple: `path` is the file's
    absolute path, `downloaded` is True if this call actually fetched
    over the network and False if the file was already in place and
    re-used.

    Streams the response so large files don't have to fit in memory.
    """
    url = url.replace('#', '%23')
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
    url = url.replace('#', '%23')
    if os.path.isfile(path_and_name) and not force_redo:
        return False

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


def download_unzip_csv(url, down_load_to, keep_zip=False):
    """
    Download a zipped csv from a URL, extract its contents into
    `down_load_to`, and (per `keep_zip`) retain the zip on disk.

    `keep_zip=False` (default) cleans up the zip after extracting,
    leaving only the extracted CSV in the cache directory. Cleanup
    only touches zips this call actually downloaded — pre-existing
    zips (from a previous call, or another concurrent process) are
    left alone.

    `keep_zip=True` addresses #56: a cached zip on disk so subsequent
    calls can re-extract without re-downloading (useful on slow
    connections).
    """
    zip_local_path, downloaded = download_to_dir(url, down_load_to)
    try:
        with zipfile.ZipFile(zip_local_path) as z:
            z.extractall(down_load_to)
    finally:
        if downloaded and not keep_zip and os.path.isfile(zip_local_path):
            os.unlink(zip_local_path)


def download_csv(url, path_and_name):
    """
    This function downloads a zipped csv using a url,
    extracts the csv and saves it a specified location
    """
    download_to_path(url, path_and_name)


def download_elements_file(url, path_and_name):
    page = session.get(url)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")
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
    r = session.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "html.parser")
    links = [link.get("href") for link in soup.find_all("a")]
    for link in links:
        if stub_link in link:
            return link
    logger.warning(f"{stub_link} not downloaded because no match for {stub_link} was found on {url}")
