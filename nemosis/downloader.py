import os
import logging
import requests
from bs4 import BeautifulSoup
import zipfile
import io
import pandas as pd

from . import defaults, custom_errors

logger = logging.getLogger(__name__)

# Windows Chrome for User-Agent request headers
USR_AGENT_HEADER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        + " AppleWebKit/537.36 (KHTML, like Gecko) "
        + "Chrome/80.0.3987.87 Safari/537.36"
    )
}


def run(year, month, day, index, filename_stub, down_load_to):
    """This function"""

    url = defaults.aemo_data_url
    # Add the year and month information to the generic AEMO data url
    url_formatted = format_aemo_url(url, year, month, filename_stub)

    # Perform the download, unzipping saving of the file
    try:
        download_unzip_csv(url_formatted, down_load_to)
    except Exception:
        logger.warning(f"{filename_stub} not downloaded")


def run_bid_tables(year, month, day, index, filename_stub, down_load_to):
    if day is None:
        run(year, month, day, index, filename_stub, down_load_to)
    else:
        try:
            _download_and_unpack_bid_move_complete_files(
                year, month, day, index, filename_stub, down_load_to
            )
        except Exception:
            logger.warning(f"{filename_stub} not downloaded. This is likely because this file is not being hosted \n" +
                           "online by AEMO. You can check this url to confirm: \n" +
                           "https://www.nemweb.com.au/REPORTS/Archive/Bidmove_Complete/. If the file is available but \n"
                           "this warning persists please contact the NEMOSIS maintainers.")

def _download_and_unpack_bid_move_complete_files(
    year, month, day, index, filename_stub, down_load_to
):
    bid_move_complete_url = "https://www.nemweb.com.au/REPORTS/Archive/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_{year}{month}02.zip"
    bid_move_complete_url = bid_move_complete_url.format(year=year, month=month)
    r = requests.get(bid_move_complete_url, headers=USR_AGENT_HEADER)
    main_zipfile = zipfile.ZipFile(io.BytesIO(r.content))
    sub_folder_names = main_zipfile.namelist()
    for name in sub_folder_names:
        sub_folder_zipfile_bytes = main_zipfile.read(name)
        sub_folder_zipfile = zipfile.ZipFile(io.BytesIO(sub_folder_zipfile_bytes))
        file_name = sub_folder_zipfile.namelist()[
            0
        ]  # Just one file so we can pull it out of the list using 0
        start_row_second_table = _find_start_row_second_table(
            sub_folder_zipfile, file_name
        )
        csv_file = sub_folder_zipfile.open(file_name)
        BIDDAYOFFER_D = pd.read_csv(
            csv_file, header=1, nrows=start_row_second_table - 3, dtype=str
        )
        BIDDAYOFFER_D.to_csv(
            os.path.join(
                down_load_to,
                "PUBLIC_DVD_BIDDAYOFFER_D_" + file_name[24:32] + "0000" + ".csv",
            ),
            index=False,
        )
        csv_file = sub_folder_zipfile.open(file_name)
        BIDPEROFFER_D = pd.read_csv(
            csv_file, header=start_row_second_table - 1, dtype=str
        )[:-1]
        BIDPEROFFER_D.to_csv(
            os.path.join(
                down_load_to,
                "PUBLIC_DVD_BIDPEROFFER_D_" + file_name[24:32] + "0000" + ".csv",
            ),
            index=False,
        )


def _find_start_row_second_table(sub_folder_zipfile, file_name):
    row = 0
    table_start_rows_found = 0
    with sub_folder_zipfile.open(file_name) as f:
        for line in f:
            row += 1
            if str(line)[2] == "I":
                table_start_rows_found += 1
                table_start_row = row
    if table_start_rows_found != 2:
        raise custom_errors.DataFormatError(
            "The data in table BIDMOVE_COMPLETE was not in the expected format. \n"
            + "Please contact the NEMOSIS package maintainers."
        )
    return table_start_row


def run_fcas4s(year, month, day, index, filename_stub, down_load_to):
    """This function"""

    # Add the year and month information to the generic AEMO data url
    url_formatted_latest = defaults.fcas_4_url.format(year, month, day, index)
    url_formatted_hist = defaults.fcas_4_url_hist.format(
        year, year, month, year, month, day, index
    )
    # Perform the download, unzipping saving of the file
    try:
        download_unzip_csv(url_formatted_latest, down_load_to)
    except Exception:
        try:
            download_unzip_csv(url_formatted_hist, down_load_to)
        except Exception as e:
            # FCAS csvs are bundled in 30 minute bundles
            # Check if the csv exists before warning
            file_check = os.path.join(down_load_to, filename_stub + ".csv")
            if not os.path.isfile(file_check):
                logger.warning(f"{filename_stub} not downloaded")


def download_unzip_csv(url, down_load_to):
    """
    This function downloads a zipped csv using a url,
    extracts the csv and saves it a specified location
    """
    r = requests.get(url, headers=USR_AGENT_HEADER)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(down_load_to)


def download_csv(url, path_and_name):
    """
    This function downloads a zipped csv using a url,
    extracts the csv and saves it a specified location
    """
    r = requests.get(url, headers=USR_AGENT_HEADER)
    with open(path_and_name, "wb") as f:
        f.write(r.content)


def download_elements_file(url, path_and_name):
    page = requests.get(url)
    text = page.text
    soup = BeautifulSoup(text, "html.parser")
    links = soup.find_all("a")
    last_file_name = links[-1].text
    link = url + last_file_name
    r = requests.get(link, headers=USR_AGENT_HEADER)
    with open(path_and_name, "wb") as f:
        f.write(r.content)


def download_xl(url, path_and_name):
    """
    This function downloads a zipped csv using a url, extracts the csv and
    saves it a specified location
    """
    r = requests.get(url, headers=USR_AGENT_HEADER)
    with open(path_and_name, "wb") as f:
        f.write(r.content)


def format_aemo_url(url, year, month, filename_stub):
    """
    This fills in the missing information in the AEMO URL
    so data for the right month, year and file name are
    downloaded
    """
    year = str(year)
    return url.format(year, year, month, filename_stub)


def status_code_return(url):
    r = requests.get(url, headers=USR_AGENT_HEADER)
    return r.status_code
