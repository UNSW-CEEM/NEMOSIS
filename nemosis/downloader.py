import os
import requests
from bs4 import BeautifulSoup
import zipfile
import io
from nemosis import defaults

# Windows Chrome for User-Agent request headers
USR_AGENT_HEADER = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                                   + ' AppleWebKit/537.36 (KHTML, like Gecko) '
                                   + 'Chrome/80.0.3987.87 Safari/537.36')}


def run(year, month, day, index, filename_stub, down_load_to):
    """This function"""

    url = defaults.aemo_data_url
    # Add the year and month information to the generic AEMO data url
    url_formatted = format_aemo_url(url, year, month, filename_stub)

    # Perform the download, unzipping saving of the file
    try:
        status_code = download_unzip_csv(url_formatted, down_load_to)
    except Exception:
        print('Warning: {} not downloaded'.format(filename_stub))


def run_fcas4s(year, month, day, index, filename_stub, down_load_to):
    """This function"""

    # Add the year and month information to the generic AEMO data url
    url_formatted_latest = defaults.fcas_4_url.format(year, month, day, index)
    url_formatted_hist = defaults.fcas_4_url_hist.format(year, year, month,
                                                         year, month, day,
                                                         index)
    # Perform the download, unzipping saving of the file
    try:
        download_unzip_csv(url_formatted_latest, down_load_to)
    except Exception:
        try:
            download_unzip_csv(url_formatted_hist, down_load_to)
        except Exception as e:
            # FCAS csvs are bundled in 30 minute bundles
            # Check if the csv exists before warning
            file_check = os.path.join(down_load_to, filename_stub + '.csv')
            if not os.path.isfile(file_check):
                print('Warning: {} not downloaded'.format(filename_stub))


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
    with open(path_and_name, 'wb') as f:
        f.write(r.content)


def download_elements_file(url, path_and_name):
    page = requests.get(url)
    text = page.text
    soup = BeautifulSoup(text, "html.parser")
    links = soup.find_all('a')
    last_file_name = links[-1].text
    link = url + last_file_name
    r = requests.get(link, headers=USR_AGENT_HEADER)
    with open(path_and_name, 'wb') as f:
        f.write(r.content)


def download_xl(url, path_and_name):
    """
    This function downloads a zipped csv using a url, extracts the csv and
    saves it a specified location
    """
    r = requests.get(url, headers=USR_AGENT_HEADER)
    with open(path_and_name, 'wb') as f:
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
