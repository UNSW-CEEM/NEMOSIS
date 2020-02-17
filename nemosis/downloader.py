import os
import requests
import zipfile
import io
from nemosis import defaults

# Windows Chrome for User-Agent request headers
USR_AGENT_HEADER = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                                   + ' AppleWebKit/537.36 (KHTML, like Gecko) '
                                   + 'Chrome/80.0.3987.87 Safari/537.36')}

# warning for HTTP 400s
warning_400 = ('Data is unavailable, its location has moved,'
               + ' or additional authentication is required')


def run(year, month, day, index, filename_stub, down_load_to):
    """This function"""

    url = defaults.aemo_data_url
    # Add the year and month information to the generic AEMO data url
    url_formatted = format_aemo_url(url, year, month, filename_stub)

    # Perform the download, unzipping saving of the file
    try:
        status_code = download_unzip_csv(url_formatted, down_load_to)
    except Exception:
        print('Warning {} not downloaded'.format(filename_stub))
        raise
    finally:
        status_code = status_code_return(url_formatted)
        if status_code >= 400 and status_code < 500:
            raise requests.exceptions.HTTPError(f'HTTP {status_code}. '
                                                + f'{warning_400}')


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
                print('Warning {} not downloaded'.format(filename_stub))
                print(e)
    finally:
        status_codes = [status_code_return(url_formatted_latest),
                        status_code_return(url_formatted_hist)]
        http_codes = [x for x in status_codes if x >= 400 and x < 500]
        if http_codes:
            raise requests.exceptions.HTTPError(f'HTTP {http_codes}. '
                                                + f'{warning_400}')


def download_unzip_csv(url, down_load_to):
    """
    This function downloads a zipped csv using a url,
    extracts the csv and saves it a specified location
    """
    r = requests.get(url, headers=USR_AGENT_HEADER)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(down_load_to)


def download_csv(url, down_load_to, path_and_name):
    """
    This function downloads a zipped csv using a url,
    extracts the csv and saves it a specified location
    """
    r = requests.get(url, headers=USR_AGENT_HEADER)
    with open(path_and_name, 'wb') as f:
        f.write(r.content)


def download_xl(url, down_load_to, path_and_name):
    """
    This function downloads a zipped csv using a url, extracts the csv and
    saves it a specified location
    """
    try:
        r = requests.get(url, headers=USR_AGENT_HEADER)
    except requests.exceptions.RequestException as e:
        print(e)
        raise
    finally:
        if r.status_code >= 400 and r.status_code < 500:
            raise requests.exceptions.HTTPError(f'HTTP {r.status_code}. '
                                                + f'{warning_400}')

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
