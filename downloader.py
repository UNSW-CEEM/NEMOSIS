import requests
import zipfile
import io
import os

if os.getcwd().split('\\')[-1] == 'osdan':
    import defaults
else:
    from osdan import defaults


def run(year, month, day, index, filename, down_load_to):
    """This function"""

    url = defaults.aemo_data_url
    # Add the year and month information to the generic AEMO data url
    url_formatted = format_aemo_url(url, year, month, filename)

    # Perform the download, unzipping saving of the file
    try:
        download_unzip_csv(url_formatted, down_load_to, filename)
    except:
        print('Warning {} not downloaded'.format(filename))


def run_fcas4s(year, month, day, index, filename, down_load_to):
    """This function"""

    # Add the year and month information to the generic AEMO data url
    url_formatted_latest = defaults.fcas_4_url.format(year, month, day, index)
    url_formatted_hist = defaults.fcas_4_url_hist.format(year, year, month, year, month, day, index)


    # Perform the download, unzipping saving of the file
    try:
        download_unzip_csv(url_formatted_latest, down_load_to, filename)
    except:
        try:
            download_unzip_csv(url_formatted_hist, down_load_to, filename)
        except:
            print('Warning {} not downloaded'.format(filename))


def download_unzip_csv(url, down_load_to, filename):
    """This function downloads a zipped csv using a url, extracts the csv and saves it a specified location and with
    a specified filename"""
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(down_load_to)


def download_csv(url, down_load_to, path_and_name):
    """This function downloads a zipped csv using a url, extracts the csv and saves it a specified location and with
    a specified filename"""
    r = requests.get(url)
    with open(path_and_name, 'wb') as f:
        f.write(r.content)


def download_xl(url, down_load_to, path_and_name):
    """This function downloads a zipped csv using a url, extracts the csv and saves it a specified location and with
    a specified filename"""
    r = requests.get(url)
    with open(path_and_name, 'wb') as f:
        f.write(r.content)

def format_aemo_url(url, year, month, filename):
    """This fills in the missing information in the AEMO url so data for the right month, year and file name are
    downloaded"""
    year = str(year)
    return url.format(year, year, month, filename[:-4])

