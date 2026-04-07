import unittest
import os

from nemosis import downloader

class TestDownloader(unittest.TestCase):
    def test_pre_check_file_is_missing(self):
        html_url = "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2026/MMSDM_2026_02/MMSDM_Historical_Data_SQLLoader/DATA/"
        child_link = "PUBLIC_ARCHIVE%23BIDDAYOFFER_D%23FILE01%23202602010000.zip"
        child_url = os.path.join(html_url, child_link)
        bad_child_link = "not_here.zip"
        bad_url = os.path.join(html_url, bad_child_link)
        self.assertFalse(downloader._pre_check_file_is_missing(child_url))
        self.assertTrue(downloader._pre_check_file_is_missing(bad_url))

        # This is aemo.com.au not nemweb.com.au
        # so don't do the check
        registration_url = "https://www.aemo.com.au/-/media/files/electricity/nem/participant_information/nem-registration-and-exemption-list.xlsx?rev=144b01b07674458b875e7a87a8293d92&sc_lang=en"
        self.assertIsNone(downloader._pre_check_file_is_missing(registration_url))
