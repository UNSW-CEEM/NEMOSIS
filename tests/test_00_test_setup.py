# This file is named 00 so that it runs first
# to help debug, because it this test fails, all others will fail

import unittest

from nemosis import defaults

class TestDefault(unittest.TestCase):
    def test_cache_path(self):
        self.assertIsNotNone(defaults.raw_data_cache, "raw_data_cache not set. Try setting NEMOSIS_TEST_CACHE env var.")
