import unittest
from src.nemosis import dynamic_data_compiler, cache_compiler, static_table
from src.nemosis import defaults
import os
from unittest.mock import patch
from io import StringIO


class TestDynamicDataCompilerRaisesExpectedErrors(unittest.TestCase):
    def test_raise_error_for_incorrect_table_name(self):
        with self.assertRaises(Exception) as context:
            dynamic_data_compiler(
                "2000/01/01 00:00:00",
                "2000/02/01 00:00:00",
                "NOTATABLE",
                defaults.raw_data_cache,
            )
        self.assertTrue(
            "Table name provided is not a dynamic table." in str(context.exception)
        )

    def test_raise_error_for_no_data_returned(self):
        with self.assertRaises(Exception) as context:
            dynamic_data_compiler(
                "2000/01/01 00:00:00",
                "2000/02/01 00:00:00",
                "DISPATCHPRICE",
                defaults.raw_data_cache,
            )
        self.assertTrue(
            (
                f"Compiling data for table DISPATCHPRICE failed. "
                + "This probably because none of the requested data "
                + "could be download from AEMO. Check your internet "
                + "connection and that the requested data is archived on: "
                + "https://nemweb.com.au see nemosis.defaults for table specific urls."
            )
            in str(context.exception)
        )

    def test_raise_error_for_filter_column_not_in_select_columns(self):
        with self.assertRaises(Exception) as context:
            dynamic_data_compiler(
                "2019/01/01 00:00:00",
                "2019/02/01 00:00:00",
                "DISPATCHPRICE",
                defaults.raw_data_cache,
                select_columns=["REGIONID", "SETTLEMENTDATE", "RRP"],
                filter_cols=["INTERVENTION"],
                filter_values=(["0"],),
            )
        self.assertTrue(
            (
                "Filter columns not valid. They must be a part of "
                + "select_columns or the table defaults."
            )
            in str(context.exception)
        )

    def test_raise_error_for_filter_column_not_in_default_columns(self):
        with self.assertRaises(Exception) as context:
            dynamic_data_compiler(
                "2019/01/01 00:00:00",
                "2019/02/01 00:00:00",
                "DISPATCHPRICE",
                defaults.raw_data_cache,
                select_columns=["REGIONID", "SETTLEMENTDATE", "RRP"],
                filter_cols=["NOTACOLUMN"],
                filter_values=(["0"],),
            )
        self.assertTrue(
            (
                "Filter columns not valid. They must be a part of "
                + "select_columns or the table defaults."
            )
            in str(context.exception)
        )

    def test_raise_error_if_fformat_not_in_expected_set(self):
        with self.assertRaises(Exception) as context:
            dynamic_data_compiler(
                "2019/01/01 00:00:00",
                "2019/02/01 00:00:00",
                "DISPATCHPRICE",
                defaults.raw_data_cache,
                fformat="db",
            )
        self.assertTrue(
            "Argument fformat must be 'csv', 'feather' or 'parquet'"
            in str(context.exception)
        )

    def test_raise_error_if_select_columns_not_in_data(self):
        with self.assertRaises(Exception) as context:
            dynamic_data_compiler(
                "2019/01/01 00:00:00",
                "2019/02/01 00:00:00",
                "DISPATCHPRICE",
                defaults.raw_data_cache,
                select_columns=["NOTACOLUMN"],
            )
        self.assertIn(
            (
                f"None of columns ['NOTACOLUMN'] are in D:/nemosis_test_cache\\PUBLIC_DVD_DISPATCHPRICE_201812010000.feather. "
                "This may be caused by user input if the 'select_columns' "
                "argument is being used, or by changed AEMO data formats. "
                "This error can be avoided by using the argument select_columns='all'."
            ),
            str(context.exception)
        )

    def test_using_select_columns_all_does_not_raise_error(self):
        price_data = dynamic_data_compiler(
            "2019/01/01 00:00:00",
            "2019/02/01 00:00:00",
            "DISPATCHPRICE",
            defaults.raw_data_cache,
            select_columns="all",
            fformat="csv",
        )
        expected_columns = [
            "I",
            "DISPATCH",
            "PRICE",
            "1",
            "SETTLEMENTDATE",
            "RUNNO",
            "REGIONID",
            "DISPATCHINTERVAL",
            "INTERVENTION",
            "RRP",
            "EEP",
            "ROP",
            "APCFLAG",
            "MARKETSUSPENDEDFLAG",
            "LASTCHANGED",
            "RAISE6SECRRP",
            "RAISE6SECROP",
            "RAISE6SECAPCFLAG",
            "RAISE60SECRRP",
            "RAISE60SECROP",
            "RAISE60SECAPCFLAG",
            "RAISE5MINRRP",
            "RAISE5MINROP",
            "RAISE5MINAPCFLAG",
            "RAISEREGRRP",
            "RAISEREGROP",
            "RAISEREGAPCFLAG",
            "LOWER6SECRRP",
            "LOWER6SECROP",
            "LOWER6SECAPCFLAG",
            "LOWER60SECRRP",
            "LOWER60SECROP",
            "LOWER60SECAPCFLAG",
            "LOWER5MINRRP",
            "LOWER5MINROP",
            "LOWER5MINAPCFLAG",
            "LOWERREGRRP",
            "LOWERREGROP",
            "LOWERREGAPCFLAG",
            "PRICE_STATUS",
            "PRE_AP_ENERGY_PRICE",
            "PRE_AP_RAISE6_PRICE",
            "PRE_AP_RAISE60_PRICE",
            "PRE_AP_RAISE5MIN_PRICE",
            "PRE_AP_RAISEREG_PRICE",
            "PRE_AP_LOWER6_PRICE",
            "PRE_AP_LOWER60_PRICE",
            "PRE_AP_LOWER5MIN_PRICE",
            "PRE_AP_LOWERREG_PRICE",
            "CUMUL_PRE_AP_ENERGY_PRICE",
            "CUMUL_PRE_AP_RAISE6_PRICE",
            "CUMUL_PRE_AP_RAISE60_PRICE",
            "CUMUL_PRE_AP_RAISE5MIN_PRICE",
            "CUMUL_PRE_AP_RAISEREG_PRICE",
            "CUMUL_PRE_AP_LOWER6_PRICE",
            "CUMUL_PRE_AP_LOWER60_PRICE",
            "CUMUL_PRE_AP_LOWER5MIN_PRICE",
            "CUMUL_PRE_AP_LOWERREG_PRICE",
        ]
        self.assertSequenceEqual(list(price_data.columns), expected_columns)

class TestWarnings(unittest.TestCase):
    def test_no_parse_warning(self):
        with patch('sys.stderr', new=StringIO()) as fakeOutput:
            print('hello world')
                
            dynamic_data_compiler(
                start_time='2017/01/01 00:00:00', 
                end_time='2017/01/01 00:05:00', 
                table_name='DISPATCHPRICE', 
                raw_data_location=defaults.raw_data_cache
            )

            stderr = fakeOutput.getvalue().strip()
        self.assertNotIn("UserWarning: Could not infer format", stderr)

class TestCacheCompilerRaisesExpectedErrors(unittest.TestCase):
    def test_raise_error_for_incorrect_table_name(self):
        with self.assertRaises(Exception) as context:
            cache_compiler(
                "2019/01/01 00:00:00",
                "2019/02/01 00:00:00",
                "NOTATABLE",
                defaults.raw_data_cache,
                fformat="db",
            )
        self.assertTrue(
            "Table name provided is not a dynamic table." in str(context.exception)
        )

    def test_raise_error_if_fformat_not_in_expected_set(self):
        with self.assertRaises(Exception) as context:
            cache_compiler(
                "2019/01/01 00:00:00",
                "2019/02/01 00:00:00",
                "DISPATCHPRICE",
                defaults.raw_data_cache,
                fformat="db",
            )
        self.assertTrue(
            "Argument fformat must be 'feather' or 'parquet'" in str(context.exception)
        )

    def test_raise_error_if_select_columns_used_without_rebuild_true(self):
        with self.assertRaises(Exception) as context:
            cache_compiler(
                "2019/01/01 00:00:00",
                "2019/02/01 00:00:00",
                "DISPATCHPRICE",
                defaults.raw_data_cache,
                select_columns="all",
            )
        self.assertTrue(
            (
                "The select_columns argument must be used with rebuild=True "
                + "to ensure the cache is built with the correct columns."
            )
            in str(context.exception)
        )



class TestStaticTableRaisesExpectedErrors(unittest.TestCase):
    def test_raise_error_for_incorrect_table_name(self):
        with self.assertRaises(Exception) as context:
            static_table("NOTATABLE", defaults.raw_data_cache)
        self.assertTrue(
            "Table name provided is not a static table." in str(context.exception)
        )

    def test_raise_error_for_no_data_returned(self):
        good_url = defaults.static_table_url["VARIABLES_FCAS_4_SECOND"]
        defaults.static_table_url["VARIABLES_FCAS_4_SECOND"] = "bad_url"
        path_and_name = (
                defaults.raw_data_cache + "/" + defaults.names["VARIABLES_FCAS_4_SECOND"]
        )
        if os.path.isfile(path_and_name):
            os.remove(path_and_name)
        with self.assertRaises(Exception) as context:
            static_table("VARIABLES_FCAS_4_SECOND", defaults.raw_data_cache)
        self.assertTrue(
            (
                f"Compiling data for table VARIABLES_FCAS_4_SECOND failed. "
                + "This probably because none of the requested data "
                + "could be download from AEMO. Check your internet "
                + "connection and that the requested data is archived on: "
                + "https://nemweb.com.au see nemosis.defaults for table specific urls."
            )
            in str(context.exception)
        )
        defaults.static_table_url["VARIABLES_FCAS_4_SECOND"] = good_url

    def test_raise_error_for_filter_column_not_in_select_columns(self):
        with self.assertRaises(Exception) as context:
            static_table(
                "VARIABLES_FCAS_4_SECOND",
                defaults.raw_data_cache,
                select_columns=["VARIABLENUMBER"],
                filter_cols=["VARIABLETYPE"],
                filter_values=(["0"],),
            )
        self.assertTrue(
            (
                "Filter columns not valid. They must be a part of "
                + "select_columns or the table defaults."
            )
            in str(context.exception)
        )

    def test_raise_error_for_filter_column_not_in_default_columns(self):
        with self.assertRaises(Exception) as context:
            static_table(
                "VARIABLES_FCAS_4_SECOND",
                defaults.raw_data_cache,
                select_columns=["VARIABLENUMBER"],
                filter_cols=["NOTACOLUMN"],
                filter_values=(["0"],),
            )
        self.assertTrue(
            (
                "Filter columns not valid. They must be a part of "
                + "select_columns or the table defaults."
            )
            in str(context.exception)
        )

    def test_raise_error_if_select_columns_not_in_data(self):
        with self.assertRaises(Exception) as context:
            static_table(
                "VARIABLES_FCAS_4_SECOND",
                defaults.raw_data_cache,
                select_columns=["NOTACOLUMN"],
            )
        self.assertIn(
            (
                f"None of columns ['NOTACOLUMN'] are in D:/nemosis_test_cache\\Ancillary Services Market Causer Pays Variables File.csv. "
                "This may be caused by user input if the 'select_columns' "
                "argument is being used, or by changed AEMO data formats. "
                "This error can be avoided by using the argument select_columns='all'."
            ),
            str(context.exception)
        )

    def test_using_select_columns_all_does_not_raise_error(self):
        price_data = static_table(
            "VARIABLES_FCAS_4_SECOND", defaults.raw_data_cache, select_columns="all"
        )
        expected_columns = ["VARIABLENUMBER", "VARIABLETYPE"]
        self.assertSequenceEqual(list(price_data.columns), expected_columns)
