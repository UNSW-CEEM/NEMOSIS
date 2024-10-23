import unittest
from datetime import timedelta
from src.nemosis import defaults, data_fetch_methods
import pandas as pd
import os


class TestFormatOptions(unittest.TestCase):
    def setUp(self):
        # TODO: Clean tests since only one table - BIDDAYOFFER_D is tested
        self.table_names = ["BIDDAYOFFER_D"]

        self.table_types = {
            "DISPATCHLOAD": "DUID",
            "DISPATCHCONSTRAINT": "CONSTRAINTID",
            "DISPATCH_UNIT_SCADA": "DUID",
            "DISPATCHPRICE": "REGIONID",
            "DISPATCHINTERCONNECTORRES": "INTERCONNECTORID",
            "DISPATCHREGIONSUM": "REGIONID",
            "BIDPEROFFER_D": "DUID-BIDTYPE",
            "BIDDAYOFFER_D": "DUID-BIDTYPE",
            "TRADINGLOAD": "DUID",
            "TRADINGPRICE": "REGIONID",
            "TRADINGREGIONSUM": "REGIONID",
            "TRADINGINTERCONNECT": "INTERCONNECTORID",
        }

        self.filter_values = {
            "DUID": (["AGLHAL"],),
            "REGIONID": (["SA1"],),
            "INTERCONNECTORID": (["VIC1-NSW1"],),
            "CONSTRAINTID": (["DATASNAP_DFS_Q_CLST"],),
            "DUID-BIDTYPE": (["AGLHAL", "ENERGY"],),
        }

    def test_dispatch_tables_start_of_month_just_csv_format_dont_keep(self):
        # Empty cache.
        for f in os.listdir(defaults.raw_data_cache):
            os.remove(os.path.join(defaults.raw_data_cache, f))

        start_time = "2018/02/01 00:00:00"
        end_time = "2018/02/01 05:15:00"
        for table in self.table_names:
            print("Testing {} returing values at start of month.".format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 63
            expected_number_of_columns = 2
            expected_first_time = pd.to_datetime(
                start_time, format="%Y/%m/%d %H:%M:%S"
            ) + timedelta(minutes=5)
            expected_last_time = pd.to_datetime(end_time, format="%Y/%m/%d %H:%M:%S")
            if table in [
                "TRADINGLOAD",
                "TRADINGPRICE",
                "TRADINGREGIONSUM",
                "TRADINGINTERCONNECT",
            ]:
                expected_length = 10
                expected_first_time = "2018/02/01 00:30:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_last_time = "2018/02/01 05:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
            if table == "BIDPEROFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
            if table == "BIDDAYOFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
                expected_length = 2
                expected_last_time = "2018/02/01 00:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_first_time = "2018/01/31 00:00:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
            data = data_fetch_methods.dynamic_data_compiler(
                start_time,
                end_time,
                table,
                defaults.raw_data_cache,
                select_columns=cols,
                filter_cols=filter_cols,
                filter_values=self.filter_values[table_type],
                fformat="csv",
                keep_csv=False,
            )
            data = data.reset_index(drop=True)
            print(table)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            self.assertEqual(len(os.listdir(defaults.raw_data_cache)), 0)
            print("Passed")

    def test_dispatch_tables_start_of_month_just_csv_format(self):
        # Empty cache.
        for f in os.listdir(defaults.raw_data_cache):
            os.remove(os.path.join(defaults.raw_data_cache, f))

        start_time = "2018/02/01 00:00:00"
        end_time = "2018/02/01 05:15:00"
        for table in self.table_names:
            print("Testing {} returing values at start of month.".format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 63
            expected_number_of_columns = 2
            expected_first_time = pd.to_datetime(
                start_time, format="%Y/%m/%d %H:%M:%S"
            ) + timedelta(minutes=5)
            expected_last_time = pd.to_datetime(end_time, format="%Y/%m/%d %H:%M:%S")
            if table in [
                "TRADINGLOAD",
                "TRADINGPRICE",
                "TRADINGREGIONSUM",
                "TRADINGINTERCONNECT",
            ]:
                expected_length = 10
                expected_first_time = "2018/02/01 00:30:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_last_time = "2018/02/01 05:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
            if table == "BIDPEROFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
            if table == "BIDDAYOFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
                expected_length = 2
                expected_last_time = "2018/02/01 00:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_first_time = "2018/01/31 00:00:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
            data = data_fetch_methods.dynamic_data_compiler(
                start_time,
                end_time,
                table,
                defaults.raw_data_cache,
                select_columns=cols,
                filter_cols=filter_cols,
                filter_values=self.filter_values[table_type],
                fformat="csv",
            )
            data = data.reset_index(drop=True)
            print(table)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            self.assertNotEqual(len(os.listdir(defaults.raw_data_cache)), 0)
            print("Passed")

            # Test that also works on second pass.
            start_time = "2018/02/01 00:00:00"
            end_time = "2018/02/01 05:15:00"
            for table in self.table_names:
                print("Testing {} returing values at start of month.".format(table))
                dat_col = defaults.primary_date_columns[table]
                table_type = self.table_types[table]
                cols = [dat_col, self.table_types[table]]
                filter_cols = (self.table_types[table],)
                expected_length = 63
                expected_number_of_columns = 2
                expected_first_time = pd.to_datetime(
                    start_time, format="%Y/%m/%d %H:%M:%S"
                ) + timedelta(minutes=5)
                expected_last_time = pd.to_datetime(
                    end_time, format="%Y/%m/%d %H:%M:%S"
                )
                if table in [
                    "TRADINGLOAD",
                    "TRADINGPRICE",
                    "TRADINGREGIONSUM",
                    "TRADINGINTERCONNECT",
                ]:
                    expected_length = 10
                    expected_first_time = "2018/02/01 00:30:00"
                    expected_first_time = pd.to_datetime(
                        expected_first_time, format="%Y/%m/%d %H:%M:%S"
                    )
                    expected_last_time = "2018/02/01 05:00:00"
                    expected_last_time = pd.to_datetime(
                        expected_last_time, format="%Y/%m/%d %H:%M:%S"
                    )
                if table == "BIDPEROFFER_D":
                    cols = [dat_col, "DUID", "BIDTYPE"]
                    filter_cols = ("DUID", "BIDTYPE")
                    expected_number_of_columns = 3
                if table == "BIDDAYOFFER_D":
                    cols = [dat_col, "DUID", "BIDTYPE"]
                    filter_cols = ("DUID", "BIDTYPE")
                    expected_number_of_columns = 3
                    expected_length = 2
                    expected_last_time = "2018/02/01 00:00:00"
                    expected_last_time = pd.to_datetime(
                        expected_last_time, format="%Y/%m/%d %H:%M:%S"
                    )
                    expected_first_time = "2018/01/31 00:00:00"
                    expected_first_time = pd.to_datetime(
                        expected_first_time, format="%Y/%m/%d %H:%M:%S"
                    )
                data = data_fetch_methods.dynamic_data_compiler(
                    start_time,
                    end_time,
                    table,
                    defaults.raw_data_cache,
                    select_columns=cols,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type],
                    fformat="csv",
                )
                data = data.reset_index(drop=True)
                print(table)
                self.assertEqual(expected_length, data.shape[0])
                self.assertEqual(expected_number_of_columns, data.shape[1])
                self.assertEqual(expected_first_time, data[dat_col][0])
                self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
                self.assertNotEqual(len(os.listdir(defaults.raw_data_cache)), 0)
                print("Passed")

    def test_dispatch_tables_start_of_month_feather_format(self):
        start_time = "2018/02/01 00:00:00"
        end_time = "2018/02/01 05:15:00"
        for table in self.table_names:
            print("Testing {} returing values at start of month.".format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 63
            expected_number_of_columns = 2
            expected_first_time = pd.to_datetime(
                start_time, format="%Y/%m/%d %H:%M:%S"
            ) + timedelta(minutes=5)
            expected_last_time = pd.to_datetime(end_time, format="%Y/%m/%d %H:%M:%S")
            if table in [
                "TRADINGLOAD",
                "TRADINGPRICE",
                "TRADINGREGIONSUM",
                "TRADINGINTERCONNECT",
            ]:
                expected_length = 10
                expected_first_time = "2018/02/01 00:30:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_last_time = "2018/02/01 05:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
            if table == "BIDPEROFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
            if table == "BIDDAYOFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
                expected_length = 2
                expected_last_time = "2018/02/01 00:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_first_time = "2018/01/31 00:00:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
            print(table)
            data = data_fetch_methods.dynamic_data_compiler(
                start_time,
                end_time,
                table,
                defaults.raw_data_cache,
                select_columns=cols,
                filter_cols=filter_cols,
                filter_values=self.filter_values[table_type],
                fformat="feather",
            )
            data = data.reset_index(drop=True)
            print(table)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print("Passed")

    def test_dispatch_tables_start_of_month_parquet_format(self):
        start_time = "2018/02/01 00:00:00"
        end_time = "2018/02/01 05:15:00"
        for table in self.table_names:
            print("Testing {} returing values at start of month.".format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 63
            expected_number_of_columns = 2
            expected_first_time = pd.to_datetime(
                start_time, format="%Y/%m/%d %H:%M:%S"
            ) + timedelta(minutes=5)
            expected_last_time = pd.to_datetime(end_time, format="%Y/%m/%d %H:%M:%S")
            if table in [
                "TRADINGLOAD",
                "TRADINGPRICE",
                "TRADINGREGIONSUM",
                "TRADINGINTERCONNECT",
            ]:
                expected_length = 10
                expected_first_time = "2018/02/01 00:30:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_last_time = "2018/02/01 05:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
            if table == "BIDPEROFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
            if table == "BIDDAYOFFER_D":
                cols = [dat_col, "DUID", "BIDTYPE"]
                filter_cols = ("DUID", "BIDTYPE")
                expected_number_of_columns = 3
                expected_length = 2
                expected_last_time = "2018/02/01 00:00:00"
                expected_last_time = pd.to_datetime(
                    expected_last_time, format="%Y/%m/%d %H:%M:%S"
                )
                expected_first_time = "2018/01/31 00:00:00"
                expected_first_time = pd.to_datetime(
                    expected_first_time, format="%Y/%m/%d %H:%M:%S"
                )
            data = data_fetch_methods.dynamic_data_compiler(
                start_time,
                end_time,
                table,
                defaults.raw_data_cache,
                select_columns=cols,
                filter_cols=filter_cols,
                filter_values=self.filter_values[table_type],
                fformat="parquet",
                parse_data_types=True,
            )
            data = data.reset_index(drop=True)
            print(table)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            self.assertFalse(all(object == data.dtypes))
            print("Passed")
