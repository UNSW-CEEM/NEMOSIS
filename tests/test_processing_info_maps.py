import unittest

from src.nemosis import processing_info_maps, defaults, query_wrappers, data_fetch_methods
import pandas as pd
from datetime import datetime, timedelta


class TestSearchTypeValidity(unittest.TestCase):
    def setUp(self):
        self.time_yesterday = (datetime.now() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        pass

    def test_start_to_end_no_duplication_between_batches(self):
        for table_name in processing_info_maps.search_type.keys():
            if processing_info_maps.search_type[table_name] == "start_to_end":
                print("Validating start_to_end type for table {}".format(table_name))
                start_time = datetime.strptime(
                    "2018/06/01 00:00:00", "%Y/%m/%d %H:%M:%S"
                )
                end_time = datetime.strptime("2018/09/01 00:00:00", "%Y/%m/%d %H:%M:%S")
                if table_name in ["DAILY_REGION_SUMMARY", "NEXT_DAY_DISPATCHLOAD", "INTERMITTENT_GEN_SCADA"]:
                    end_time = self.time_yesterday
                    start_time = self.time_yesterday - timedelta(days=8)
                if table_name in ["FCAS_4_SECOND"]:
                    end_time = self.time_yesterday
                    start_time = self.time_yesterday - timedelta(hours=2)
                data_tables = data_fetch_methods._dynamic_data_fetch_loop(
                    start_search=start_time,
                    start_time=start_time,
                    end_time=end_time,
                    table_name=table_name,
                    raw_data_location=defaults.raw_data_cache,
                    select_columns=defaults.table_primary_keys[table_name],
                    date_filter=None,
                    keep_csv=False,
                )
                all_data = pd.concat(data_tables, sort=False)
                contains_duplicates = all_data.duplicated().any()
                self.assertEqual(
                    False, contains_duplicates, "table {}".format(table_name)
                )
                print("Type valid, no duplicates found.")

    def test_start_to_end_has_settlement_or_interval_col(self):
        for table_name in processing_info_maps.search_type.keys():
            if processing_info_maps.search_type[table_name] == "start_to_end":
                has_settlement_date_col = (
                    "SETTLEMENTDATE" in defaults.table_columns[table_name]
                )
                has_interval_datetime_col = (
                    "INTERVAL_DATETIME" in defaults.table_columns[table_name]
                )
                has_interval_timestamp_col = (
                    "TIMESTAMP" in defaults.table_columns[table_name]
                )
                has_run_datetime_col = (
                    "RUN_DATETIME" in defaults.table_columns[table_name]
                )
                has_either = (
                    has_interval_datetime_col
                    or has_settlement_date_col
                    or has_interval_timestamp_col
                    or has_run_datetime_col
                )
                self.assertEqual(True, has_either)
                print(
                    "{} is valid candidate for type start_to_end as there is a SETTLEMENTDATE, "
                    "INTERVAL_DATETIME or TIMESTAMP column to filter on.".format(
                        table_name
                    )
                )

    def test_all_no_duplication_between_batches(self):
        for table_name in processing_info_maps.search_type.keys():
            if processing_info_maps.search_type[table_name] == "all":
                print("Validating all type for table {}".format(table_name))
                if table_name in [
                    "GENCONDATA",
                    "SPDCONNECTIONPOINTCONSTRAINT",
                    "SPDINTERCONNECTORCONSTRAINT",
                    "DUDETAILSUMMARY",
                    "LOSSMODEL",
                    "LOSSFACTORMODEL",
                    "MNSP_DAYOFFER",
                    "MNSP_PEROFFER",
                    "MNSP_INTERCONNECTOR",
                    "INTERCONNECTOR",
                    "INTERCONNECTORCONSTRAINT",
                    "DUDETAIL",
                    "MARKET_PRICE_THRESHOLDS",
                    "PARTICIPANT"
                ]:
                    print(
                        "{} is known to contain duplicate entries and is exempted from this test, a finalise "
                        "data processing step is included in dynamic data fetch to clean up these duplicates.".format(
                            table_name
                        )
                    )
                    continue
                start_test_window = defaults.nem_data_model_start_time
                start_time = datetime.strptime(start_test_window, "%Y/%m/%d %H:%M:%S")
                end_time = datetime.strptime("2018/01/01 00:00:00", "%Y/%m/%d %H:%M:%S")
                start_search = datetime.strptime(start_test_window, "%Y/%m/%d %H:%M:%S")
                data_tables = data_fetch_methods._dynamic_data_fetch_loop(
                    start_search=start_search,
                    start_time=start_time,
                    end_time=end_time,
                    table_name=table_name,
                    raw_data_location=defaults.raw_data_cache,
                    select_columns=defaults.table_primary_keys[table_name],
                    date_filter=None,
                    keep_csv=False,
                )
                all_data = pd.concat(data_tables, sort=False)
                contains_duplicates = all_data.duplicated().any()
                self.assertEqual(
                    False, contains_duplicates, "table {}".format(table_name)
                )
                print("Type valid, no duplicates found.")

    def test_all_no_duplication_between_batches_with_finalise_step(self):
        for table_name in processing_info_maps.search_type.keys():
            if processing_info_maps.search_type[table_name] == "all":
                print("Testing duplicate removal for table {}".format(table_name))
                start_test_window = defaults.nem_data_model_start_time
                # start_test_window = '2018/01/01 00:00:00'
                start_time = datetime.strptime(start_test_window, "%Y/%m/%d %H:%M:%S")
                end_time = datetime.strptime("2018/01/01 00:00:00", "%Y/%m/%d %H:%M:%S")
                start_search = datetime.strptime(start_test_window, "%Y/%m/%d %H:%M:%S")
                data_tables = data_fetch_methods._dynamic_data_fetch_loop(
                    start_search=start_search,
                    start_time=start_time,
                    end_time=end_time,
                    table_name=table_name,
                    raw_data_location=defaults.raw_data_cache,
                    select_columns=defaults.table_primary_keys[table_name],
                    date_filter=None,
                    keep_csv=False,
                )
                all_data = pd.concat(data_tables, sort=False)
                all_data = query_wrappers.drop_duplicates_by_primary_key(
                    all_data, start_time, table_name
                )
                contains_duplicates = all_data.duplicated().any()
                self.assertEqual(False, contains_duplicates)
                print("Type valid, no duplicates found.")

    def test_start_to_end_has_no_settlement_interval_or_timestamp_col(self):
        for table_name in processing_info_maps.search_type.keys():
            if processing_info_maps.search_type[table_name] == "all":
                has_settlement_date_col = (
                    "SETTLEMENTDATE" in defaults.table_columns[table_name]
                )
                has_interval_datetime_col = (
                    "INTERVAL_DATETIME" in defaults.table_columns[table_name]
                )
                has_interval_timestamp_col = (
                    "TIMESTAMP" in defaults.table_columns[table_name]
                )
                has_either = (
                    has_interval_datetime_col
                    or has_settlement_date_col
                    or has_interval_timestamp_col
                )
                self.assertEqual(False, has_either, "table {}".format(table_name))
                print(
                    "{} is valid candidate for type all as there is not a SETTLEMENTDATE, "
                    "INTERVAL_DATETIME or TIMESTAMP column to filter on".format(
                        table_name
                    )
                )

    def test_last_contains_data_from_first(self):
        for table_name in processing_info_maps.search_type.keys():
            if processing_info_maps.search_type[table_name] == "end":
                start_test_window = defaults.nem_data_model_start_time
                # start_test_window = '2018/01/01 00:00:00'
                start_time = datetime.strptime(start_test_window, "%Y/%m/%d %H:%M:%S")
                end_time = datetime.strptime("2018/01/01 00:00:00", "%Y/%m/%d %H:%M:%S")
                start_search = datetime.strptime(start_test_window, "%Y/%m/%d %H:%M:%S")
                select_columns = None
                (
                    _,
                    _,
                    select_columns,
                    _,
                    _,
                ) = data_fetch_methods._set_up_dynamic_compilers(
                    table_name, start_time, end_time, select_columns
                )
                data_tables = data_fetch_methods._dynamic_data_fetch_loop(
                    start_search=start_search,
                    start_time=start_time,
                    end_time=end_time,
                    table_name=table_name,
                    raw_data_location=defaults.raw_data_cache,
                    select_columns=select_columns,
                    date_filter=None,
                    keep_csv=False,
                )
                first_data_table = data_tables[35].loc[
                    :, defaults.table_primary_keys[table_name]
                                   ]
                last_data_table = data_tables[-1]
                comp = pd.merge(
                    first_data_table,
                    last_data_table,
                    "left",
                    defaults.table_primary_keys[table_name],
                )
                non_primary_col = [
                    col
                    for col in defaults.table_columns[table_name]
                    if col not in defaults.table_primary_keys[table_name]
                ][0]
                missing_from_last = comp[comp[non_primary_col].isnull()]
                self.assertEqual(False, missing_from_last.empty)
