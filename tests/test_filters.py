import unittest
import pandas as pd
from src.nemosis import filters
from pandas.testing import assert_frame_equal
from datetime import datetime
import numpy as np


class TestFiltersStartDate(unittest.TestCase):
    def setUp(self):
        self.start_date_data = pd.DataFrame(
            {
                "START_DATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"],
                "END_DATE": ["2015/01/01 00:00:00", "2015/07/01 00:12:00"],
            }
        )

    def test_start_date_pick_first_of_two(self):
        start_time = datetime.strptime("2014/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2014/09/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_start_and_end_date(
            self.start_date_data, start_time=start_time, end_time=end_time
        )
        aim = pd.DataFrame(
            {"START_DATE": ["2011/01/01 00:00:00"], "END_DATE": ["2015/01/01 00:00:00"]}
        )
        aim["START_DATE"] = pd.to_datetime(
            aim["START_DATE"], format="%Y/%m/%d %H:%M:%S"
        )
        aim["END_DATE"] = pd.to_datetime(aim["END_DATE"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_start_date_pick_second_of_two(self):
        start_time = datetime.strptime("2015/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_start_and_end_date(
            self.start_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {"START_DATE": ["2015/01/01 00:00:00"], "END_DATE": ["2015/07/01 00:12:00"]}
        )
        aim["START_DATE"] = pd.to_datetime(
            aim["START_DATE"], format="%Y/%m/%d %H:%M:%S"
        )
        aim["END_DATE"] = pd.to_datetime(aim["END_DATE"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_start_date_pick_two_of_two_by_overlaping_interval(self):
        start_time = datetime.strptime("2011/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_start_and_end_date(
            self.start_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {
                "START_DATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"],
                "END_DATE": ["2015/01/01 00:00:00", "2015/07/01 00:12:00"],
            }
        )
        aim["START_DATE"] = pd.to_datetime(
            aim["START_DATE"], format="%Y/%m/%d %H:%M:%S"
        )
        aim["END_DATE"] = pd.to_datetime(aim["END_DATE"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_start_date_pick_none_of_two_by_overshooting_date(self):
        start_time = datetime.strptime("2018/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2019/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_start_and_end_date(
            self.start_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"START_DATE": [], "END_DATE": []})
        aim["START_DATE"] = pd.to_datetime(
            aim["START_DATE"], format="%Y/%m/%d %H:%M:%S"
        )
        aim["END_DATE"] = pd.to_datetime(aim["END_DATE"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_start_date_pick_none_of_two_by_undershooting_date(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_start_and_end_date(
            self.start_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"START_DATE": [], "END_DATE": []})
        aim["START_DATE"] = pd.to_datetime(
            aim["START_DATE"], format="%Y/%m/%d %H:%M:%S"
        )
        aim["END_DATE"] = pd.to_datetime(aim["END_DATE"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)


class TestFiltersEffectiveDate(unittest.TestCase):
    def setUp(self):
        self.last_changed_data = pd.DataFrame(
            {"EFFECTIVEDATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )

    def test_start_date_pick_first_of_two(self):
        start_time = datetime.strptime("2014/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2014/09/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_effective_date(
            self.last_changed_data, start_time=start_time, end_time=end_time
        )
        aim = pd.DataFrame({"EFFECTIVEDATE": ["2011/01/01 00:00:00"]})
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_start_date_two_of_two_with_date_window_on_second(self):
        start_time = datetime.strptime("2015/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_effective_date(
            self.last_changed_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {"EFFECTIVEDATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_start_date_pick_two_of_two_by_overlaping_interval(self):
        start_time = datetime.strptime("2011/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_effective_date(
            self.last_changed_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {"EFFECTIVEDATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_start_date_pick_two_of_two_by_overshooting_date(self):
        start_time = datetime.strptime("2018/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2019/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_effective_date(
            self.last_changed_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {"EFFECTIVEDATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_start_date_pick_none_of_two_by_undershooting_date(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_effective_date(
            self.last_changed_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"EFFECTIVEDATE": []})
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)


class TestFiltersSettlementDate(unittest.TestCase):
    def setUp(self):
        self.settlement_date_data = pd.DataFrame(
            {"SETTLEMENTDATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )

    def test_settlement_date_pick_first_of_two(self):
        start_time = datetime.strptime("2010/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2011/09/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_settlementdate(
            self.settlement_date_data, start_time=start_time, end_time=end_time
        )
        aim = pd.DataFrame({"SETTLEMENTDATE": ["2011/01/01 00:00:00"]})
        aim["SETTLEMENTDATE"] = pd.to_datetime(
            aim["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_settlement_date_pick_second_of_two(self):
        start_time = datetime.strptime("2014/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_settlementdate(
            self.settlement_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"SETTLEMENTDATE": ["2015/01/01 00:00:00"]})
        aim["SETTLEMENTDATE"] = pd.to_datetime(
            aim["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_settlement_date_pick_two_of_two_by_overlaping_interval(self):
        start_time = datetime.strptime("2010/12/31 23:59:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        aim = pd.DataFrame(
            {"SETTLEMENTDATE": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )
        result = filters.filter_on_settlementdate(
            self.settlement_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim["SETTLEMENTDATE"] = pd.to_datetime(
            aim["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_settlement_date_pick_none_of_two_by_overshooting_date(self):
        start_time = datetime.strptime("2018/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2019/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_settlementdate(
            self.settlement_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"SETTLEMENTDATE": []})
        aim["SETTLEMENTDATE"] = pd.to_datetime(
            aim["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_settlement_date_pick_none_of_two_by_undershooting_date(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_settlementdate(
            self.settlement_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"SETTLEMENTDATE": []})
        aim["SETTLEMENTDATE"] = pd.to_datetime(
            aim["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_settlement_date_end_date_exclusive_by_undershooting(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/12/31 23:59:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_settlementdate(
            self.settlement_date_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"SETTLEMENTDATE": []})
        aim["SETTLEMENTDATE"] = pd.to_datetime(
            aim["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)


class TestFiltersTimeStamp(unittest.TestCase):
    def setUp(self):
        self.time_stamp_data = pd.DataFrame(
            {"TIMESTAMP": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )

    def test_time_stamp_pick_first_of_two(self):
        start_time = datetime.strptime("2010/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2011/09/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_timestamp(
            self.time_stamp_data, start_time=start_time, end_time=end_time
        )
        aim = pd.DataFrame({"TIMESTAMP": ["2011/01/01 00:00:00"]})
        aim["TIMESTAMP"] = pd.to_datetime(aim["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_time_stamp_pick_second_of_two(self):
        start_time = datetime.strptime("2014/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_timestamp(
            self.time_stamp_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"TIMESTAMP": ["2015/01/01 00:00:00"]})
        aim["TIMESTAMP"] = pd.to_datetime(aim["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_time_stamp_pick_two_of_two_by_overlaping_interval(self):
        start_time = datetime.strptime("2010/12/31 23:59:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        aim = pd.DataFrame(
            {"TIMESTAMP": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )
        result = filters.filter_on_timestamp(
            self.time_stamp_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim["TIMESTAMP"] = pd.to_datetime(aim["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_time_stamp_pick_none_of_two_by_overshooting_date(self):
        start_time = datetime.strptime("2018/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2019/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_timestamp(
            self.time_stamp_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"TIMESTAMP": []})
        aim["TIMESTAMP"] = pd.to_datetime(aim["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_time_stamp_pick_none_of_two_by_undershooting_date(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_timestamp(
            self.time_stamp_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"TIMESTAMP": []})
        aim["TIMESTAMP"] = pd.to_datetime(aim["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)

    def test_time_stamp_end_date_exclusive_by_undershooting(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/12/31 23:59:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_timestamp(
            self.time_stamp_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"TIMESTAMP": []})
        aim["TIMESTAMP"] = pd.to_datetime(aim["TIMESTAMP"], format="%Y/%m/%d %H:%M:%S")
        assert_frame_equal(aim, result)


class TestFiltersIntervalDatetime(unittest.TestCase):
    def setUp(self):
        self.interval_datetime_data = pd.DataFrame(
            {"INTERVAL_DATETIME": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )

    def test_interval_datetime_pick_first_of_two(self):
        start_time = datetime.strptime("2010/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2011/09/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_interval_datetime(
            self.interval_datetime_data, start_time=start_time, end_time=end_time
        )
        aim = pd.DataFrame({"INTERVAL_DATETIME": ["2011/01/01 00:00:00"]})
        aim["INTERVAL_DATETIME"] = pd.to_datetime(
            aim["INTERVAL_DATETIME"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_interval_datetime_pick_second_of_two(self):
        start_time = datetime.strptime("2014/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_interval_datetime(
            self.interval_datetime_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTERVAL_DATETIME": ["2015/01/01 00:00:00"]})
        aim["INTERVAL_DATETIME"] = pd.to_datetime(
            aim["INTERVAL_DATETIME"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_interval_datetime_pick_two_of_two_by_overlaping_interval(self):
        start_time = datetime.strptime("2010/12/31 23:59:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2015/05/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        aim = pd.DataFrame(
            {"INTERVAL_DATETIME": ["2011/01/01 00:00:00", "2015/01/01 00:00:00"]}
        )
        result = filters.filter_on_interval_datetime(
            self.interval_datetime_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim["INTERVAL_DATETIME"] = pd.to_datetime(
            aim["INTERVAL_DATETIME"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_interval_datetime_pick_none_of_two_by_overshooting_date(self):
        start_time = datetime.strptime("2018/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2019/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_interval_datetime(
            self.interval_datetime_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTERVAL_DATETIME": []})
        aim["INTERVAL_DATETIME"] = pd.to_datetime(
            aim["INTERVAL_DATETIME"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_interval_datetime_stamp_pick_none_of_two_by_undershooting_date(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/05/01 00:13:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_interval_datetime(
            self.interval_datetime_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTERVAL_DATETIME": []})
        aim["INTERVAL_DATETIME"] = pd.to_datetime(
            aim["INTERVAL_DATETIME"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_interval_datetime_end_date_exclusive_by_undershooting(self):
        start_time = datetime.strptime("2010/04/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        end_time = datetime.strptime("2010/12/31 23:59:00", "%Y/%m/%d %H:%M:%S")
        result = filters.filter_on_interval_datetime(
            self.interval_datetime_data, start_time=start_time, end_time=end_time
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTERVAL_DATETIME": []})
        aim["INTERVAL_DATETIME"] = pd.to_datetime(
            aim["INTERVAL_DATETIME"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)


class TestFiltersColumnValue(unittest.TestCase):
    def setUp(self):
        self.int_filter_data = pd.DataFrame({"INTCOL": [1, 10, 100, -5, 0, 10, 10]})
        self.int_and_string_filter_data = pd.DataFrame(
            {
                "INTCOL": [1, 10, 100, -5, 0, 10, 10],
                "STRINGCOL": ["1", "10", "100", "-5", "0", "10", "10"],
            }
        )

    def test_filter_one_col_one_value_positive_ints(self):
        filter_cols = ("INTCOL",)
        filter_values = ([10],)
        result = filters.filter_on_column_value(
            self.int_filter_data, filter_cols, filter_values
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTCOL": [10, 10, 10]})
        assert_frame_equal(aim, result)

    def test_filter_one_col_two_values_positive_and_negative_ints(self):
        filter_cols = ("INTCOL",)
        filter_values = ([10, -5],)
        result = filters.filter_on_column_value(
            self.int_filter_data, filter_cols, filter_values
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTCOL": [10, -5, 10, 10]})
        assert_frame_equal(aim, result)

    def test_filter_two_cols_one_value_each_not_matching(self):
        filter_cols = ("INTCOL", "STRINGCOL")
        filter_values = ([10], ["100"])
        result = filters.filter_on_column_value(
            self.int_and_string_filter_data, filter_cols, filter_values
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTCOL": [], "STRINGCOL": []})
        aim = aim.astype(dtype={"INTCOL": np.int64, "STRINGCOL": str})
        assert_frame_equal(aim, result)

    def test_filter_two_cols_one_value_each_matching(self):
        filter_cols = ("INTCOL", "STRINGCOL")
        filter_values = ([10], ["10"])
        result = filters.filter_on_column_value(
            self.int_and_string_filter_data, filter_cols, filter_values
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTCOL": [10, 10, 10], "STRINGCOL": ["10", "10", "10"]})
        assert_frame_equal(aim, result)

    def test_filter_just_one_of_two_cols(self):
        filter_cols = ("INTCOL",)
        filter_values = ([10],)
        result = filters.filter_on_column_value(
            self.int_and_string_filter_data, filter_cols, filter_values
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTCOL": [10, 10, 10], "STRINGCOL": ["10", "10", "10"]})
        assert_frame_equal(aim, result)

    def test_filter_one_empty_values_returns_empty_data_frame(self):
        filter_cols = ("INTCOL",)
        filter_values = ([],)
        result = filters.filter_on_column_value(
            self.int_and_string_filter_data, filter_cols, filter_values
        ).reset_index(drop=True)
        aim = pd.DataFrame({"INTCOL": [], "STRINGCOL": []})
        aim = aim.astype(dtype={"INTCOL": np.int64, "STRINGCOL": str})
        assert_frame_equal(aim, result)


if __name__ == "__main__":
    unittest.main()
