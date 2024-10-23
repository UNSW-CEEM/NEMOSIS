import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime
from src.nemosis import defaults, query_wrappers


class TestDispatchDateSetup(unittest.TestCase):
    def setUp(self):
        self.start_time = "2017/01/01 00:10:00"
        self.end_time = "2019/06/03 12:15:00"

    def test_start_time_and_end_time(self):
        start_time, end_time = query_wrappers.dispatch_date_setup(
            self.start_time, self.end_time
        )
        self.assertEqual(start_time, "2016/12/30 23:59:59")
        self.assertEqual(end_time, "2019/06/03 00:00:00")


class TestFCASFinalise(unittest.TestCase):
    def setUp(self):
        self.testDataFrame1 = pd.DataFrame(
            {"StingsWithSpaces": [" HELLO ", "foo and baa ", " at front"]}
        )
        self.testDataFrame2 = pd.DataFrame(
            {
                "StingsWithSpaces": [" HELLO ", "foo and baa ", " at front"],
                "INTs2ignore": [1, 2, 3],
            }
        )
        self.testDataFrame3 = pd.DataFrame(
            {
                "StingsWithSpaces": [" HELLO ", "foo and baa ", " at front"],
                "INTs2ignore": [1, 2, 3],
                "StingsWithSpaces2": [" HELLO ", "foo and baa ", " at front"],
            }
        )

    def test_string_cleanup_one_col(self):
        result = query_wrappers.fcas4s_finalise(self.testDataFrame1, None, None)
        aim = pd.DataFrame({"StingsWithSpaces": ["HELLO", "foo and baa", "at front"]})
        assert_frame_equal(aim, result)

    def test_string_cleanup_one_col_ignore_one_col(self):
        result = query_wrappers.fcas4s_finalise(self.testDataFrame2, None, None)
        aim = pd.DataFrame(
            {
                "StingsWithSpaces": ["HELLO", "foo and baa", "at front"],
                "INTs2ignore": [1, 2, 3],
            }
        )
        assert_frame_equal(aim, result)

    def test_cleanup_ignore_cleanup(self):
        result = query_wrappers.fcas4s_finalise(self.testDataFrame3, None, None)
        aim = pd.DataFrame(
            {
                "StingsWithSpaces": ["HELLO", "foo and baa", "at front"],
                "INTs2ignore": [1, 2, 3],
                "StingsWithSpaces2": ["HELLO", "foo and baa", "at front"],
            }
        )
        assert_frame_equal(aim, result)


class TestMostRecent(unittest.TestCase):
    def setUp(self):
        self.dummyGenConData = pd.DataFrame(
            {
                "EFFECTIVEDATE": [
                    "2017/01/01 00:00:00",
                    "2017/01/04 00:15:00",
                    "2018/05/01 00:00:00",
                ],
                "VERSIONNO": ["5", "1", "1"],
                "GENCONID": ["ID1", "ID1", "ID1"],
            }
        )
        self.dummyGenConData["EFFECTIVEDATE"] = pd.to_datetime(
            self.dummyGenConData["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )

        self.dummyGenConData2 = pd.DataFrame(
            {
                "EFFECTIVEDATE": [
                    "2017/01/01 00:00:00",
                    "2017/01/04 00:15:00",
                    "2017/01/04 00:15:00",
                ],
                "VERSIONNO": ["5", "1", "2"],
                "GENCONID": ["ID1", "ID1", "ID1"],
            }
        )
        self.dummyGenConData2["EFFECTIVEDATE"] = pd.to_datetime(
            self.dummyGenConData2["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )

        self.dummyGenConData3 = pd.DataFrame(
            {
                "EFFECTIVEDATE": [
                    "2017/01/01 00:00:00",
                    "2017/01/04 00:15:00",
                    "2017/01/04 00:15:00",
                    "2017/01/01 00:00:00",
                    "2017/01/04 00:15:00",
                    "2017/01/04 00:15:00",
                ],
                "VERSIONNO": ["5", "1", "2", "5", "1", "2"],
                "GENCONID": ["ID1", "ID1", "ID1", "ID2", "ID2", "ID2"],
            }
        )
        self.dummyGenConData3["EFFECTIVEDATE"] = pd.to_datetime(
            self.dummyGenConData3["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )

    def test_one_id_gencondata_start_date_after_all(self):
        start_time = datetime.strptime("2019/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        table_name = "dummy"
        defaults.primary_date_columns["dummy"] = "EFFECTIVEDATE"
        defaults.effective_date_group_col["dummy"] = ["GENCONID"]
        result = query_wrappers.most_recent_records_before_start_time(
            self.dummyGenConData, start_time, table_name
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {
                "EFFECTIVEDATE": ["2018/05/01 00:00:00"],
                "VERSIONNO": ["1"],
                "GENCONID": ["ID1"],
            }
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_one_id_gencondata_start_date_after_first(self):
        start_time = datetime.strptime("2017/01/01 01:00:00", "%Y/%m/%d %H:%M:%S")
        table_name = "dummy"
        defaults.primary_date_columns["dummy"] = "EFFECTIVEDATE"
        defaults.effective_date_group_col["dummy"] = ["GENCONID"]
        result = (
            query_wrappers.most_recent_records_before_start_time(
                self.dummyGenConData, start_time, table_name
            )
            .sort_values("EFFECTIVEDATE", ascending=False)
            .reset_index(drop=True)
        )
        aim = (
            pd.DataFrame(
                {
                    "EFFECTIVEDATE": [
                        "2017/01/01 00:00:00",
                        "2017/01/04 00:15:00",
                        "2018/05/01 00:00:00",
                    ],
                    "VERSIONNO": ["5", "1", "1"],
                    "GENCONID": ["ID1", "ID1", "ID1"],
                }
            )
            .sort_values("EFFECTIVEDATE", ascending=False)
            .reset_index(drop=True)
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_one_id_gencondata_start_date_before_all(self):
        start_time = datetime.strptime("2010/01/01 01:00:00", "%Y/%m/%d %H:%M:%S")
        table_name = "dummy"
        defaults.primary_date_columns["dummy"] = "EFFECTIVEDATE"
        defaults.effective_date_group_col["dummy"] = ["GENCONID"]
        result = (
            query_wrappers.most_recent_records_before_start_time(
                self.dummyGenConData, start_time, table_name
            )
            .sort_values("EFFECTIVEDATE", ascending=False)
            .reset_index(drop=True)
        )
        aim = (
            pd.DataFrame(
                {
                    "EFFECTIVEDATE": [
                        "2017/01/01 00:00:00",
                        "2017/01/04 00:15:00",
                        "2018/05/01 00:00:00",
                    ],
                    "VERSIONNO": ["5", "1", "1"],
                    "GENCONID": ["ID1", "ID1", "ID1"],
                }
            )
            .sort_values("EFFECTIVEDATE", ascending=False)
            .reset_index(drop=True)
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_one_id_gencondata_and_repeated_effectivedate_start_date_after_all(self):
        start_time = datetime.strptime("2019/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        table_name = "dummy"
        defaults.primary_date_columns["dummy"] = "EFFECTIVEDATE"
        defaults.effective_date_group_col["dummy"] = ["GENCONID"]
        result = query_wrappers.most_recent_records_before_start_time(
            self.dummyGenConData2, start_time, table_name
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {
                "EFFECTIVEDATE": ["2017/01/04 00:15:00", "2017/01/04 00:15:00"],
                "VERSIONNO": ["1", "2"],
                "GENCONID": ["ID1", "ID1"],
            }
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)

    def test_2_id_and_repeated_effectivedate_start_date_after_all(self):
        start_time = datetime.strptime("2019/06/01 00:00:00", "%Y/%m/%d %H:%M:%S")
        table_name = "dummy"
        defaults.primary_date_columns["dummy"] = "EFFECTIVEDATE"
        defaults.effective_date_group_col["dummy"] = ["GENCONID"]
        result = query_wrappers.most_recent_records_before_start_time(
            self.dummyGenConData3, start_time, table_name
        ).reset_index(drop=True)
        aim = pd.DataFrame(
            {
                "EFFECTIVEDATE": [
                    "2017/01/04 00:15:00",
                    "2017/01/04 00:15:00",
                    "2017/01/04 00:15:00",
                    "2017/01/04 00:15:00",
                ],
                "VERSIONNO": ["1", "2", "1", "2"],
                "GENCONID": ["ID1", "ID1", "ID2", "ID2"],
            }
        )
        aim["EFFECTIVEDATE"] = pd.to_datetime(
            aim["EFFECTIVEDATE"], format="%Y/%m/%d %H:%M:%S"
        )
        assert_frame_equal(aim, result)
