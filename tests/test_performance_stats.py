import unittest
import pandas as pd
from src.nemosis import custom_tables, defaults, data_fetch_methods
import math
import numpy as np
import time
from datetime import datetime, timedelta
import os


class TestBaseVolumeWeightAveragePriceFunction(unittest.TestCase):
    def setUp(self):
        self.volume = pd.Series([55, 0, math.nan, 60, 40])
        self.price = pd.Series([88, 90, -100, -100, 50])
        self.pricing_data = pd.DataFrame(
            {
                "SCADAVALUE": [8, 9, 50, 11, 10, 0],
                "TRADING_TOTALCLEARED": [
                    20,
                    math.nan,
                    math.nan,
                    80,
                    math.nan,
                    math.nan,
                ],
                "TRADING_RRP": [80, math.nan, math.nan, 100, math.nan, math.nan],
                "DISPATCH_RRP": [80, 90, 89, 111, 110, 75],
            }
        )
        pass

    def test_volume_weighted_average_price(self):
        vwap = custom_tables.volume_weighted_average_price(self.volume, self.price)
        self.assertAlmostEqual(vwap, 5.419354839, 4)

    def test_trading_price(self):
        vwap = custom_tables.volume_weighted_average_trading_price(self.pricing_data)
        self.assertAlmostEqual(vwap, 96, 4)

    def test_spot_price(self):
        vwap = custom_tables.volume_weighted_average_spot_price(self.pricing_data)
        self.assertAlmostEqual(vwap, 93.42045455, 4)


class TestPerformanceAtNodalPeak(unittest.TestCase):
    def setUp(self):
        self.cap_and_output = pd.DataFrame(
            {
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:10:00",
                    "2015/01/01 12:00:00",
                    "2015/01/01 12:10:00",
                    "2015/01/01 22:10:00",
                ],
                "DUID": ["A", "A", "A", "A", "A", "A"],
                "MAXCAPACITY": [68, 68, 68, 68, 68, 68],
                "SCADAVALUE": [8, 9, 50, 11, 10, 0],
                "TOTALDEMAND": [1000, 1100, 13000, 900, 800, 1000],
            }
        )
        self.cap_and_output["SETTLEMENTDATE"] = pd.to_datetime(
            self.cap_and_output["SETTLEMENTDATE"]
        )
        self.cap_and_output_nans = pd.DataFrame(
            {
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:10:00",
                    "2015/01/01 12:00:00",
                    "2015/01/01 12:10:00",
                    "2015/01/01 22:10:00",
                ],
                "DUID": ["A", "A", "A", "A", "A", "A"],
                "MAXCAPACITY": [68, 68, 68, 68, 68, 68],
                "SCADAVALUE": [8, 9, math.nan, 11, 10, 0],
                "TOTALDEMAND": [1000, 1100, 13000, 900, 800, 1000],
            }
        )
        self.cap_and_output_nans["SETTLEMENTDATE"] = pd.to_datetime(
            self.cap_and_output["SETTLEMENTDATE"]
        )

        pass

    def test_performance_at_nodal_peak(self):
        peak = custom_tables.performance_at_nodal_peak(self.cap_and_output)
        self.assertAlmostEqual(peak, 50 / 68, 4)

    def test_performance_at_nodal_peak_nans(self):
        peak = custom_tables.performance_at_nodal_peak(self.cap_and_output_nans)
        self.assertAlmostEqual(peak, 0.0, 4)


class TestCapacityFactor90(unittest.TestCase):
    def setUp(self):
        self.cap_and_output = pd.DataFrame(
            {
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:10:00",
                    "2015/01/01 12:00:00",
                    "2015/01/01 12:10:00",
                    "2015/01/01 22:10:00",
                ],
                "DUID": ["A", "A", "A", "A", "A", "A"],
                "MAXCAPACITY": [68, 68, 68, 68, 68, 68],
                "SCADAVALUE": [8, 9, 50, 11, 10, 0],
                "TOTALDEMAND": [1000, 1100, 13000, 900, 800, 1000],
            }
        )
        self.cap_and_output["SETTLEMENTDATE"] = pd.to_datetime(
            self.cap_and_output["SETTLEMENTDATE"]
        )
        self.cap_and_output2 = pd.DataFrame(
            {
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:10:00",
                    "2015/01/01 12:00:00",
                    "2015/01/01 12:10:00",
                    "2015/01/01 22:10:00",
                    "2015/01/02 00:00:00",
                    "2015/01/02 00:05:00",
                    "2015/01/02 00:10:00",
                    "2015/01/02 12:00:00",
                    "2015/01/02 12:10:00",
                    "2015/01/02 22:10:00",
                ],
                "DUID": ["A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"],
                "MAXCAPACITY": [68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68],
                "SCADAVALUE": [8, 9, 50, 11, 10, 0, 8, 9, 50, 11, 10, 90],
                "TOTALDEMAND": [
                    10000,
                    11000,
                    13000,
                    900,
                    800,
                    1000,
                    1000,
                    11000,
                    13000,
                    900,
                    800,
                    18000,
                ],
            }
        )
        self.cap_and_output2["SETTLEMENTDATE"] = pd.to_datetime(
            self.cap_and_output2["SETTLEMENTDATE"]
        )
        self.cap_and_output2_nans = pd.DataFrame(
            {
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:10:00",
                    "2015/01/01 12:00:00",
                    "2015/01/01 12:10:00",
                    "2015/01/01 22:10:00",
                    "2015/01/02 00:00:00",
                    "2015/01/02 00:05:00",
                    "2015/01/02 00:10:00",
                    "2015/01/02 12:00:00",
                    "2015/01/02 12:10:00",
                    "2015/01/02 22:10:00",
                ],
                "DUID": ["A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"],
                "MAXCAPACITY": [68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68],
                "SCADAVALUE": [8, 9, 50, 11, 10, 0, 8, 9, 50, 11, 10, math.nan],
                "TOTALDEMAND": [
                    10000,
                    11000,
                    13000,
                    900,
                    800,
                    1000,
                    1000,
                    11000,
                    13000,
                    900,
                    800,
                    18000,
                ],
            }
        )
        self.cap_and_output2_nans["SETTLEMENTDATE"] = pd.to_datetime(
            self.cap_and_output2["SETTLEMENTDATE"]
        )
        self.cap_and_output2_nans2 = pd.DataFrame(
            {
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:10:00",
                    "2015/01/01 12:00:00",
                    "2015/01/01 12:10:00",
                    "2015/01/01 22:10:00",
                    "2015/01/02 00:00:00",
                    "2015/01/02 00:05:00",
                    "2015/01/02 00:10:00",
                    "2015/01/02 12:00:00",
                    "2015/01/02 12:10:00",
                    "2015/01/02 22:10:00",
                ],
                "DUID": ["A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"],
                "MAXCAPACITY": [
                    math.nan,
                    math.nan,
                    math.nan,
                    68,
                    68,
                    68,
                    68,
                    68,
                    68,
                    68,
                    68,
                    68,
                ],
                "SCADAVALUE": [
                    math.nan,
                    math.nan,
                    math.nan,
                    11,
                    10,
                    0,
                    8,
                    9,
                    50,
                    11,
                    10,
                    90,
                ],
                "TOTALDEMAND": [
                    10000,
                    11000,
                    13000,
                    900,
                    800,
                    1000,
                    1000,
                    11000,
                    13000,
                    900,
                    800,
                    18000,
                ],
            }
        )
        self.cap_and_output2_nans2["SETTLEMENTDATE"] = pd.to_datetime(
            self.cap_and_output2["SETTLEMENTDATE"]
        )
        pass

    def test_capacity_factor(self):
        peak = custom_tables.capacity_factor_over_90th_percentile_of_nodal_demand(
            self.cap_and_output
        )
        self.assertAlmostEqual(peak, 50 / 68, 4)

    def test_capacity_factor_2_intervals(self):
        peak = custom_tables.capacity_factor_over_90th_percentile_of_nodal_demand(
            self.cap_and_output2
        )
        self.assertAlmostEqual(peak, (50 / 68 + 90 / 68) / 2, 4)

    def test_capacity_factor_2_intervals_one_nan(self):
        peak = custom_tables.capacity_factor_over_90th_percentile_of_nodal_demand(
            self.cap_and_output2_nans
        )
        self.assertAlmostEqual(peak, (50 / 68 + 0.0 / 68) / 2, 4)

    def test_capacity_factor_2_intervals_plant_built_after_first(self):
        peak = custom_tables.capacity_factor_over_90th_percentile_of_nodal_demand(
            self.cap_and_output2_nans2
        )
        self.assertAlmostEqual(peak, (90 / 68), 4)


class TestCapacityScadaBasedStats(unittest.TestCase):
    def setUp(self):
        self.cap_and_output = pd.DataFrame(
            {"MAXCAPACITY": [68, 68, 68, 68, 68], "SCADAVALUE": [8, 9, 50, 11, 10]}
        )
        pass

    def test_capacity_factor(self):
        cf = custom_tables.capacity_factor(self.cap_and_output)
        self.assertAlmostEqual(cf, 0.258823529, 4)

    def test_volume(self):
        v = custom_tables.volume(self.cap_and_output)
        self.assertAlmostEqual(v, 7.3333, 4)


class TestMonthlyGroupingForStats(unittest.TestCase):
    def setUp(self):
        self.cap_and_output = pd.DataFrame(
            {
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:10:00",
                    "2015/02/01 00:00:00",
                    "2015/02/01 00:05:00",
                    "2015/02/01 00:10:00",
                ],
                "DUID": ["A", "A", "A", "A", "A", "A"],
                "MAXCAPACITY": [68, 68, 68, 68, 68, 68],
                "SCADAVALUE": [8, 9, 50, 11, 10, 0],
                "TRADING_TOTALCLEARED": [
                    111,
                    math.nan,
                    math.nan,
                    250,
                    math.nan,
                    math.nan,
                ],
                "TRADING_RRP": [115, math.nan, math.nan, 250, math.nan, math.nan],
                "DISPATCH_RRP": [112, 97, 102, 81, 85, 91],
                "TOTALDEMAND": [1000, 1020, 1100, 990, 1100, 897],
            }
        )
        self.cap_and_output["SETTLEMENTDATE"] = pd.to_datetime(
            self.cap_and_output["SETTLEMENTDATE"]
        )
        self.cf_by_month = pd.DataFrame(
            {
                "Month": ["2015-01", "2015-02"],
                "DUID": ["A", "A"],
                "CapacityFactor": [0.328431373, 0.102941176],
                "Volume": [67.0 / 12, 21.0 / 12],
                "TRADING_VWAP": [115.0, 250.0],
                "DISPATCH_VWAP": [102.5223881, 82.9047619],
                "NodalPeakCapacityFactor": [0.735294118, 0.147058824],
                "Nodal90thPercentileCapacityFactor": [0.735294118, 0.147058824],
            }
        )
        pass

    def test_one_duid_two_months_example(self):
        cf_by_month_and_duid = custom_tables.stats_by_month_and_plant(
            self.cap_and_output
        )
        cf_by_month_and_duid = cf_by_month_and_duid.reset_index(drop=True)
        pd.testing.assert_frame_equal(cf_by_month_and_duid, self.cf_by_month)


class TestMergeTables(unittest.TestCase):
    def setUp(self):
        self.gen_info = pd.DataFrame(
            {
                "EFFECTIVEDATE": [
                    "2014/01/01 00:00:00",
                    "2017/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                ],
                "DUID": ["A", "A", "B"],
                "MAXCAPACITY": [333, 400, 250],
            }
        )
        self.gen_info["EFFECTIVEDATE"] = pd.to_datetime(self.gen_info["EFFECTIVEDATE"])
        self.gen_info2 = pd.DataFrame(
            {
                "START_DATE": [
                    "2014/01/01 00:00:00",
                    "2017/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                ],
                "DUID": ["A", "A", "B"],
                "REGIONID": ["NSW1", "NSW1", "VIC1"],
            }
        )
        self.gen_info2["START_DATE"] = pd.to_datetime(self.gen_info2["START_DATE"])
        self.scada = pd.DataFrame(
            {
                "DUID": ["A", "A", "B", "B"],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
                "SCADAVALUE": [150, 150, 200, 220],
            }
        )
        self.scada["SETTLEMENTDATE"] = pd.to_datetime(self.scada["SETTLEMENTDATE"])
        self.trading_load = pd.DataFrame(
            {
                "DUID": ["A", "B"],
                "SETTLEMENTDATE": ["2015/01/01 00:00:00", "2015/01/01 00:00:00"],
                "TOTALCLEARED": [150, 200],
            }
        )
        self.trading_load["SETTLEMENTDATE"] = pd.to_datetime(
            self.trading_load["SETTLEMENTDATE"]
        )
        self.dispatch_price = pd.DataFrame(
            {
                "REGIONID": ["NSW1", "NSW1", "VIC1", "VIC1"],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
                "RRP": [99, 110, 300, 500],
            }
        )
        self.dispatch_price["SETTLEMENTDATE"] = pd.to_datetime(
            self.dispatch_price["SETTLEMENTDATE"]
        )
        self.trading_price = pd.DataFrame(
            {
                "REGIONID": ["NSW1", "VIC1"],
                "SETTLEMENTDATE": ["2015/01/01 00:00:00", "2015/01/01 00:00:00"],
                "RRP": [99, 300],
            }
        )
        self.trading_price["SETTLEMENTDATE"] = pd.to_datetime(
            self.trading_price["SETTLEMENTDATE"]
        )
        self.region_summary = pd.DataFrame(
            {
                "REGIONID": ["NSW1", "NSW1", "VIC1", "VIC1"],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
                "TOTALDEMAND": [5000, 5010, 8000, 8700],
            }
        )
        self.region_summary["SETTLEMENTDATE"] = pd.to_datetime(
            self.region_summary["SETTLEMENTDATE"]
        )

        self.expected_combined_df = pd.DataFrame(
            {
                "DUID": ["A", "A", "B", "B"],
                "DUDETAIL_EFFECTIVEDATE": [
                    "2014/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                ],
                "REGIONID": ["NSW1", "NSW1", "VIC1", "VIC1"],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
                "MAXCAPACITY": [333, 333, 250, 250],
                "SCADAVALUE": [150, 150, 200, 220],
                "TRADING_TOTALCLEARED": [150, math.nan, 200, math.nan],
                "DISPATCH_RRP": [99, 110, 300, 500],
                "TRADING_RRP": [99, math.nan, 300, math.nan],
                "TOTALDEMAND": [5000, 5010, 8000, 8700],
            }
        )
        ix = pd.date_range(
            start=datetime.strptime("2015/01/01 00:00:00", "%Y/%m/%d %H:%M:%S"),
            end=datetime.strptime("2015/01/01 00:10:00", "%Y/%m/%d %H:%M:%S")
            - timedelta(minutes=5),
            freq="5T",
        )
        self.timeseries_df = pd.DataFrame(index=ix)
        self.timeseries_df.reset_index(inplace=True)
        self.timeseries_df.columns = ["SETTLEMENTDATE"]
        self.expected_combined_df["SETTLEMENTDATE"] = pd.to_datetime(
            self.expected_combined_df["SETTLEMENTDATE"]
        )
        self.expected_combined_df = self.expected_combined_df.sort_values(
            "SETTLEMENTDATE"
        )

    def test_merge_tables(self):
        merged_table = custom_tables.merge_tables_for_plant_stats(
            self.timeseries_df,
            self.gen_info,
            self.gen_info2,
            self.scada,
            self.trading_load,
            self.dispatch_price,
            self.trading_price,
            self.region_summary,
        )
        np.array_equal(merged_table, self.expected_combined_df)


class TestSelectHighestVersionNumber(unittest.TestCase):
    def setUp(self):
        self.gen_info = pd.DataFrame(
            {
                "EFFECTIVEDATE": [
                    "2014/01/01 00:00:00",
                    "2017/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                ],
                "VERSIONNO": ["1", "1", "1", "2"],
                "DUID": ["A", "A", "B", "B"],
                "MAXCAPACITY": [333, 400, 250, 800],
            }
        )
        self.gen_info["EFFECTIVEDATE"] = pd.to_datetime(self.gen_info["EFFECTIVEDATE"])
        self.expected_result = pd.DataFrame(
            {
                "EFFECTIVEDATE": [
                    "2014/01/01 00:00:00",
                    "2017/01/01 00:00:00",
                    "2014/01/01 00:00:00",
                ],
                "VERSIONNO": ["1", "1", "2"],
                "DUID": ["A", "A", "B"],
                "MAXCAPACITY": [333, 400, 800],
            }
        )
        self.expected_result["EFFECTIVEDATE"] = pd.to_datetime(
            self.expected_result["EFFECTIVEDATE"]
        )

    def test_select_highest_version_no(self):
        result = custom_tables.select_highest_version_number(
            self.gen_info, defaults.table_primary_keys["DUDETAIL"]
        )
        np.array_equal(result, self.expected_result)


class TestSelectInterventionIfPresent(unittest.TestCase):
    def setUp(self):
        self.dispatch_price = pd.DataFrame(
            {
                "REGIONID": ["NSW1", "NSW1", "NSW1", "VIC1", "VIC1"],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
                "RRP": [99, 110, 112, 300, 500],
                "INTERVENTION": ["0", "0", "1", "0", "1"],
            }
        )
        self.dispatch_price["SETTLEMENTDATE"] = pd.to_datetime(
            self.dispatch_price["SETTLEMENTDATE"]
        )
        self.expected_result = pd.DataFrame(
            {
                "REGIONID": ["NSW1", "NSW1", "VIC1", "VIC1"],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
                "RRP": [99, 112, 300, 500],
                "INTERVENTION": ["0", "1", "0", "1"],
            }
        )
        self.expected_result["SETTLEMENTDATE"] = pd.to_datetime(
            self.expected_result["SETTLEMENTDATE"]
        )

    def test_select_highest_version_no(self):
        result = custom_tables.select_intervention_if_present(
            self.dispatch_price, defaults.table_primary_keys["DISPATCHPRICE"]
        )
        np.array_equal(result, self.expected_result)


class TestPlantStats(unittest.TestCase):
    def setUp(self):
        pass

    def test_plant_stats(self):
        if os.path.isfile("C:/Users/user/Documents/plant_stats.csv"):
            t0 = time.time()
            plant_types = data_fetch_methods.static_table_xl(
                "",
                "",
                "Generators and Scheduled Loads",
                defaults.raw_data_cache,
                select_columns=[
                    "DUID",
                    "Fuel Source - Primary",
                    "Region",
                    "Participant",
                ],
            )
            plant_stats = custom_tables.plant_stats(
                "2017/07/01 00:05:00",
                "2018/07/01 00:05:00",
                "",
                defaults.raw_data_cache,
            )
            plant_stats = pd.merge(plant_stats, plant_types, "left", "DUID")
            plant_stats["TRADING_COST"] = (
                plant_stats["Volume"] * plant_stats["TRADING_VWAP"]
            )
            plant_stats["DISPATCH_COST"] = (
                plant_stats["Volume"] * plant_stats["DISPATCH_VWAP"]
            )
            plant_stats.to_csv("C:/Users/user/Documents/plant_stats_tp.csv")
            print(time.time() - t0)


class TestPlantsAgainstExcelNumbers(unittest.TestCase):
    def setUp(self):
        pass

    def test_nyngan1(self):
        if os.path.isfile("E:/plants_stats_test_data/NYNGAN1/NYNGAN1_test.xlsx"):
            xls = pd.ExcelFile("E:/plants_stats_test_data/NYNGAN1/NYNGAN1_test.xlsx")
            table = pd.read_excel(xls, "Plant_stats", dtype=str)
            results = custom_tables.plant_stats(
                "2017/01/01 00:00:00",
                "2017/02/01 00:00:00",
                "",
                defaults.raw_data_cache,
                filter_cols=["DUID"],
                filter_values=[("NYNGAN1",)],
            )
            for col in [col for col in table.columns if col not in ["Month", "DUID"]]:
                table[col] = table[col].astype(float)
            results.reset_index(drop=True, inplace=True)
            pd.testing.assert_frame_equal(results, table)

    def test_eildon2(self):
        if os.path.isfile("E:/plants_stats_test_data/EILDON2/EILDON2_test.xlsx"):
            xls = pd.ExcelFile("E:/plants_stats_test_data/EILDON2/EILDON2_test.xlsx")
            table = pd.read_excel(xls, "Plant_stats", dtype=str)
            results = custom_tables.plant_stats(
                "2018/01/01 00:00:00",
                "2018/02/01 00:00:00",
                "",
                defaults.raw_data_cache,
                filter_cols=["DUID"],
                filter_values=[("EILDON2",)],
            )
            for col in [col for col in table.columns if col not in ["Month", "DUID"]]:
                table[col] = table[col].astype(float)
            results.reset_index(drop=True, inplace=True)
            pd.testing.assert_frame_equal(results, table)


class TestCalcTradingLoad(unittest.TestCase):
    def setUp(self):
        self.scada = pd.DataFrame(
            {
                "DUID": ["A", "A", "B", "B"],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
                "SCADAVALUE": [150, 150, 200, 220],
            }
        )
        self.scada["SETTLEMENTDATE"] = pd.to_datetime(self.scada["SETTLEMENTDATE"])
        self.result = pd.DataFrame(
            {
                "DUID": ["A", "A", "B", "B"],
                "TOTALCLEARED": [150, 150, 200, 220],
                "SETTLEMENTDATE": [
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                    "2015/01/01 00:00:00",
                    "2015/01/01 00:05:00",
                ],
            }
        )
        self.result["SETTLEMENTDATE"] = pd.to_datetime(self.result["SETTLEMENTDATE"])

    def test_calc_trading_load_simple(self):
        trading_load = custom_tables.calc_trading_load(self.scada)
        pd.testing.assert_frame_equal(trading_load, self.result)


class TestCalcTradingLoad(unittest.TestCase):
    def setUp(self):
        pass

    def test_calc_trading_load_simple(self):
        pass
        # custom_tables.trading_and_dispatch_cost()
