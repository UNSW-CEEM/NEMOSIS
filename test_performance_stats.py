import unittest
import processing_info_maps
import data_fetch_methods
import defaults
import pandas as pd
from datetime import datetime
import query_wrapers
import custom_tables
import math
import numpy as np


class TestBaseVolumeWeightAveragePriceFunction(unittest.TestCase):
    def setUp(self):
        self.volume = pd.Series([55, 0, math.nan, 60, 40])
        self.price = pd.Series([88, 90, -100, -100, 50])
        self.pricing_data = pd.DataFrame({
             'SCADAVALUE': [8, 9, 50, 11, 10, 0],
             'TRADINGTOTALCLEARED': [20, math.nan, math.nan, 80, math.nan, math.nan],
             'TRADINGRRP': [80, math.nan, math.nan, 100, math.nan, math.nan],
             'DISPATCHRRP':[80, 90, 89, 111, 110, 75]})
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
            {'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00', '2015/01/01 00:10:00',
                                '2015/01/01 12:00:00', '2015/01/01 12:10:00', '2015/01/01 22:10:00'],
             'DUID': ['A', 'A', 'A', 'A', 'A', 'A'],
             'MAXCAPACITY': [68, 68, 68, 68, 68, 68],
             'SCADAVALUE': [8, 9, 50, 11, 10, 0],
             'TOTALDEMAND': [1000, 1100, 13000, 900, 800, 1000]})
        self.cap_and_output['SETTLEMENTDATE'] = pd.to_datetime(self.cap_and_output['SETTLEMENTDATE'])
        pass

    def test_performance_at_nodal_peak(self):
        peak = custom_tables.performance_at_nodal_peak(self.cap_and_output)
        self.assertAlmostEqual(peak, 50/68, 4)


class TestCapacityFactor90(unittest.TestCase):
    def setUp(self):
        self.cap_and_output = pd.DataFrame(
            {'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00', '2015/01/01 00:10:00',
                                '2015/01/01 12:00:00', '2015/01/01 12:10:00', '2015/01/01 22:10:00'],
             'DUID': ['A', 'A', 'A', 'A', 'A', 'A'],
             'MAXCAPACITY': [68, 68, 68, 68, 68, 68],
             'SCADAVALUE': [8, 9, 50, 11, 10, 0],
             'TOTALDEMAND': [1000, 1100, 13000, 900, 800, 1000]})
        self.cap_and_output['SETTLEMENTDATE'] = pd.to_datetime(self.cap_and_output['SETTLEMENTDATE'])
        self.cap_and_output2 = pd.DataFrame(
            {'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00', '2015/01/01 00:10:00',
                                '2015/01/01 12:00:00', '2015/01/01 12:10:00', '2015/01/01 22:10:00',
                                '2015/01/02 00:00:00', '2015/01/02 00:05:00', '2015/01/02 00:10:00',
                                '2015/01/02 12:00:00', '2015/01/02 12:10:00', '2015/01/02 22:10:00'],
             'DUID': ['A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A'],
             'MAXCAPACITY': [68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68, 68],
             'SCADAVALUE': [8, 9, 50, 11, 10, 0, 8, 9, 50, 11, 10, 90],
             'TOTALDEMAND': [10000, 11000, 13000, 900, 800, 1000, 1000, 11000, 13000, 900, 800, 18000]})
        self.cap_and_output2['SETTLEMENTDATE'] = pd.to_datetime(self.cap_and_output2['SETTLEMENTDATE'])
        pass

    def test_capacity_factor(self):
        peak = custom_tables.capacity_factor_over_90th_percentile_of_nodal_demand(self.cap_and_output)
        self.assertAlmostEqual(peak, 50/68, 4)

    def test_capacity_factor_2_intervals(self):
        peak = custom_tables.capacity_factor_over_90th_percentile_of_nodal_demand(self.cap_and_output2)
        self.assertAlmostEqual(peak, (50/68 + 90/68) / 2, 4)


class TestCapacityScadaBasedStats(unittest.TestCase):
    def setUp(self):
        self.cap_and_output = pd.DataFrame({'MAXCAPACITY': [68, 68, 68, 68, 68], 'SCADAVALUE': [8, 9, 50, 11, 10]})
        pass

    def test_capacity_factor(self):
        cf = custom_tables.capacity_factor(self.cap_and_output)
        self.assertAlmostEqual(cf, 0.258823529, 4)

    def test_volume(self):
        v = custom_tables.volume(self.cap_and_output)
        self.assertAlmostEqual(v, 7.3333, 4)


class TestMonthlyGroupingForCF(unittest.TestCase):
    def setUp(self):
        self.cap_and_output = pd.DataFrame(
            {'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00', '2015/01/01 00:10:00',
                                '2015/02/01 00:00:00', '2015/02/01 00:05:00', '2015/02/01 00:10:00'],
             'DUID': ['A', 'A', 'A', 'A', 'A', 'A'],
             'MAXCAPACITY': [68, 68, 68, 68, 68, 68],
             'SCADAVALUE': [8, 9, 50, 11, 10, 0]})
        self.cap_and_output['SETTLEMENTDATE'] = pd.to_datetime(self.cap_and_output['SETTLEMENTDATE'])
        self.cf_by_month = pd.DataFrame(
            {'MONTH': ['2015-01', '2015-02'],
             'DUID': ['A', 'A'],
             'CapacityFactor': [0.328431373, 0.102941176]})
        pass

    def test_capacity_grouped(self):
        cf_by_month_and_duid = custom_tables.stats_by_month_and_plant(self.cap_and_output)
        cf_by_month_and_duid = cf_by_month_and_duid.reset_index(drop=True)
        pd.testing.assert_frame_equal(cf_by_month_and_duid, self.cf_by_month)


class TestMergeTables(unittest.TestCase):
    def setUp(self):
        self.gen_info = pd.DataFrame({
            'EFFECTIVEDATE': ['2014/01/01 00:00:00', '2017/01/01 00:00:00', '2014/01/01 00:00:00'],
            'DUID': ['A', 'A', 'B'],
            'REGIONID': ['NSW1', 'NSW1', 'VIC1'],
            'MAXCAPACITY': [333, 400, 250]})
        self.gen_info['EFFECTIVEDATE'] = pd.to_datetime(self.gen_info['EFFECTIVEDATE'])
        self.scada = pd.DataFrame({
            'DUID': ['A', 'A', 'B', 'B'],
            'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00',
                               '2015/01/01 00:00:00', '2015/01/01 00:05:00'],
            'SCADAVALUE': [150, 150,  200, 220]})
        self.scada['SETTLEMENTDATE'] = pd.to_datetime(self.scada['SETTLEMENTDATE'])
        self.trading_load = pd.DataFrame({
            'DUID': ['A', 'B'],
            'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:00:00'],
            'TOTALCLEARED': [150, 200]})
        self.trading_load['SETTLEMENTDATE'] = pd.to_datetime(self.trading_load['SETTLEMENTDATE'])
        self.dispatch_price = pd.DataFrame({
            'REGIONID': ['NSW1', 'NSW1', 'VIC1', 'VIC1'],
            'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00',
                              '2015/01/01 00:00:00', '2015/01/01 00:05:00'],
            'RRP': [99, 110,  300, 500]})
        self.dispatch_price['SETTLEMENTDATE'] = pd.to_datetime(self.dispatch_price['SETTLEMENTDATE'])
        self.trading_price = pd.DataFrame({
            'REGIONID': ['NSW1', 'VIC1'],
            'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:00:00'],
            'RRP': [99,  300]})
        self.trading_price['SETTLEMENTDATE'] = pd.to_datetime(self.trading_price['SETTLEMENTDATE'])
        self.region_summary = pd.DataFrame({
            'REGIONID': ['NSW1', 'NSW1', 'VIC1', 'VIC1'],
            'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00',
                              '2015/01/01 00:00:00', '2015/01/01 00:05:00'],
            'TOTALDEMAND': [5000, 5010,  8000, 8700]})
        self.region_summary['SETTLEMENTDATE'] = pd.to_datetime(self.region_summary['SETTLEMENTDATE'])

        self.expected_combined_df = pd.DataFrame({
            'DUID': ['A', 'A', 'B', 'B'],
            'DUDETAIL_EFFECTIVEDATE': ['2014/01/01 00:00:00', '2014/01/01 00:00:00',
                                       '2014/01/01 00:00:00', '2014/01/01 00:00:00'],
            'REGIONID': ['NSW1', 'NSW1', 'VIC1', 'VIC1'],
            'SETTLEMENTDATE': ['2015/01/01 00:00:00', '2015/01/01 00:05:00',
                               '2015/01/01 00:00:00', '2015/01/01 00:05:00'],
            'MAXCAPACITY': [333, 333, 250, 250],
            'SCADAVALUE': [150, 150, 200, 220],
            'TRADING_TOTALCLEARED': [150, math.nan, 200, math.nan],
            'DISPATCH_RRP': [99, 110,  300, 500],
            'TRADING_RRP': [99, math.nan, 300, math.nan],
            'TOTALDEMAND': [5000, 5010, 8000, 8700]})
        self.expected_combined_df['SETTLEMENTDATE'] = pd.to_datetime(self.expected_combined_df['SETTLEMENTDATE'])
        self.expected_combined_df = self.expected_combined_df.sort_values('SETTLEMENTDATE')

    def test_merge_tables(self):
        merged_table = custom_tables.merge_tables_for_plant_stats(self.gen_info, self.scada, self.trading_load,
                                                                  self.dispatch_price, self.trading_price,
                                                                  self.region_summary)
        np.array_equal(merged_table, self.expected_combined_df)


class TestPlantStats(unittest.TestCase):
    def setUp(self):
        pass

    def test_plant_stats(self):
        plant_stats = custom_tables.plant_stats('2015/01/01 00:00:00', '2015/01/02 00:00:00', '', 'E:/raw_aemo_data')
        plant_stats.to_csv('C:/Users/user/Documents/plant_stats.csv')

