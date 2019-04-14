import unittest
from nemosis import data_fetch_methods
import pandas as pd
from nemosis import custom_tables
import math
import numpy as np
import time
from nemosis import defaults
from datetime import datetime, timedelta
import os


class TestBaseVolumeWeightAveragePriceFunction(unittest.TestCase):
    def setUp(self):
        self.scada_data = pd.DataFrame({
             'SETTLEMENTDATE': ['2015/01/01 01:05:00', '2015/01/01 01:10:00', '2015/01/01 01:15:00',
                                '2015/01/01 01:20:00', '2015/01/01 01:25:00', '2015/01/01 01:30:00',
                                '2015/01/01 01:35:00', '2015/01/01 01:40:00', '2015/01/01 01:45:00',
                                '2015/01/01 01:50:00', '2015/01/01 01:55:00', '2015/01/01 02:00:00',
                                '2015/01/01 02:05:00',
                                '2015/01/01 01:05:00', '2015/01/01 01:10:00', '2015/01/01 01:15:00',
                                '2015/01/01 01:20:00', '2015/01/01 01:25:00', '2015/01/01 01:30:00',
                                '2015/01/01 01:35:00', '2015/01/01 01:40:00', '2015/01/01 01:45:00',
                                '2015/01/01 01:50:00', '2015/01/01 01:55:00', '2015/01/01 02:00:00',
                                '2015/01/01 02:05:00'],
             'DUID': ['BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG', 'BFG',
                      'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ', 'XYZ'],
             'SCADAVALUE': [85.1, 92.8, 95.7, 112.6, 105.1, 104.2, 85.1, 92.8, 95.7, 112.6, 105.1, 104.2, 100.2,
                            100.5, 105.8, 95.7, 112.6, 105.1, 105.2, 85.1, 92.8, 95.7, 95.6, 105.1, 104.2, 85.2]})
        self.test_meter_data = pd.DataFrame({
            'DUID': ['BFG', 'BFG', 'XYZ', 'XYZ'],
            'SETTLEMENTDATE': ['2015/01/01 01:30:00', '2015/01/01 02:00:00',
                                '2015/01/01 01:30:00', '2015/01/01 02:00:00'],
            'Interval_energy': [49.625, 50.25416667, 51.43333333, 48.2125]})
        pass

    def test_test_estimate_meter_data(self):
        self.scada_data['SETTLEMENTDATE'] = pd.to_datetime(self.scada_data['SETTLEMENTDATE'])
        meter_data = custom_tables.estimate_meter_data(self.scada_data)
        self.test_meter_data['SETTLEMENTDATE'] = pd.to_datetime(self.test_meter_data['SETTLEMENTDATE'])
        pd.testing.assert_frame_equal(meter_data, self.test_meter_data)


    def test_get__meter_data(self):
        meter_data = custom_tables.get_meter_data(start_time='2018/01/01 12:00:00', end_time='2018/01/01 14:00:00',
                                                  table_name='', raw_data_location='E:/raw_aemo_data',
                                                  select_columns=None, filter_cols=('DUID',),
                                                  filter_values=(['BROKENH1'],))
        scada = data_fetch_methods.dynamic_data_compiler(start_time='2018/01/01 12:00:00', end_time='2018/01/01 14:05:00',
                                                  table_name='DISPATCH_UNIT_SCADA', raw_data_location='E:/raw_aemo_data',
                                                  select_columns=None, filter_cols=('DUID',),
                                                  filter_values=(['BROKENH1'],))
        x=1
