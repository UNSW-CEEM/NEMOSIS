import unittest
import os
from datetime import datetime, timedelta
from nemosis import data_fetch_methods
from nemosis import defaults
import pandas as pd
from nemosis import custom_tables


class TestDynamicDataCompilerWithSettlementDateFiltering(unittest.TestCase):
    def setUp(self):
        self.table_names = ['BIDDAYOFFER_D', 'BIDPEROFFER_D', 'DISPATCHLOAD',
                            'DISPATCHCONSTRAINT', 'DISPATCH_UNIT_SCADA',
                            'DISPATCHPRICE', 'DISPATCHINTERCONNECTORRES',
                            'DISPATCHREGIONSUM', 'TRADINGLOAD', 'TRADINGPRICE',
                            'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']

        self.table_types = {'DISPATCHLOAD': 'DUID',
                            'DISPATCHCONSTRAINT': 'CONSTRAINTID',
                            'DISPATCH_UNIT_SCADA': 'DUIDONLY',
                            'DISPATCHPRICE': 'REGIONID',
                            'DISPATCHINTERCONNECTORRES': 'INTERCONNECTORID',
                            'DISPATCHREGIONSUM': 'REGIONID',
                            'BIDPEROFFER_D': 'DUID-BIDTYPE',
                            'BIDDAYOFFER_D': 'DUID-BIDTYPE',
                            'TRADINGLOAD': 'DUIDONLY',
                            'TRADINGPRICE': 'REGIONIDONLY',
                            'TRADINGREGIONSUM': 'REGIONIDONLY',
                            'TRADINGINTERCONNECT': 'INTERCONNECTORIDONLY'}

        self.table_filters = {'DISPATCHLOAD': ['DUID', 'INTERVENTION'],
                              'DISPATCHCONSTRAINT': ['CONSTRAINTID',
                                                     'INTERVENTION'],
                              'DISPATCH_UNIT_SCADA': ['DUID'],
                              'DISPATCHPRICE': ['REGIONID', 'INTERVENTION'],
                              'DISPATCHINTERCONNECTORRES': ['INTERCONNECTORID',
                                                            'INTERVENTION'],
                              'DISPATCHREGIONSUM': ['REGIONID',
                                                    'INTERVENTION'],
                              'BIDPEROFFER_D': ['DUID', 'BIDTYPE'],
                              'BIDDAYOFFER_D': ['DUID', 'BIDTYPE'],
                              'TRADINGLOAD': ['DUID'],
                              'TRADINGPRICE': ['REGIONID'],
                              'TRADINGREGIONSUM': ['REGIONID'],
                              'TRADINGINTERCONNECT': ['INTERCONNECTORID']}

        self.filter_values = {'DUID': (['AGLHAL'], [0]),
                              'DUIDONLY': (['AGLHAL'],),
                              'REGIONID': (['SA1'], [0]),
                              'REGIONIDONLY': (['SA1'],),
                              'INTERCONNECTORID': (['VIC1-NSW1'], [0]),
                              'INTERCONNECTORIDONLY': (['VIC1-NSW1'],),
                              'CONSTRAINTID': (['DATASNAP_DFS_Q_CLST'], [0]),
                              'DUID-BIDTYPE': (['AGLHAL'], ['ENERGY'])}

    def test_dispatch_tables_start_of_month(self):
        start_time = '2018/02/01 00:00:00'
        end_time = '2018/02/01 05:15:00'
        for table in self.table_names:
            print(f'Testing {table} returning values at start of month.')
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 63
            expected_number_of_columns = 2
            expected_first_time = \
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S') +
                 timedelta(minutes=5))
            expected_last_time = pd.to_datetime(end_time,
                                                format='%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM',
                         'TRADINGINTERCONNECT']:
                expected_length = 10
                expected_first_time = '2018/02/01 00:30:00'
                expected_first_time =\
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
                expected_last_time = '2018/02/01 05:00:00'
                expected_last_time =\
                    pd.to_datetime(expected_last_time,
                                   format='%Y/%m/%d %H:%M:%S')
            if "ONLY" not in table_type:
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                expected_length = 2
                expected_last_time = '2018/02/01 00:00:00'
                expected_last_time = \
                    pd.to_datetime(expected_last_time,
                                   format='%Y/%m/%d %H:%M:%S')
                expected_first_time = '2018/01/31 00:00:00'
                expected_first_time =\
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_end_of_month(self):
        start_time = '2018/01/31 21:00:00'
        end_time = '2018/02/01 00:00:00'
        for table in self.table_names:
            print('Testing {} returing values at end of month.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 36
            expected_number_of_columns = 2
            expected_first_time =\
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S') +
                 timedelta(minutes=5))
            expected_last_time =\
                pd.to_datetime(end_time, format='%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM',
                         'TRADINGINTERCONNECT']:
                expected_length = 6
                expected_first_time = '2018/01/31 21:30:00'
                expected_first_time =\
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
            if "ONLY" not in table_type:
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_last_time =\
                    expected_first_time.replace(hour=0, minute=0)
                expected_first_time =\
                    expected_first_time.replace(hour=0, minute=0)
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_straddle_2_months(self):
        start_time = '2018/02/28 21:00:00'
        end_time = '2018/03/01 21:00:00'
        for table in self.table_names:
            print(f'Testing {table} returing values from adjacent months.')
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 288
            expected_number_of_columns = 2
            expected_first_time =\
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time = pd.to_datetime(end_time,
                                                format='%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE',
                         'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 48
                expected_first_time = '2018/02/28 21:30:00'
                expected_first_time =\
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
            if "ONLY" not in table_type:
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                expected_length = 2
                expected_last_time =\
                    expected_last_time.replace(hour=0, minute=0)
                expected_first_time =\
                    expected_first_time.replace(hour=0, minute=0)
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_start_of_year(self):
        start_time = '2018/01/01 00:00:00'
        end_time = '2018/01/01 01:00:00'
        for table in self.table_names:
            print('Testing {} returing values at start of year.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 12
            expected_number_of_columns = 2
            expected_first_time = \
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time = pd.to_datetime(end_time,
                                                format='%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE',
                         'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 2
                expected_first_time = '2018/01/01 00:30:00'
                expected_first_time = \
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
            if "ONLY" not in table_type:
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_last_time =\
                    (expected_last_time.replace(hour=0, minute=0)
                     - timedelta(days=1))
                expected_first_time =\
                    (expected_first_time.replace(hour=0, minute=0)
                     - timedelta(days=1))
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_end_of_year(self):
        start_time = '2017/12/31 23:00:00'
        end_time = '2018/01/01 00:00:00'
        for table in self.table_names:
            print('Testing {} returing values at end of year.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 12
            expected_number_of_columns = 2
            expected_first_time = \
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time =\
                pd.to_datetime(end_time, format='%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE',
                         'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 2
                expected_first_time = '2017/12/31 23:30:00'
                expected_first_time =\
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
            if "ONLY" not in table_type:
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_first_time =\
                    expected_first_time.replace(hour=0, minute=0)
                expected_last_time = expected_first_time
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_straddle_years(self):
        start_time = '2017/12/31 23:00:00'
        end_time = '2018/01/01 01:00:00'
        for table in self.table_names:
            print(f'Testing {table} returning values from adjacent years.')
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 24
            expected_number_of_columns = 2
            expected_first_time =\
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time =\
                pd.to_datetime(end_time, format='%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE',
                         'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 4
                expected_first_time = '2017/12/31 23:30:00'
                expected_first_time =\
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
            if "ONLY" not in table_type:
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_first_time =\
                    expected_first_time.replace(hour=0, minute=0)
                expected_last_time = expected_first_time
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')


class TestDynamicDataCompilerWithSettlementDateFiltering2021OfferData(unittest.TestCase):
    def setUp(self):
        self.table_names = ['BIDPEROFFER_D', 'BIDDAYOFFER_D']

        self.table_types = {'BIDPEROFFER_D': 'DUID-BIDTYPE',
                            'BIDDAYOFFER_D': 'DUID-BIDTYPE'}

        self.table_filters = {'BIDPEROFFER_D': ['DUID', 'BIDTYPE'],
                              'BIDDAYOFFER_D': ['DUID', 'BIDTYPE']}

        self.filter_values = {'DUID-BIDTYPE': (['AGLHAL'], ['ENERGY'])}

    def test_dispatch_tables_start_of_month(self):
        start_time = '2021/06/01 00:00:00'
        end_time = '2021/06/01 05:15:00'
        for table in self.table_names:
            print(f'Testing {table} returning values at start of month.')
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 63
            expected_number_of_columns = 3
            expected_first_time = (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S') + timedelta(minutes=5))
            expected_last_time = pd.to_datetime(end_time, format='%Y/%m/%d %H:%M:%S')
            if table == 'BIDDAYOFFER_D':
                expected_length = 2
                expected_last_time = '2021/06/01 00:00:00'
                expected_last_time = \
                    pd.to_datetime(expected_last_time,
                                   format='%Y/%m/%d %H:%M:%S')
                expected_first_time = '2021/05/31 00:00:00'
                expected_first_time =\
                    pd.to_datetime(expected_first_time,
                                   format='%Y/%m/%d %H:%M:%S')
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_end_of_month(self):
        start_time = '2021/06/30 21:00:00'
        end_time = '2021/07/01 00:00:00'
        for table in self.table_names:
            print('Testing {} returing values at end of month.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 36
            expected_number_of_columns = 3
            expected_first_time =\
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S') +
                 timedelta(minutes=5))
            expected_last_time =\
                pd.to_datetime(end_time, format='%Y/%m/%d %H:%M:%S')
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_last_time = expected_first_time.replace(hour=0, minute=0)
                expected_first_time = expected_first_time.replace(hour=0, minute=0)
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_straddle_2_months(self):
        start_time = '2021/06/30 21:00:00'
        end_time = '2021/07/01 21:00:00'
        for table in self.table_names:
            print(f'Testing {table} returing values from adjacent months.')
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 288
            expected_number_of_columns = 3
            expected_first_time =\
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time = pd.to_datetime(end_time,
                                                format='%Y/%m/%d %H:%M:%S')
            if table == 'BIDDAYOFFER_D':
                expected_length = 2
                expected_last_time =\
                    expected_last_time.replace(hour=0, minute=0)
                expected_first_time =\
                    expected_first_time.replace(hour=0, minute=0)
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_start_of_year(self):
        start_time = '2021/01/01 00:00:00'
        end_time = '2021/01/01 01:00:00'
        for table in self.table_names:
            print('Testing {} returing values at start of year.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 12
            expected_number_of_columns = 3
            expected_first_time = \
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time = pd.to_datetime(end_time,
                                                format='%Y/%m/%d %H:%M:%S')
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_last_time =\
                    (expected_last_time.replace(hour=0, minute=0)
                     - timedelta(days=1))
                expected_first_time =\
                    (expected_first_time.replace(hour=0, minute=0)
                     - timedelta(days=1))
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_end_of_year(self):
        start_time = '2021/12/31 23:00:00'
        end_time = '2022/01/01 00:00:00'
        for table in self.table_names:
            print('Testing {} returing values at end of year.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 12
            expected_number_of_columns = 3
            expected_first_time = \
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time =\
                pd.to_datetime(end_time, format='%Y/%m/%d %H:%M:%S')
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_first_time =\
                    expected_first_time.replace(hour=0, minute=0)
                expected_last_time = expected_first_time
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_straddle_years(self):
        start_time = '2021/12/31 23:00:00'
        end_time = '2022/01/01 01:00:00'
        for table in self.table_names:
            print(f'Testing {table} returning values from adjacent years.')
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            filter_cols = self.table_filters[table]
            cols = [dat_col, *filter_cols]
            expected_length = 24
            expected_number_of_columns = 3
            expected_first_time =\
                (pd.to_datetime(start_time, format='%Y/%m/%d %H:%M:%S')
                 + timedelta(minutes=5))
            expected_last_time =\
                pd.to_datetime(end_time, format='%Y/%m/%d %H:%M:%S')
            if table == 'BIDDAYOFFER_D':
                expected_length = 1
                expected_first_time =\
                    expected_first_time.replace(hour=0, minute=0)
                expected_last_time = expected_first_time
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, fformat="feather", keep_csv=False,
                    filter_cols=filter_cols,
                    filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')


class TestDynamicDataCompilerWithEffectiveDateFiltering(unittest.TestCase):
    def setUp(self):
        self.table_names = ['GENCONDATA', 'SPDREGIONCONSTRAINT',
                            'SPDCONNECTIONPOINTCONSTRAINT',
                            'SPDINTERCONNECTORCONSTRAINT']

    def test_filtering_for_one_interval_returns(self):
        start_time = '2018/02/20 23:00:00'
        end_time = '2018/02/20 23:05:00'
        for table in self.table_names:
            print('Testing {} returing values for 1 interval.'.format(table))
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    fformat="feather", keep_csv=False,
                    select_columns=defaults.table_primary_keys[table])
            group_cols = [col for col in defaults.table_primary_keys[table]
                          if col != 'EFFECTIVEDATE']
            contains_duplicates = data.duplicated(group_cols).any()
            self.assertEqual(False, contains_duplicates)
            not_empty = data.shape[0] > 0
            self.assertEqual(True, not_empty)
            print('Passed')


class TestCacheCompiler(unittest.TestCase):
    def setUp(self):
        self.table_names = ['BIDDAYOFFER_D', 'BIDPEROFFER_D', 'DISPATCHLOAD',
                            'DISPATCHCONSTRAINT', 'DISPATCH_UNIT_SCADA',
                            'DISPATCHPRICE', 'DISPATCHINTERCONNECTORRES',
                            'DISPATCHREGIONSUM', 'TRADINGLOAD', 'TRADINGPRICE',
                            'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']
        self.id_cols = {'DISPATCHLOAD': 'DUID',
                        'DISPATCHCONSTRAINT': 'CONSTRAINTID',
                        'DISPATCH_UNIT_SCADA': 'DUID',
                        'DISPATCHPRICE': 'REGIONID',
                        'DISPATCHINTERCONNECTORRES': 'INTERCONNECTORID',
                        'DISPATCHREGIONSUM': 'REGIONID',
                        'BIDPEROFFER_D': 'DUID',
                        'BIDDAYOFFER_D': 'DUID',
                        'TRADINGLOAD': 'DUID',
                        'TRADINGPRICE': 'REGIONID',
                        'TRADINGREGIONSUM': 'REGIONID',
                        'TRADINGINTERCONNECT': 'INTERCONNECTORID'}

    def test_caching_and_typing_works_feather(self):
        start_time = '2018/02/20 23:00:00'
        end_time = '2018/02/20 23:30:00'
        for table in self.table_names:
            dat_col = defaults.primary_date_columns[table]
            id_col = self.id_cols[table]
            print(f'Testing {table} returing values for 1 interval.')
            data_fetch_methods.cache_compiler(
                start_time, end_time, table, defaults.raw_data_cache,
                fformat="feather"
                )
            data = data_fetch_methods.dynamic_data_compiler(
                start_time, end_time, table, defaults.raw_data_cache,
                fformat="feather"
            )
            dat_col_type = data[dat_col].dtype
            id_col_type = data[id_col].dtype
            not_empty = data.shape[0] > 0
            not_typed = all(data.dtypes == "object")
            self.assertTrue(not_empty)
            self.assertFalse(not_typed)
            self.assertEqual(dat_col_type, "<M8[ns]")
            self.assertEqual(id_col_type, "object")
            print('Passed')

    def test_caching_and_typing_works_parquet(self):
        start_time = '2018/02/20 23:00:00'
        end_time = '2018/02/20 23:30:00'
        for table in self.table_names:
            dat_col = defaults.primary_date_columns[table]
            id_col = self.id_cols[table]
            print('Testing {} returing values for 1 interval.'.format(table))
            data_fetch_methods.cache_compiler(
                start_time, end_time, table, defaults.raw_data_cache,
                fformat="parquet"
                )
            data = data_fetch_methods.dynamic_data_compiler(
                start_time, end_time, table, defaults.raw_data_cache,
                fformat="parquet"
            )
            dat_col_type = data[dat_col].dtype
            id_col_type = data[id_col].dtype
            not_empty = data.shape[0] > 0
            not_typed = all(data.dtypes == "object")
            self.assertTrue(not_empty)
            self.assertFalse(not_typed)
            self.assertEqual(dat_col_type, "<M8[ns]")
            self.assertEqual(id_col_type, "object")
            print('Passed')

    def test_caching_with_select_columns_works(self):
        start_time = '2018/02/20 23:00:00'
        end_time = '2018/02/20 23:30:00'
        table = 'DISPATCHPRICE'
        print('Testing {} returing values for 1 interval.'.format(table))
        data_fetch_methods.cache_compiler(
            start_time, end_time, table, defaults.raw_data_cache,
            fformat="parquet", rebuild=True
            )
        data_fetch_methods.cache_compiler(
            start_time, end_time, table, defaults.raw_data_cache,
            fformat="parquet", select_columns=['SETTLEMENTDATE', 'REGIONID'],
            rebuild=True
            )
        data = pd.read_parquet(os.path.join(defaults.raw_data_cache, 'PUBLIC_DVD_DISPATCHPRICE_201802010000.parquet'))
        data_fetch_methods.cache_compiler(
            start_time, end_time, table, defaults.raw_data_cache,
            fformat="parquet", rebuild=True
            )
        got_columns = list(data.columns)
        expected_columns = ['SETTLEMENTDATE', 'REGIONID']
        self.assertSequenceEqual(got_columns, expected_columns)
        print('Passed')


class TestDynamicDataCompilerWithStartDateFiltering(unittest.TestCase):
    def setUp(self):
        self.table_names = ['DUDETAILSUMMARY']

    def test_filtering_for_one_interval_returns(self):
        start_time = '2018/02/20 23:00:00'
        end_time = '2018/02/20 23:05:00'
        for table in self.table_names:
            print('Testing {} returing values for 1 interval.'.format(table))
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table,
                    defaults.raw_data_cache, fformat="feather", keep_csv=False,
                    select_columns=defaults.table_primary_keys[table])
            group_cols = [col for col in defaults.table_primary_keys[table]
                          if col != 'START_DATE']
            contains_duplicates = data.duplicated(group_cols).any()
            self.assertEqual(False, contains_duplicates)
            not_empty = data.shape[0] > 0
            self.assertEqual(True, not_empty)
            print('Passed')


class TestFCAS4SecondData(unittest.TestCase):
    def setUp(self):
        self.start_day = (datetime.now() - timedelta(30)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        self.start_month = self.start_day.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        self.start_year = self.start_day.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        pass

    def test_dispatch_tables_start_of_month(self):
        table = 'FCAS_4_SECOND'
        start_time = self.start_day
        minute_offset = 5
        end_time = start_time + timedelta(minutes=minute_offset)
        start_str = start_time.strftime("%Y/%m/%d %H:%M:%S")
        end_str = end_time.strftime("%Y/%m/%d %H:%M:%S")

        print('Testing {} returing values at start of month.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        # expected length assumes first data point is at 00:00:00
        expected_length = 15 * minute_offset
        length_check = False
        expected_number_of_columns = 3
        data = data_fetch_methods.dynamic_data_compiler(
                start_str, end_str, table, defaults.raw_data_cache,
                fformat="feather", keep_csv=False, select_columns=cols)
        length_array = data[dat_col].drop_duplicates()
        if length_array.shape[0] == expected_length:
            length_check = True
        # case if first data point is 00:00:01/02/03
        elif length_array.shape[0] == expected_length - 1:
            length_check = True
        self.assertTrue(length_check)
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')

    def test_fcas_tables_end_of_month(self):
        table = 'FCAS_4_SECOND'
        minute_offset = 5
        start_time = self.start_month - timedelta(minutes=minute_offset)
        end_time = start_time + timedelta(minutes=minute_offset * 2)
        start_str = start_time.strftime("%Y/%m/%d %H:%M:%S")
        end_str = end_time.strftime("%Y/%m/%d %H:%M:%S")

        print('Testing {} returing values at end of month.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        # expected length assumes first data point is at 00:00:00
        expected_length = 15 * (minute_offset * 2)
        length_check = False
        expected_number_of_columns = 3
        data = data_fetch_methods.dynamic_data_compiler(
                start_str, end_str, table, defaults.raw_data_cache,
                fformat="feather", keep_csv=False, select_columns=cols)
        length_array = data[dat_col].drop_duplicates()
        if length_array.shape[0] == expected_length:
            length_check = True
        # case if first data point is 00:00:01/02/03
        elif length_array.shape[0] == expected_length - 1:
            length_check = True
        print(length_array.shape[0])
        self.assertTrue(length_check)
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')

    @unittest.skipIf((datetime.now() - datetime(year=datetime.now().year,
                                                month=1, day=1)).days > 60,
                     "start of year data not available: > 60 days ago")
    def test_fcas_tables_end_of_year(self):
        table = 'FCAS_4_SECOND'
        minute_offset = 5
        start_time = self.start_year - timedelta(minutes=minute_offset)
        end_time = start_time + timedelta(minutes=minute_offset * 2)
        start_str = start_time.strftime("%Y/%m/%d %H:%M:%S")
        end_str = end_time.strftime("%Y/%m/%d %H:%M:%S")

        print('Testing {} returing values at end of year.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        expected_length = 15 * (minute_offset * 2)
        length_check = False
        expected_number_of_columns = 3
        data = data_fetch_methods.dynamic_data_compiler(
                start_str, end_str, table, defaults.raw_data_cache,
                select_columns=cols, fformat="feather", keep_csv=False)
        length_array = data[dat_col].drop_duplicates()
        if length_array.shape[0] == expected_length:
            length_check = True
        # case if first data point is 00:00:01/02/03
        elif length_array.shape[0] == expected_length - 1:
            length_check = True
        self.assertTrue(length_check)
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')


class TestStaticTables(unittest.TestCase):
    def setUp(self):
        pass

    def test_fcas_elements_table(self):
        start_time = '2017/12/31 23:55:04'
        end_time = '2018/01/01 00:05:00'
        table = 'ELEMENTS_FCAS_4_SECOND'
        cols = ['ELEMENTNUMBER', 'EMSNAME']
        filter_cols = ('ELEMENTNUMBER',)
        func = \
            data_fetch_methods._static_table_wrapper_for_gui
        data = func(start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, filter_cols=filter_cols,
                    filter_values=(['1'],))
        expected_length = 1
        expected_number_of_columns = 2
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')

    def test_fcas_variable_table(self):
        start_time = '2018/12/31 23:55:04'
        end_time = '2018/01/01 00:05:00'
        table = 'VARIABLES_FCAS_4_SECOND'
        cols = ['VARIABLENUMBER', 'VARIABLETYPE']
        filter_cols = ('VARIABLENUMBER',)
        func = data_fetch_methods._static_table_wrapper_for_gui
        data = func(start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols, filter_cols=filter_cols,
                    filter_values=(['2'],))
        expected_length = 1
        expected_number_of_columns = 2
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')

    def test_registration_list(self):
        table = 'Generators and Scheduled Loads'
        cols = ['DUID', 'Technology Type - Primary']
        filter_cols = ('DUID',)
        data = data_fetch_methods.static_table_xl(table,
                                                  defaults.raw_data_cache,
                                                  select_columns=cols,
                                                  filter_cols=filter_cols,
                                                  filter_values=(['AGLHAL'],))
        expected_length = 1
        expected_number_of_columns = 2
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')


class TestCustomTables(unittest.TestCase):
    def setUp(self):
        pass

    @unittest.skipIf((datetime.now()
                      - datetime(year=datetime.now().year,
                                 month=1, day=1)).days > 60,
                     "start of year data not available: > 60 days ago")
    def test_dispatch_tables_straddle_years(self):
        table = 'FCAS_4_SECOND'
        minute_offset = 5
        start_time = self.start_year - timedelta(minutes=minute_offset)
        end_time = start_time + timedelta(minutes=minute_offset * 2)

        print('Testing custom table {}.'.format(table))
        data = custom_tables.fcas4s_scada_match(start_time, end_time,
                                                table, defaults.raw_data_cache)
        data = data.reset_index(drop=True)
        contains_duplicates = data.duplicated(['MARKETNAME']).any()
        self.assertEqual(False, contains_duplicates)
        contains_duplicates = data.duplicated(['ELEMENTNUMBER']).any()
        self.assertEqual(False, contains_duplicates)
        not_empty = data.shape[0] > 0
        self.assertEqual(True, not_empty)
        print('Passed')
