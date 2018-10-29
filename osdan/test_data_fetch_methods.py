import unittest
from datetime import timedelta
from osdan import data_fetch_methods
from osdan import defaults
import pandas as pd
from osdan import custom_tables


class TestDynamicDataCompilerWithSettlementDateFiltering(unittest.TestCase):
    def setUp(self):
        self.table_names = ['BIDDAYOFFER_D', 'BIDPEROFFER_D', 'DISPATCHLOAD', 'DISPATCHCONSTRAINT',
                            'DISPATCH_UNIT_SCADA', 'DISPATCHPRICE', 'DISPATCHINTERCONNECTORRES', 'DISPATCHREGIONSUM',
                            'TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']

        self.table_types = {'DISPATCHLOAD': 'DUID', 'DISPATCHCONSTRAINT': 'CONSTRAINTID', 'DISPATCH_UNIT_SCADA': 'DUID',
                       'DISPATCHPRICE': 'REGIONID', 'DISPATCHINTERCONNECTORRES': 'INTERCONNECTORID',
                       'DISPATCHREGIONSUM': 'REGIONID', 'BIDPEROFFER_D': 'DUID-BIDTYPE',
                       'BIDDAYOFFER_D': 'DUID-BIDTYPE', 'TRADINGLOAD': 'DUID', 'TRADINGPRICE': 'REGIONID',
                       'TRADINGREGIONSUM': 'REGIONID', 'TRADINGINTERCONNECT': 'INTERCONNECTORID'}

        self.filter_values = {'DUID': (['AGLHAL'],), 'REGIONID': (['SA1'],), 'INTERCONNECTORID': (['VIC1-NSW1'],),
                              'CONSTRAINTID': (['DATASNAP_DFS_Q_CLST'],), 'DUID-BIDTYPE': (['AGLHAL', 'ENERGY'],)}

    def test_dispatch_tables_start_of_month(self):
        start_time = '2015/05/01 00:00:00'
        end_time = '2015/05/01 05:15:00'
        for table in self.table_names:
            print('Testing {} returing values at start of month.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 63
            expected_number_of_columns = 2
            expected_first_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(minutes=5)
            expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 10
                expected_first_time = '2015/05/01 00:30:00'
                expected_first_time = pd.Timestamp.strptime(expected_first_time, '%Y/%m/%d %H:%M:%S')
                expected_last_time = '2015/05/01 05:00:00'
                expected_last_time = pd.Timestamp.strptime(expected_last_time, '%Y/%m/%d %H:%M:%S')
            if table == 'BIDPEROFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
                expected_length = 2
                expected_last_time = '2015/05/01 00:00:00'
                expected_last_time = pd.Timestamp.strptime(expected_last_time, '%Y/%m/%d %H:%M:%S')
                expected_first_time = '2015/04/30 00:00:00'
                expected_first_time = pd.Timestamp.strptime(expected_first_time, '%Y/%m/%d %H:%M:%S')
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols,
                    filter_cols=filter_cols, filter_values=self.filter_values[table_type])
            data = data.reset_index(drop=True)
            print(table)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_end_of_month(self):
        start_time = '2013/07/31 21:00:00'
        end_time = '2013/08/01 00:00:00'
        for table in self.table_names:
            print('Testing {} returing values at end of month.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 36
            expected_number_of_columns = 2
            expected_first_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(minutes=5)
            expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 6
                expected_first_time = '2013/07/31 21:30:00'
                expected_first_time = pd.Timestamp.strptime(expected_first_time, '%Y/%m/%d %H:%M:%S')
            if table == 'BIDPEROFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
                expected_length = 1
                expected_last_time = expected_first_time.replace(hour=0, minute=0)
                expected_first_time = expected_first_time.replace(hour=0, minute=0)
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols,
                    filter_cols=filter_cols, filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_stradle_2_months(self):
        start_time = '2014/03/31 21:00:00'
        end_time = '2014/04/01 21:00:00'
        for table in self.table_names:
            print('Testing {} returing values from adjacent months.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 288
            expected_number_of_columns = 2
            expected_first_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(minutes=5)
            expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
            if table in ['TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 48
                expected_first_time = '2014/03/31 21:30:00'
                expected_first_time = pd.Timestamp.strptime(expected_first_time, '%Y/%m/%d %H:%M:%S')
            if table == 'BIDPEROFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
                expected_length = 2
                expected_last_time = expected_last_time.replace(hour=0, minute=0)
                expected_first_time = expected_first_time.replace(hour=0, minute=0)
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols,
                    filter_cols=filter_cols, filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_start_of_year(self):
        start_time = '2014/01/01 00:00:00'
        end_time = '2014/01/01 01:00:00'
        for table in self.table_names:
            print('Testing {} returing values at start of year.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 12
            expected_number_of_columns = 2
            expected_first_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(minutes=5)
            expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
            if table in [ 'TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 2
                expected_first_time = '2014/01/01 00:30:00'
                expected_first_time = pd.Timestamp.strptime(expected_first_time, '%Y/%m/%d %H:%M:%S')
            if table == 'BIDPEROFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
                expected_length = 1
                expected_last_time = expected_last_time.replace(hour=0, minute=0) - timedelta(days=1)
                expected_first_time = expected_first_time.replace(hour=0, minute=0) - timedelta(days=1)
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols,
                    filter_cols=filter_cols, filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_end_of_year(self):
        start_time = '2013/12/31 23:00:00'
        end_time = '2014/01/01 00:00:00'
        for table in self.table_names:
            print('Testing {} returing values at end of year.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 12
            expected_number_of_columns = 2
            expected_first_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(minutes=5)
            expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
            if table in [ 'TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 2
                expected_first_time = '2013/12/31 23:30:00'
                expected_first_time = pd.Timestamp.strptime(expected_first_time, '%Y/%m/%d %H:%M:%S')
            if table == 'BIDPEROFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
                expected_length = 1
                expected_first_time = expected_first_time.replace(hour=0, minute=0)
                expected_last_time = expected_first_time
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols,
                    filter_cols=filter_cols, filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')

    def test_dispatch_tables_stradle_years(self):
        start_time = '2017/12/31 23:00:00'
        end_time = '2018/01/01 01:00:00'
        for table in self.table_names:
            print('Testing {} returing values from adjacent years.'.format(table))
            dat_col = defaults.primary_date_columns[table]
            table_type = self.table_types[table]
            cols = [dat_col, self.table_types[table]]
            filter_cols = (self.table_types[table],)
            expected_length = 24
            expected_number_of_columns = 2
            expected_first_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(minutes=5)
            expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
            if table in [ 'TRADINGLOAD', 'TRADINGPRICE', 'TRADINGREGIONSUM', 'TRADINGINTERCONNECT']:
                expected_length = 4
                expected_first_time = '2017/12/31 23:30:00'
                expected_first_time = pd.Timestamp.strptime(expected_first_time, '%Y/%m/%d %H:%M:%S')
            if table == 'BIDPEROFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
            if table == 'BIDDAYOFFER_D':
                cols = [dat_col, 'DUID', 'BIDTYPE']
                filter_cols = ('DUID', 'BIDTYPE')
                expected_number_of_columns = 3
                expected_length = 1
                expected_first_time = expected_first_time.replace(hour=0, minute=0)
                expected_last_time = expected_first_time
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=cols,
                    filter_cols=filter_cols, filter_values=self.filter_values[table_type])
            data = data.sort_values(dat_col)
            data = data.reset_index(drop=True)
            self.assertEqual(expected_length, data.shape[0])
            self.assertEqual(expected_number_of_columns, data.shape[1])
            self.assertEqual(expected_first_time, data[dat_col][0])
            self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
            print('Passed')


class TestDynamicDataCompilerWithEffectiveDateFiltering(unittest.TestCase):
    def setUp(self):
        self.table_names = ['GENCONDATA', 'SPDREGIONCONSTRAINT', 'SPDCONNECTIONPOINTCONSTRAINT',
                            'SPDINTERCONNECTORCONSTRAINT']

    def test_filtering_for_one_interval_returns(self):
        start_time = '2017/05/20 23:00:00'
        end_time = '2017/05/20 23:05:00'
        for table in self.table_names:
            print('Testing {} returing values for 1 interval.'.format(table))
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=defaults.table_primary_keys[table])
            group_cols = [col for col in defaults.table_primary_keys[table] if col != 'EFFECTIVEDATE']
            contains_duplicates = data.duplicated(group_cols).any()
            self.assertEqual(False, contains_duplicates)
            not_empty = data.shape[0] > 0
            self.assertEqual(True, not_empty)
            print('Passed')


class TestDynamicDataCompilerWithStartDateFiltering(unittest.TestCase):
    def setUp(self):
        self.table_names = ['DUDETAILSUMMARY']

    def test_filtering_for_one_interval_returns(self):
        start_time = '2017/05/20 23:00:00'
        end_time = '2017/05/20 23:05:00'
        for table in self.table_names:
            print('Testing {} returing values for 1 interval.'.format(table))
            data = data_fetch_methods.dynamic_data_compiler(
                    start_time, end_time, table, defaults.raw_data_cache,
                    select_columns=defaults.table_primary_keys[table] + ['END_DATE'])
            group_cols = [col for col in defaults.table_primary_keys[table] if col != 'START_DATE']
            contains_duplicates = data.duplicated(group_cols).any()
            self.assertEqual(False, contains_duplicates)
            not_empty = data.shape[0] > 0
            self.assertEqual(True, not_empty)
            print('Passed')


class TestFACS4SecondData(unittest.TestCase):
    def setUp(self):
        pass

    def test_dispatch_tables_start_of_month(self):
        table = 'FCAS_4_SECOND'
        start_time = '2015/05/01 00:00:00'
        end_time = '2015/05/01 00:05:08'
        print('Testing {} returing values at start of month.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        filter_cols = ('ELEMENTNUMBER', 'VARIABLENUMBER')
        expected_length = 77
        expected_number_of_columns = 3
        expected_firt_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(seconds=4)
        expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
        data = data_fetch_methods.dynamic_data_compiler(
                start_time, end_time, table, defaults.raw_data_cache,
                select_columns=cols,
                filter_cols=filter_cols, filter_values=(['1'], ['3']))
        data = data.sort_values(dat_col)
        data = data.reset_index(drop=True)
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        self.assertEqual(expected_firt_time, data[dat_col][0])
        self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
        print('Passed')

    def test_fcas_tables_end_of_month(self):
        table = 'FCAS_4_SECOND'
        start_time = '2013/07/31 23:55:03'
        end_time = '2013/08/01 00:00:04'
        print('Testing {} returing values at end of month.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        filter_cols = ('ELEMENTNUMBER', 'VARIABLENUMBER')
        expected_length = 75
        expected_number_of_columns = 3
        expected_firt_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(seconds=4)
        expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
        data = data_fetch_methods.dynamic_data_compiler(
            start_time, end_time, table, defaults.raw_data_cache,
            select_columns=cols,
            filter_cols=filter_cols, filter_values=(['1'], ['3']))
        data = data.sort_values(dat_col)
        data = data.reset_index(drop=True)
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        self.assertEqual(expected_firt_time, data[dat_col][0])
        self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
        print('Passed')

    def test_fcas_tables_stradle_2_months(self):
        table = 'FCAS_4_SECOND'
        start_time = '2013/07/31 23:55:03'
        end_time = '2013/08/01 00:05:04'
        print('Testing {} returing values at end of month.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        filter_cols = ('ELEMENTNUMBER', 'VARIABLENUMBER')
        expected_length = 150
        expected_number_of_columns = 3
        expected_firt_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(seconds=4)
        expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
        data = data_fetch_methods.dynamic_data_compiler(
            start_time, end_time, table, defaults.raw_data_cache,
            select_columns=cols,
            filter_cols=filter_cols, filter_values=(['1'], ['3']))
        data = data.sort_values(dat_col)
        data = data.reset_index(drop=True)
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        self.assertEqual(expected_firt_time, data[dat_col][0])
        self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
        print('Passed')

    def test_fcas_tables_end_of_year(self):
        table = 'FCAS_4_SECOND'
        start_time = '2013/12/31 23:50:04'
        end_time = '2014/01/01 00:00:00'
        print('Testing {} returing values at end of year.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        filter_cols = ('ELEMENTNUMBER', 'VARIABLENUMBER')
        expected_length = 149
        expected_number_of_columns = 3
        expected_firt_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(seconds=4)
        expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
        data = data_fetch_methods.dynamic_data_compiler(
            start_time, end_time, table, defaults.raw_data_cache,
            select_columns=cols,
            filter_cols=filter_cols, filter_values=(['1'], ['3']))
        data = data.sort_values(dat_col)
        data = data.reset_index(drop=True)
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        self.assertEqual(expected_firt_time, data[dat_col][0])
        self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
        print('Passed')

    def test_dispatch_tables_stradle_years(self):
        table = 'FCAS_4_SECOND'
        start_time = '2011/12/31 23:55:04'
        end_time = '2012/01/01 00:05:00'
        print('Testing {} returing values from adjacent years.'.format(table))
        dat_col = defaults.primary_date_columns[table]
        cols = [dat_col, 'ELEMENTNUMBER', 'VARIABLENUMBER']
        filter_cols = ('ELEMENTNUMBER', 'VARIABLENUMBER')
        expected_length = 149
        expected_number_of_columns = 3
        expected_firt_time = pd.Timestamp.strptime(start_time, '%Y/%m/%d %H:%M:%S') + timedelta(seconds=4)
        expected_last_time = pd.Timestamp.strptime(end_time, '%Y/%m/%d %H:%M:%S')
        data = data_fetch_methods.dynamic_data_compiler(
            start_time, end_time, table, defaults.raw_data_cache,
            select_columns=cols,
            filter_cols=filter_cols, filter_values=(['1'], ['3']))
        data = data.sort_values(dat_col)
        data = data.reset_index(drop=True)
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        self.assertEqual(expected_firt_time, data[dat_col][0])
        self.assertEqual(expected_last_time, data[dat_col].iloc[-1])
        print('Passed')


class TestStaticTables(unittest.TestCase):
    def setUp(self):
        pass

    def test_fcas_elements_table(self):
        start_time = ''
        end_time = ''
        table = 'ELEMENTS_FCAS_4_SECOND'
        cols = ['ELEMENTNUMBER', 'EMSNAME']
        filter_cols = ('ELEMENTNUMBER',)
        data = data_fetch_methods.static_table( start_time, end_time, table, defaults.raw_data_cache,
            select_columns=cols, filter_cols=filter_cols, filter_values=(['1'],))
        expected_length = 1
        expected_number_of_columns = 2
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')

    def test_fcas_variable_table(self):
        start_time = ''
        end_time = ''
        table = 'VARIABLES_FCAS_4_SECOND'
        cols = ['VARIABLENUMBER', 'VARIABLETYPE']
        filter_cols = ('VARIABLENUMBER',)
        data = data_fetch_methods.static_table( start_time, end_time, table, defaults.raw_data_cache,
            select_columns=cols, filter_cols=filter_cols, filter_values=(['2'],))
        expected_length = 1
        expected_number_of_columns = 2
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')

    def test_registration_list(self):
        start_time = ''
        end_time = ''
        table = 'Generators and Scheduled Loads'
        cols = ['DUID', 'Technology Type - Primary']
        filter_cols = ('DUID',)
        data = data_fetch_methods.static_table_xl( start_time, end_time, table, defaults.raw_data_cache,
            select_columns=cols, filter_cols=filter_cols, filter_values=(['AGLHAL'],))
        expected_length = 1
        expected_number_of_columns = 2
        self.assertEqual(expected_length, data.shape[0])
        self.assertEqual(expected_number_of_columns, data.shape[1])
        print('Passed')


class TestCustomTables(unittest.TestCase):
    def setUp(self):
        pass

    def test_dispatch_tables_stradle_years(self):
        table = 'FCAS_4s_SCADA_MAP'
        start_time = '2012/01/01 00:05:00'
        end_time = '2012/01/01 01:00:00'
        print('Testing custom table {}.'.format(table))
        data = custom_tables.fcas4s_scada_match(start_time, end_time, table, defaults.raw_data_cache)
        data = data.reset_index(drop=True)
        contains_duplicates = data.duplicated(['MARKETNAME']).any()
        self.assertEqual(False, contains_duplicates)
        contains_duplicates = data.duplicated(['ELEMENTNUMBER']).any()
        self.assertEqual(False, contains_duplicates)
        not_empty = data.shape[0] > 0
        self.assertEqual(True, not_empty)
        print('Passed')
