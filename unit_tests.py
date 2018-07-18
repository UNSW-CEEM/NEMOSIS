import pandas as pd
import gui
import os
import defaults
from nose.tools import assert_dict_equal


def test_1(app, test_session):
    table = 'DISPATCHLOAD'
    table_index = defaults.return_tables.index(table)
    row_state = {}
    row_state['name'] = 'test_1'
    row_state['type'] = 'query'
    row_state['start_time'] = '2018/04/01 00:05:00'
    row_state['end_time'] = '2018/04/01 00:10:00'
    row_state['table'] = (table_index,)
    columns = ['SETTLEMENTDATE', 'DUID', 'INITIALMW', 'INTERVENTION']
    column_indexes = [defaults.table_columns[table].index(column) for column in columns]
    row_state['columns'] = column_indexes
    row_state['filters_contents'] = {}
    row_state['filters_contents']['DUID'] = ['AGLHAL']
    row_state['filters_selection'] = {}
    row_state['filters_selection']['DUID'] = (0,)
    test_session['rows'] = []
    test_session['rows'].append(row_state)
    app.load_session(test_session)
    app.run_queries()

    actual_result = {'SETTLEMENTDATE': [row_state['start_time'], row_state['start_time']],
                     'DUID': ['AGLHAL', 'AGLHAL'],
                     'INITIALMW': ['40', '40.16999'],
                     'INTERVENTION': ['0', '1']}
    test_result = pd.read_csv(test_session['raw_data_location'] + '/' + row_state['name'] + '.csv', dtype=str)
    assert_dict_equal(actual_result, test_result.to_dict('list'), 'FAILED TEST ONE')
    print('PASSED TEST ONE')


def test_2(app, test_session):
    table = 'DISPATCHLOAD'
    table_index = defaults.return_tables.index(table)
    row_state = {}
    row_state['name'] = 'test_1'
    row_state['type'] = 'query'
    row_state['start_time'] = '2018/04/01 00:00:00'
    row_state['end_time'] = '2018/04/01 00:05:00'
    row_state['table'] = (table_index,)
    columns = ['SETTLEMENTDATE', 'DUID', 'INITIALMW', 'INTERVENTION']
    column_indexes = [defaults.table_columns[table].index(column) for column in columns]
    row_state['columns'] = column_indexes
    row_state['filters_contents'] = {}
    row_state['filters_contents']['DUID'] = ['AGLHAL']
    row_state['filters_selection'] = {}
    row_state['filters_selection']['DUID'] = (0,)
    test_session['rows'] = []
    test_session['rows'].append(row_state)
    app.load_session(test_session)
    app.run_queries()

    actual_result = {'SETTLEMENTDATE': [row_state['start_time'], row_state['start_time']],
                     'DUID': ['AGLHAL', 'AGLHAL'],
                     'INITIALMW': ['40', '39.36125'],
                     'INTERVENTION': ['0', '1']}
    test_result = pd.read_csv(test_session['raw_data_location'] + '/' + row_state['name'] + '.csv', dtype=str)
    assert_dict_equal(actual_result, test_result.to_dict('list'), 'FAILED TEST TWO')
    print('PASSED TEST TWO')


def test_3(app, test_session):

    """Test retrieving dispatch data from two adjoining month files"""

    table = 'DISPATCHLOAD'
    table_index = defaults.return_tables.index(table)
    row_state = {}
    row_state['name'] = 'test_1'
    row_state['type'] = 'query'
    row_state['start_time'] = '2018/04/01 00:00:00'
    row_state['end_time'] = '2018/04/01 00:10:00'
    row_state['table'] = (table_index,)
    columns = ['SETTLEMENTDATE', 'DUID', 'INITIALMW', 'INTERVENTION']
    column_indexes = [defaults.table_columns[table].index(column) for column in columns]
    row_state['columns'] = column_indexes
    row_state['filters_contents'] = {}
    row_state['filters_contents']['DUID'] = ['AGLHAL']
    row_state['filters_selection'] = {}
    row_state['filters_selection']['DUID'] = (0,)
    test_session['rows'] = []
    test_session['rows'].append(row_state)
    app.load_session(test_session)
    app.run_queries()

    actual_result = {'SETTLEMENTDATE': [row_state['start_time'], row_state['start_time'], '2018/04/01 00:05:00',
                                        '2018/04/01 00:05:00'],
                     'DUID': ['AGLHAL', 'AGLHAL', 'AGLHAL', 'AGLHAL'],
                     'INITIALMW': ['40', '39.36125', '40', '40.16999'],
                     'INTERVENTION': ['0', '1', '0', '1']}
    test_result = pd.read_csv(test_session['raw_data_location'] + '/' + row_state['name'] + '.csv', dtype=str)
    assert_dict_equal(actual_result, test_result.to_dict('list'), 'FAILED TEST THREE')
    print('PASSED TEST THREE')


def test_4(app, test_session):

    """Test retrieving dispatch data from two adjoining month files"""

    table = 'DUDETAILSUMMARY'
    table_index = defaults.return_tables.index(table)
    row_state = {}
    row_state['name'] = 'test_1'
    row_state['type'] = 'query'
    row_state['start_time'] = '2018/04/01 00:00:00'
    row_state['end_time'] = '2018/04/01 00:10:00'
    row_state['table'] = (table_index,)
    columns = ['DUID', 'START_DATE', 'END_DATE', 'REGIONID']
    column_indexes = [defaults.table_columns[table].index(column) for column in columns]
    row_state['columns'] = column_indexes
    row_state['filters_contents'] = {}
    row_state['filters_contents']['DUID'] = ['AGLHAL']
    row_state['filters_selection'] = {}
    row_state['filters_selection']['DUID'] = (0,)
    test_session['rows'] = []
    test_session['rows'].append(row_state)
    app.load_session(test_session)
    app.run_queries()

    actual_result = {'START_DATE': ['2017/07/01 00:00:00'],
                     'END_DATE': ['2100/12/31 00:00:00'],
                     'DUID': ['AGLHAL'],
                     'REGIONID': ['SA1']}
    test_result = pd.read_csv(test_session['raw_data_location'] + '/' + row_state['name'] + '.csv', dtype=str)
    assert_dict_equal(actual_result, test_result.to_dict('list'), 'FAILED TEST FOUR')
    print('PASSED TEST FOUR')


def test_5(app, test_session):

    """Test retrieving dispatch data from two adjoining month files"""

    table = 'DUDETAILSUMMARY'
    table_index = defaults.return_tables.index(table)
    row_state = {}
    row_state['name'] = 'test_1'
    row_state['type'] = 'query'
    row_state['start_time'] = '2016/04/01 00:00:00'
    row_state['end_time'] = '2018/04/01 00:10:00'
    row_state['table'] = (table_index,)
    columns = ['DUID', 'START_DATE', 'END_DATE', 'REGIONID']
    column_indexes = [defaults.table_columns[table].index(column) for column in columns]
    row_state['columns'] = column_indexes
    row_state['filters_contents'] = {}
    row_state['filters_contents']['DUID'] = ['AGLHAL', 'MPP_1']
    row_state['filters_selection'] = {}
    row_state['filters_selection']['DUID'] = (0, 1)
    test_session['rows'] = []
    test_session['rows'].append(row_state)
    app.load_session(test_session)
    app.run_queries()

    actual_result = {'START_DATE': ['2017/07/01 00:00:00', '2016/07/01 00:00:00', '2015/07/01 00:00:00',
                                    '2017/07/01 00:00:00', '2017/05/12 00:00:00','2017/04/01 00:00:00',
                                    '2016/07/01 00:00:00', '2015/07/01 00:00:00'],
                     'END_DATE': ['2100/12/31 00:00:00', '2017/07/01 00:00:00', '2016/07/01 00:00:00',
                                  '2100/12/31 00:00:00','2017/07/01 00:00:00', '2017/05/12 00:00:00',
                                  '2017/04/01 00:00:00', '2016/07/01 00:00:00'],
                     'DUID': ['AGLHAL', 'AGLHAL', 'AGLHAL', 'MPP_1', 'MPP_1', 'MPP_1', 'MPP_1', 'MPP_1'],
                     'REGIONID': ['SA1', 'SA1', 'SA1', 'QLD1', 'QLD1', 'QLD1', 'QLD1', 'QLD1']}
    test_result = pd.read_csv(test_session['raw_data_location'] + '/' + row_state['name'] + '.csv', dtype=str)
    test_result = test_result.sort_values('START_DATE', ascending=False)
    test_result = test_result.sort_values('DUID')
    assert_dict_equal(actual_result, test_result.to_dict('list'), 'FAILED TEST FIVE')
    print('PASSED TEST FIVE')


def test_6(app, test_session):

    """Test retrieving dispatch data from two adjoining month files"""

    table = 'DISPATCHCONSTRAINT'
    table_index = defaults.return_tables.index(table)
    row_state = {}
    row_state['name'] = 'test_1'
    row_state['type'] = 'query'
    row_state['start_time'] = '2010/04/01 00:00:00'
    row_state['end_time'] = '2010/04/01 00:05:00'
    row_state['table'] = (table_index,)
    columns = ['SETTLEMENTDATE', 'CONSTRAINTID', 'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO', 'RHS']
    column_indexes = [defaults.table_columns[table].index(column) for column in columns]
    row_state['columns'] = column_indexes
    row_state['filters_contents'] = {}
    row_state['filters_contents']['CONSTRAINTID'] = ['$GSTONE6']
    row_state['filters_selection'] = {}
    row_state['filters_selection']['CONSTRAINTID'] = (0,)
    test_session['rows'] = []
    test_session['rows'].append(row_state)
    app.load_session(test_session)
    app.run_queries()

    actual_result = {'SETTLEMENTDATE': ['2010/04/01 00:00:00'],
                     'RHS': ['110'],
                     'CONSTRAINTID': ['$GSTONE6'],
                     'GENCONID_EFFECTIVEDATE': [''],
                     'GENCONID_VERSIONNO': ['']}
    test_result = pd.read_csv(test_session['raw_data_location'] + '/' + row_state['name'] + '.csv', dtype=str)
    if not actual_result == test_result.to_dict('list'):
        assert_dict_equal(actual_result, test_result.fillna('').to_dict('list'), 'FAILED TEST SIX')
    print('PASSED TEST SIX')


def test_7(app, test_session):

    """Test retrieving dispatch data from two adjoining month files"""

    table = 'GENCONDATA'
    table_index = defaults.return_tables.index(table)
    row_state = {}
    row_state['name'] = 'test_1'
    row_state['type'] = 'query'
    row_state['start_time'] = '2010/04/01 00:00:00'
    row_state['end_time'] = '2010/04/01 00:05:00'
    row_state['table'] = (table_index,)
    columns = ['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'CONSTRAINTTYPE', 'CONSTRAINTVALUE']
    column_indexes = [defaults.table_columns[table].index(column) for column in columns]
    row_state['columns'] = column_indexes
    row_state['filters_contents'] = {}
    row_state['filters_contents']['GENCONID'] = ['$GSTONE6']
    row_state['filters_selection'] = {}
    row_state['filters_selection']['GENCONID'] = (0,)
    test_session['rows'] = []
    test_session['rows'].append(row_state)
    app.load_session(test_session)
    app.run_queries()

    actual_result = {'GENCONID': ['$GSTONE6'],
                     'EFFECTIVEDATE': [''],
                     'VERSIONNO': [''],
                     'CONSTRAINTTYPE': [''],
                     'CONSTRAINTVALUE': ['']}
    test_result = pd.read_csv(test_session['raw_data_location'] + '/' + row_state['name'] + '.csv', dtype=str)
    if not actual_result == test_result.to_dict('list'):
        assert_dict_equal(actual_result, test_result.fillna('').to_dict('list'), 'FAILED TEST SEVEN')
    print('PASSED TEST SEVEN')


def run_tests():
    root = gui.tk.Tk()
    app = gui.App(root)
    test_session = {}
    test_session['raw_data_location'] = 'E:/raw_aemo_data'
    test_session['save_location'] = 'E:/tests'
    test_1(app, test_session)
    test_2(app, test_session)
    test_3(app, test_session)
    test_4(app, test_session)
    test_5(app, test_session)
    test_6(app, test_session)
    test_7(app, test_session)

    return

run_tests()
