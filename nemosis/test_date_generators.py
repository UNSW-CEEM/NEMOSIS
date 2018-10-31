import unittest
from datetime import datetime
from nemosis import date_generators


class TestYearAndMonthGen(unittest.TestCase):
    def setUp(self):
        pass

    def test_two_times_not_at_edge_of_month_return_one_month(self):
        start_time = datetime.strptime('2017/01/02 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2017/01/03 00:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_and_month_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2017')
        self.assertEqual(times[0][1], '01')
        self.assertEqual(times[0][2], None)
        self.assertEqual(times[0][3], None)
        self.assertEqual(len(times), 1)


    def test_two_times_first_at_edge_of_month_return_month_before_and_month_of_times(self):
        start_time = datetime.strptime('2017/02/01 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2017/02/03 00:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_and_month_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2017')
        self.assertEqual(times[0][1], '01')
        self.assertEqual(times[0][2], None)
        self.assertEqual(times[0][3], None)
        self.assertEqual(times[1][0], '2017')
        self.assertEqual(times[1][1], '02')
        self.assertEqual(times[1][2], None)
        self.assertEqual(times[1][3], None)

    def test_two_times_first_at_edge_of_year_return_month_before_and_month_of_times(self):
        start_time = datetime.strptime('2017/01/01 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2017/01/03 00:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_and_month_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2016')
        self.assertEqual(times[0][1], '12')
        self.assertEqual(times[0][2], None)
        self.assertEqual(times[0][3], None)
        self.assertEqual(times[1][0], '2017')
        self.assertEqual(times[1][1], '01')
        self.assertEqual(times[1][2], None)
        self.assertEqual(times[1][3], None)

    def test_two_times_second_at_edge_of_month_returns_one_month(self):
        start_time = datetime.strptime('2017/01/05 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2017/01/31 00:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_and_month_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2017')
        self.assertEqual(times[0][1], '01')
        self.assertEqual(times[0][2], None)
        self.assertEqual(times[0][3], None)
        self.assertEqual(len(times), 1)

    def test_two_times_second_at_edge_of_year_returns_one_month(self):
        start_time = datetime.strptime('2017/12/02 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2017/12/31 00:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_and_month_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2017')
        self.assertEqual(times[0][1], '12')
        self.assertEqual(times[0][2], None)
        self.assertEqual(times[0][3], None)
        self.assertEqual(len(times), 1)

    def test_two_times_in_middle_of_jan_and_march_return_3_months(self):
        start_time = datetime.strptime('2017/01/05 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2017/03/24 00:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_and_month_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2017')
        self.assertEqual(times[0][1], '01')
        self.assertEqual(times[0][2], None)
        self.assertEqual(times[0][3], None)
        self.assertEqual(times[1][0], '2017')
        self.assertEqual(times[1][1], '02')
        self.assertEqual(times[1][2], None)
        self.assertEqual(times[1][3], None)
        self.assertEqual(times[2][0], '2017')
        self.assertEqual(times[2][1], '03')
        self.assertEqual(times[2][2], None)
        self.assertEqual(times[2][3], None)
        self.assertEqual(len(times), 3)


class TestYearMonthDayIndexGen(unittest.TestCase):
    def setUp(self):
        pass

    def test_two_times_in_middle_of_adjacent_hours(self):
        # Whole of each hour should be returned.
        start_time = datetime.strptime('2013/01/05 12:20:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2013/01/05 13:45:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_month_day_index_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2013')
        self.assertEqual(times[0][1], '01')
        self.assertEqual(times[0][2], '05')
        self.assertEqual(times[0][3], '1355')
        self.assertEqual(times[-1][0], '2013')
        self.assertEqual(times[-1][1], '01')
        self.assertEqual(times[-1][2], '05')
        self.assertEqual(times[-1][3], '1200')
        self.assertEqual(len(times), 24)

    def test_two_times_one_at_start_of_year_should_not_over_flow_to_previous_year(self):
        start_time = datetime.strptime('2013/01/01 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2013/01/01 01:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_month_day_index_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2013')
        self.assertEqual(times[0][1], '01')
        self.assertEqual(times[0][2], '01')
        self.assertEqual(times[0][3], '0155')
        self.assertEqual(times[-1][0], '2013')
        self.assertEqual(times[-1][1], '01')
        self.assertEqual(times[-1][2], '01')
        self.assertEqual(times[-1][3], '0000')
        self.assertEqual(len(times), 24)

    def test_two_times_one_at_start_of_month_should_not_over_flow_to_previous_month(self):
        start_time = datetime.strptime('2013/02/01 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2013/02/01 01:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_month_day_index_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2013')
        self.assertEqual(times[0][1], '02')
        self.assertEqual(times[0][2], '01')
        self.assertEqual(times[0][3], '0155')
        self.assertEqual(times[-1][0], '2013')
        self.assertEqual(times[-1][1], '02')
        self.assertEqual(times[-1][2], '01')
        self.assertEqual(times[-1][3], '0000')
        self.assertEqual(len(times), 24)

    def test_no_missing_values_in_a_week(self):
        start_time = datetime.strptime('2013/02/01 00:00:00', '%Y/%m/%d %H:%M:%S')
        end_time = datetime.strptime('2013/02/07 00:00:00', '%Y/%m/%d %H:%M:%S')
        gen = date_generators.year_month_day_index_gen(start_time, end_time)
        times = [(year, month, day, index) for year, month, day, index in gen]
        self.assertEqual(times[0][0], '2013')
        self.assertEqual(times[0][1], '02')
        self.assertEqual(times[0][2], '01')
        self.assertEqual(times[0][3], '2355')
        self.assertEqual(times[-1][0], '2013')
        self.assertEqual(times[-1][1], '02')
        self.assertEqual(times[-1][2], '07')
        self.assertEqual(times[-1][3], '0000')
        self.assertEqual(len(times), 1740)

