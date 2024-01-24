import unittest
import helpers
from datetime import datetime

class TestHelpers(unittest.TestCase):

    def test_get_dates_between(self):

        date_string_list = ["20230101", "20230102", "20230103", "20230104", "20230105"]
        expected_values = []
        actual_values = []
        for date_string in date_string_list:
            formatted_datetime = datetime.strptime(date_string, helpers.DATE_FORMAT).strftime(helpers.DATE_FORMAT)
            expected_values.append(formatted_datetime)

        for date in helpers.get_dates_between("20230101", "20230105"):
            actual_values.append(date)

        self.assertEqual(expected_values, actual_values)

    def test_convert_midnight_to_previous_day(self):
        expected_value_non_midnight_time = "17/12/2022 12:00"
        actual_value_non_midnight_time = helpers.convert_midnight_to_previous_day("17/12/2022 12:00")

        self.assertEqual(expected_value_non_midnight_time, actual_value_non_midnight_time)

        expected_value_midnight_time = "17/12/2022 23:59"
        actual_value_midnight_time = helpers.convert_midnight_to_previous_day("18/12/2022 0:00")

        self.assertEqual(expected_value_midnight_time, actual_value_midnight_time)

    def test_get_30_min_intervals(self):
        actual_intervals = helpers.get_30_min_intervals()
        expected_intervals = [
            '00:30', '01:00', '01:30', '02:00', '02:30', '03:00', '03:30', '04:00', '04:30', '05:00',
            '05:30', '06:00', '06:30', '07:00', '07:30', '08:00', '08:30', '09:00', '09:30', '10:00',
            '10:30', '11:00', '11:30', '12:00', '12:30', '13:00', '13:30', '14:00', '14:30', '15:00',
            '15:30', '16:00', '16:30', '17:00', '17:30', '18:00', '18:30', '19:00', '19:30', '20:00',
            '20:30', '21:00', '21:30', '22:00', '22:30', '23:00', '23:30', '00:00'
        ]
        #ask sachin if there is a better way than this?
        self.assertEqual(actual_intervals, expected_intervals)


if __name__ == "__main__":
    unittest.main()