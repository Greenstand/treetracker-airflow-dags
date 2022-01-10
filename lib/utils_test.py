import utils
import unittest   # The test framework

class Test_Utils(unittest.TestCase):
    def test_get_start_and_end_date_of_previous_month(self):
        result = utils.get_start_and_end_date_of_previous_month('2020-12-31')
        self.assertEqual(result, ('2020-11-01', '2020-11-30'))
        result = utils.get_start_and_end_date_of_previous_month('2020-12-02')
        self.assertEqual(result, ('2020-11-01', '2020-11-30'))
        result = utils.get_start_and_end_date_of_previous_month('2020-11-02')
        self.assertEqual(result, ('2020-10-01', '2020-10-31'))

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()