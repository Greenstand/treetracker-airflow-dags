import pre_request
import unittest   # The test framework

class Test(unittest.TestCase):
    def test(self):
        # read env variables DB_URL
        import os
        print("grower export");
        # read env variables DB_URL
        result = pre_request.pre_request('http://www.google.com/xxxxxxxx')
        # use unittest to check the result
        # self.assertTrue(result)

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()