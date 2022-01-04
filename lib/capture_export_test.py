import capture_export
import unittest   # The test framework

class Test_TestIncrementDecrement(unittest.TestCase):
    def test_capture_export(self):
        import psycopg2
        # read env variables DB_URL
        import os
        print("capture export");
        # read env variables DB_URL
        DB_URL = os.environ['DB_URL']
        print("DB_URL:", DB_URL)
        CKAN_DOMAIN = os.environ['CKAN_DOMAIN']
        # assert CKAN_DOMAIN exists
        self.assertTrue(CKAN_DOMAIN)
        conn = psycopg2.connect(DB_URL, sslmode='require')
        # execute capture_export.capture_export(conn, '2020-12') should throw an error
        # try:
        capture_export.capture_export(conn, '2020-12-31', 1)
        # self.assertTrue(False)
        # except Exception as e:
        #     print("get error when exec SQL:", e)
        #     self.assertTrue(True)

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()