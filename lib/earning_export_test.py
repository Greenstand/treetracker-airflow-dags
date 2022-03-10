import earning_export
import utils
import unittest   # The test framework

class Test_TestIncrementDecrement(unittest.TestCase):
    def test_earning_export(self):
        import psycopg2
        # read env variables DB_URL
        import os
        print("earning export");
        # read env variables DB_URL
        DB_URL = os.environ['DB_URL']
        print("DB_URL:", DB_URL)
        CKAN_DOMAIN = os.environ['CKAN_DOMAIN']
        # assert CKAN_DOMAIN exists
        self.assertTrue(CKAN_DOMAIN)
        CKAN_DATASET_NAME_EARNING_DATA = os.environ['CKAN_DATASET_NAME_EARNING_DATA']
        self.assertTrue(CKAN_DATASET_NAME_EARNING_DATA)
        CKAN_API_KEY = os.environ['CKAN_API_KEY']
        self.assertTrue(CKAN_API_KEY)
        # dict of env variables
        ckan_config = {
            "CKAN_DOMAIN": CKAN_DOMAIN,
            "CKAN_DATASET_NAME_EARNING_DATA": CKAN_DATASET_NAME_EARNING_DATA,
            "CKAN_API_KEY": CKAN_API_KEY,
        }
        conn = psycopg2.connect(DB_URL, sslmode='require')
        # execute earning_export.earning_export(conn, '2020-12') should throw an error
        # try:
        date = '2022-01-01'
        start_end = utils.get_start_and_end_date_of_previous_month(date)
        earning_export.earning_export(conn, start_end[0], start_end[1], ckan_config)
        # self.assertTrue(False)
        # except Exception as e:
        #     print("get error when exec SQL:", e)
        #     self.assertTrue(True)
        # self.assertTrue(False)
        # except Exception as e:
        #     print("get error when exec SQL:", e)
        #     self.assertTrue(True)

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()