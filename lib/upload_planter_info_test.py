import unittest

from lib.upload_planter_info import upload_planter_info   # The test framework

class Test_AssignNewTreesToCluster(unittest.TestCase):
    def test(self):
        import psycopg2
        # read env variables DB_URL
        import os
        print("capture export");
        # read env variables DB_URL
        DB_URL = os.environ['DB_URL']
        print("DB_URL:", DB_URL)
        conn = psycopg2.connect(DB_URL, sslmode='require')
        host = os.environ['FTP_HOST']
        user = os.environ['FTP_USER']
        password = os.environ['FTP_PASSWORD']
        upload_planter_info(conn, host, user, password, False);

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()