import messaging
import unittest   # The test framework

class Test(unittest.TestCase):
    def test(self):
        import psycopg2
        # read env variables DB_URL
        import os
        print("create authors");
        # read env variables DB_URL
        DB_URL = os.environ['DB_URL']
        print("DB_URL:", DB_URL)
        conn = psycopg2.connect(DB_URL, sslmode='require')
        result = messaging.create_authors(conn, None)
        # use unittest to check the result

class Test2(unittest.TestCase):
    def test(self):
        import psycopg2
        # read env variables DB_URL
        import os
        print("create authors");
        # read env variables DB_URL
        DB_URL = os.environ['DB_URL']
        print("DB_URL:", DB_URL)
        conn = psycopg2.connect(DB_URL, sslmode='require')
        result = messaging.create_authors(conn, True)
        # use unittest to check the result

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()