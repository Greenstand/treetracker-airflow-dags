import sync_entity_stakeholder
import unittest   # The test framework

class Test(unittest.TestCase):
    def test(self):
        import psycopg2
        # read env variables DB_URL
        import os
        # read env variables DB_URL
        DB_URL = os.environ['DB_URL']
        print("DB_URL:", DB_URL)
        conn = psycopg2.connect(DB_URL, sslmode='require')
        result = sync_entity_stakeholder.sync_entity_stakeholder(conn, False)
        # use unittest to check the result
        self.assertTrue(result)

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()