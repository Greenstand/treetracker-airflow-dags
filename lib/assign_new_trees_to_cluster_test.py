import capture_export
import unittest

from lib.assign_new_trees_to_cluster import assign_new_trees_to_cluster   # The test framework

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
        assign_new_trees_to_cluster(conn, True);

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()