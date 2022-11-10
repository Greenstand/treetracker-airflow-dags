import unittest

from lib.assign_new_trees_to_cluster import assign_new_trees_to_cluster
from lib.pre_request_map_clusters import pre_request_map_clusters   # The test framework

class Test(unittest.TestCase):
    def test(self):
        pre_request_map_clusters("https://prod-k8s.treetracker.org/tiles");

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()