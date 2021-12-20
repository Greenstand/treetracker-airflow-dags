import capture_export
import unittest   # The test framework

class Test_TestIncrementDecrement(unittest.TestCase):
    def test_capture_export(self):
        print("capture export");
        self.assertEqual(capture_export.capture_export(), "Done")

if __name__ == '__main__':
    # Run the unit tests in the test suite with name 'Test_TestIncrementDecrement'
    unittest.main()