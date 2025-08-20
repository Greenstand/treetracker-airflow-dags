import subprocess
import unittest


class TestUsingFunctionsFromExternalRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        """Install treetracker functions repository as a package"""
        cls.result = subprocess.run(
            ["pip3", "install", "git+https://github.com/Greenstand/treetracker-functions.git@dev"
                                "#egg=functions"]
            , stdout=subprocess.PIPE)

    def test_treetracker_functions_repository_is_successfully_installed(self):
        self.assertEqual(self.result.returncode, 0)

    def test_hello_function_from_tree_tracker_functions_repository(self):
        from functions.refresh_view import hello  # noqa
        self.assertEqual(hello('Chen'), 'Hello, Chen!')


if __name__ == "__main__":
    unittest.main()
