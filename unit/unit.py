import sys
import unittest
from typing import Optional


# Python Version Check.
class Checker:

    def version(self):
        major: Optional[int] = sys.version_info[0]
        minor: Optional[int] = sys.version_info[1]
        print("Python " + str(major) + "." + str(minor) + " :", id(self))


# Unit Test.
class VersionTest(unittest.TestCase):

    @classmethod
    def setupClass(cls):
        print("setupClass")

    @classmethod
    def tearDownClass(cls):
        print("tearDownClass")

    # Create Instance.
    def setUp(self):
        self.checker = Checker()
        print("setUp", id(self))

    def tearDown(self):
        self.checker.version()
        print("tearDown", id(self))

    # Python Version: 3.x == 3.x
    def test_check_major(self):
        py_major: Optional[int] = sys.version_info[0]
        major_calc = int(py_major)
        set_major = int(3)
        print("test_check_major", id(self))
        self.assertEqual(major_calc, set_major)

    # Python Version: (x.11 or x.10) > x.4
    def test_check_minor(self):
        py_minor: Optional[int] = sys.version_info[1]
        minor_calc = int(py_minor)
        set_minor = int(4)
        print("test_check_minor", id(self))
        self.assertGreater(minor_calc, set_minor)


if __name__ == '__main__':
    unittest.main()
