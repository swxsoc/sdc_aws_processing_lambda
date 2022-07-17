"""
This module tests the FileSorter class and it's functions
TODO: This is skeleton test for starter repo, still needs to be implemented
"""

from file_sorter.file_sorter import FileSorter


def test_initializing_class():
    """
    Test function that tests if class initializes correctly if passed correct variables.
    """
    test_file_key = "/dev_merit_file.txt"

    test_sorter = FileSorter(test_file_key)

    assert test_sorter is not None
