"""
This module tests the FileProcessor class and it's functions
TODO: This is skeleton test for starter repo, still needs to be implemented
"""

from file_processor.file_processor import FileProcessor


def test_initializing_class():
    """
    Test function that tests if class initializes correctly if
    passed correct variables.
    """
    test_bucket = "test_bucket"
    test_file_key = "/test_file_key.txt"

    test_process = FileProcessor(test_bucket, test_file_key)

    assert test_process is not None
