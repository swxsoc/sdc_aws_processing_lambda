"""
This module contains the handler function and the main function
which contains the logicthat initializes the FileProcessor class
in it's correct environment.
"""

from file_processor import file_processor


def handler(event, context) -> dict:
    """
    This is the lambda handler function that acts as a proxy
    to the main function handle_event

    :param event: Event data passed from the lambda trigger
    :type event: dict
    :param context: Lambda context
    :type context: dict
    :return: Returns a 200 (Successful) / 500 (Error) HTTP response
    :rtype: dict
    """

    return file_processor.handle_event(event, context)
