"""
This module contains the handler function and the main function
which contains the logicthat initializes the FileProcessor class
in it's correct environment.
"""

from typing import Any

from file_processor import file_processor


def handler(event: dict[str, Any], context: Any) -> dict[str, int | str]:
    """
    Handle the AWS Lambda invocation.

    Parameters
    ----------
    event : dict[str, Any]
        Lambda event payload, typically containing S3 event records.
    context : Any
        AWS Lambda runtime context object.

    Returns
    -------
    dict[str, int | str]
        Response dictionary containing ``statusCode`` and serialized ``body``.
    """

    return file_processor.handle_event(event, context)
