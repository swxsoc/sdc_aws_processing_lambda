"""
This module contains the handler function and the main function
which contains the logicthat initializes the FileProcessor class
in it's correct environment.

TODO: Skeleton Code for initial repo, logic still needs to be
implemented and docstrings expanded
"""

import logging
import json
from file_processor.file_processor import FileProcessor

logger = logging.getLogger()
# Debug used for development
logger.setLevel(logging.DEBUG)


def handler(event, context):
    """
    This is the lambda handler function that passes variables to
    the function that handles the logic that initializes the
    FileProcessor class in it's correct environment.
    """

    # Extract needed information from event
    try:
        bucket = event["Bucket"]
        file_key = event["FileKey"]

        """
        Pass required variables to process function and returns a
        200 (Successful) / 500 (Error) HTTP response
        """
        response = process_file(bucket, file_key)

        return response

    except BaseException as exception:
        logger.error("Error occurred with Lambda Function: %s", exception)
        return {
            "statusCode": 500,
            "body": json.dumps("Error Extracting Variables from Event"),
        }


def process_file(bucket, file_key):
    """
    This is the main function that handles logic that initializes
    the FileProcessor class in it's correct environment.
    """

    # Production (Official Release) Environment / Local Development
    if "dev_" not in file_key:
        try:
            logger.info("Initializing FileProcessor - Environment: Production")
            process = FileProcessor(bucket, file_key)
            logger.warning("FileProcessor Initialized Successfully")
            process.process_file()

            return {
                "statusCode": 200,
                "body": json.dumps("File Processed Successfully"),
            }

        except BaseException as exception:
            logger.error("Error occurred with FileProcessor: %s", exception)

            return {
                "statusCode": 500,
                "body": json.dumps("Error Processing File"),
            }

    # Development (Master Branch but not Official Release) Environment
    else:
        try:
            # This will be used to process files marked with the prefix 'dev_'
            # for the development pipeline (code in master but not in release).
            # pylint: disable=import-outside-toplevel
            from dev_file_processor.file_processor import (
                FileProcessor as DevFileProcessor,
            )

            logger.info(
                "Initializing FileProcessor - Environment: Development"
            )
            process = DevFileProcessor(bucket, file_key)
            logger.info("FileProcessor Initialized Successfully")
            process.process_file()

            return {
                "statusCode": 200,
                "body": json.dumps("File Processed Successfully"),
            }

        except BaseException as exception:
            logger.error("Error occurred with FileProcessor: %s", exception)

            return {
                "statusCode": 500,
                "body": json.dumps("Error Processing File"),
            }
