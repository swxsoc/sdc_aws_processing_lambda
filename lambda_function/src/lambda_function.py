"""
This module contains the handler function and the main function which contains the logic
that initializes the FileSorter class in it's correct environment.

TODO: Skeleton Code for initial repo, logic still needs to be implemented and docstrings expanded
"""

import logging
import json
import os
from file_sorter.file_sorter import FileSorter

logger = logging.getLogger()
# Debug used for development
logger.setLevel(logging.DEBUG)


def handler(event, context):
    """
    This is the lambda handler function that passes variables to the function that
    handles the logic that initializes the FileProcessor class in it's correct environment.
    """
    # Extract needed information from event
    try:
        file_key = event["FileKey"]
        environment = os.getenv("LAMBDA_ENVIRONMENT")
        # Pass required variables to sort function and returns a 200 (Successful) / 500 (Error) HTTP response
        response = sort_file(environment, file_key)

        return response

    except:
        return {
            "statusCode": 500,
            "body": json.dumps("Error Extracting Variables from Event"),
        }


def sort_file(environment, file_key):
    """
    This is the main function that handles logic that initializes the FileProcessor class
    in it's correct environment.
    """

    # Production (Official Release) Environment / Local Development
    if environment == "PRODUCTION" and "dev_" not in file_key:
        try:
            logger.info("Initializing FileSorter - Environment: Production")
            sorter = FileSorter(file_key)
            logger.warning("FileSorter Initialized Successfully")
            sorter.sort_file()

            return {"statusCode": 200, "body": json.dumps("File Sorted Successfully")}

        except:
            logger.error("Error occurred with FileSorter")

            return {"statusCode": 500, "body": json.dumps("Error Sorting File")}

    # Development (Master Branch but not Official Release) Environment
    elif environment == "DEVELOPMENT" and "dev_" in file_key:
        try:
            logger.info("Initializing FileSorter - Environment: Development")
            sorter = FileSorter(file_key)
            logger.warning("FileSorter Initialized Successfully")
            sorter.sort_file()

            return {"statusCode": 200, "body": json.dumps("File Sorted Successfully")}

        except:
            logger.error("Error occurred with FileSorter")

            return {"statusCode": 500, "body": json.dumps("Error Sorting File")}

    else:
        return {"statusCode": 500, "body": json.dumps("Invalid key for environment")}
