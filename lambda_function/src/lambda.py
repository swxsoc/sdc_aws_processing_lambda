"""
This module contains the handler function and the main function
which contains the logicthat initializes the FileProcessor class
in it's correct environment.

TODO: Skeleton Code for initial repo, logic still needs to be
implemented and docstrings expanded
"""

import json
import os
import logging


from file_processor.file_processor import FileProcessor, log  # noqa: E402

# To remove boto3 noisy debug logging
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)


def handler(event, context) -> dict:
    """
    This is the lambda handler function that passes variables to the function that
    handles the logic that initializes the FileProcessor class in it's correct
    environment.

    :param event: Event data passed from the lambda trigger
    :type event: dict
    :param context: Lambda context
    :type context: dict
    :return: Returns a 200 (Successful) / 500 (Error) HTTP response
    :rtype: dict
    """
    # Extract needed information from event
    try:
        environment = os.getenv("LAMBDA_ENVIRONMENT")
        if environment is None:
            environment = "DEVELOPMENT"

        # Check if SNS or S3 event
        records = json.loads(event["Records"][0]["Sns"]["Message"])["Records"]

        # Parse message from SNS Notification
        for s3_event in records:
            # Extract needed information from event
            s3_bucket = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]

            # / 500 (Error) HTTP response
            response = environment_setup(
                s3_bucket=s3_bucket, file_key=file_key, environment=environment
            )

            return response

    except Exception as e:
        # Pass required variables to sort function and returns a 200 (Successful)
        # / 500 (Error) HTTP response
        log.error({"status": "ERROR", "message": e})

        return {
            "statusCode": 500,
            "body": json.dumps(f"Error Processing File: {e}"),
        }


def environment_setup(s3_bucket: str, file_key: str, environment: str) -> dict:
    """
    This is the main function that handles logic that initializes
    the FileProcessor class in it's correct environment.

    :param s3_bucket: The name of the S3 bucket the file is located in
    :type s3_bucket: str
    :param file_key: The name of the S3 object that is being processed
    :type file_key: str
    :param environment: The environment the FileProcessor is running in
    :type environment: str
    """

    # Production (Official Release) Environment / Local Development
    try:
        log.info(f"Initializing FileProcessor - Environment: {environment}")
        if environment == "Production":
            # Initialize FileProcessor class
            FileProcessor(
                s3_bucket=s3_bucket, file_key=file_key, environment=environment
            )

        else:
            # pylint: disable=import-outside-toplevel
            from dev_file_processor.file_processor import (
                FileProcessor as DevFileProcessor,
            )

            # Initialize FileProcessor class
            DevFileProcessor(
                s3_bucket=s3_bucket, file_key=file_key, environment=environment
            )

        return {
            "statusCode": 200,
            "body": json.dumps("File Processed Successfully"),
        }

    except Exception as e:
        log.error({"status": "ERROR", "message": e})

        return {
            "statusCode": 500,
            "body": json.dumps("Error Processing File"),
        }
