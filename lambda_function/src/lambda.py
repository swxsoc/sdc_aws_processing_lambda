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


def handler(event, context):
    """
    This is the lambda handler function that passes variables to the function that
    handles the logic that initializes the FileProcessor class in it's correct
    environment.
    """
    # Extract needed information from event
    try:

        environment = os.getenv("LAMBDA_ENVIRONMENT")
        if environment is None:
            environment = "DEVELOPMENT"

        for s3_event in event["Records"]:

            s3_bucket = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]
            # Pass required variables to sort function and returns a 200 (Successful)
            # / 500 (Error) HTTP response
            response = process_file(
                environment=environment, s3_bucket=s3_bucket, file_key=file_key
            )

            return response

    except BaseException as e:

        # Pass required variables to sort function and returns a 200 (Successful)
        # / 500 (Error) HTTP response
        log.error({"status": "ERROR", "message": e})

        return {"statusCode": 500, "body": json.dumps(f"Error Processing File: {e}")}


def process_file(s3_bucket, file_key, environment):
    """
    This is the main function that handles logic that initializes
    the FileProcessor class in it's correct environment.
    """

    # Production (Official Release) Environment / Local Development
    try:
        log.info(f"Initializing FileProcessor - Environment: {environment}")
        # Parse file key to get instrument name
        # file_key_array = file_key.split("/")
        # parsed_file_key = file_key_array[-1]
        # science_file = util.parse_science_filename(parsed_file_key)
        # print(science_file)
        if environment == "Production":
            FileProcessor(
                s3_bucket=s3_bucket, s3_object=file_key, environment=environment
            )
        else:
            # pylint: disable=import-outside-toplevel
            from dev_file_processor.file_processor import (
                FileProcessor as DevFileProcessor,
            )

            DevFileProcessor(
                s3_bucket=s3_bucket, s3_object=file_key, environment=environment
            )

            log.info("File Processed Successfully")

        return {"statusCode": 200, "body": json.dumps("File Processed Successfully")}

    except BaseException as e:
        log.error({"status": "ERROR", "message": e})

        return {"statusCode": 500, "body": json.dumps("Error Processing File")}
