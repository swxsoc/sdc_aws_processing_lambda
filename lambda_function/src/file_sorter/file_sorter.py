"""
This Module contains the FileSorter class that will sort the files into the appropriate
HERMES instrument folder.

TODO: Skeleton Code for initial repo, class still needs to be implemented including
logging to DynamoDB + S3 log file and docstrings expanded
"""

import logging
import boto3

logger = logging.getLogger()
# Debug mode used for development
# logger.setLevel(logging.DEBUG)


class FileSorter:
    """
    Main FileSorter class which initializes an object with the data file and the
    bucket event which triggered the lambda function to be called.
    """

    def __init__(self, file_key):
        """
        FileSorter Constructor
        """

        # Initialize Class Variables
        self.bucket = "swsoc-incoming"
        self.file_key = file_key

    def sort_file(self):
        """
        Function that chooses calls correct sorting function
        based off file key name.
        """

        if "spani" in self.file_key:
            self._sort_spani_file()

        elif "nemesis" in self.file_key:
            self._sort_nemesis_file()

        elif "eea" in self.file_key:
            self._sort_eea_file()

        elif "merit" in self.file_key:
            self._sort_merit_file()

        else:
            logger.error("ERROR: Not a valid file key")
            raise Exception("Not a valid file key event")

    def _copy_from_incoming_to_bucket(self, destination_bucket):

        """
        Function to copy file from S3 incoming bucket using bucket key to destination bucket
        """
        logger.info("Moving File: %s", self.file_key)
        try:
            # TODO: Add downloading logic
            s3 = boto3.resource("s3")
            copy_source = {"Bucket": self.bucket, "Key": self.file_key}
            # Copy S3 file from incoming bucket to destination bucket
            s3.meta.client.copy(copy_source, destination_bucket, self.file_key)
            logger.info("File Moved Successfully: %s", self.file_key)

            # Remove file from incoming bucket
            logger.info("Removing File From Incoming Bucket: %s", self.file_key)
            obj = s3.Object(self.bucket, self.file_key)
            # obj.delete()
            logger.info("File Removed Successfully: %s", self.file_key)
            return True

        except:
            logger.error("Error when Moving File: %s", self.file_key)
            return None

    def _log_status_in_dynamo_db(self):
        """
        This function logs the if the file was sorted successfully/unsuccessfully
        in a DynamoDB table
        TODO: Add logging logic
        """

    def _log_status_in_s3(self):
        """
        This function logs the if the file was sorted successfully/unsuccessfully
        in a logfile in an S3 Bucket
        TODO: Add logging logic
        """

    def _sort_spani_file(self):
        """
        This function sorts file into the SPAN-i Instrument Bucket
        TODO: Add sorting logic
        """
        logger.info("Sorting SPAN-i Instrument File: %s", self.file_key)
        try:
            # self._copy_from_incoming_to_bucket("hermes-spani")
            logger.info("File Sorted Successfully: %s", self.file_key)

        except:
            logger.error("Error when Sorting File: %s", self.file_key)

    def _sort_nemesis_file(self):
        """
        This function sorts file into the NEMESIS Instrument Bucket
        TODO: Add processing logic
        """
        logger.info("Sorting NEMESIS Instrument File: %s", self.file_key)
        try:
            # self._copy_from_incoming_to_bucket("hermes-nemesis")
            logger.info("File Sorted Successfully: %s", self.file_key)

        except:
            logger.error("Error when Sorting File: %s", self.file_key)

    def _sort_eea_file(self):
        """
        This function sorts file into the EEA Instrument Bucket
        TODO: Add sorting logic
        """
        logger.info("Sorting EEA Instrument File: %s", self.file_key)
        try:
            # self._copy_from_incoming_to_bucket("hermes-eea")
            logger.info("File Sorted Successfully: %s", self.file_key)

        except:
            logger.error("Error when Sorting File: %s", self.file_key)

    def _sort_merit_file(self):
        """
        This function sorts file into the MERIT Instrument Bucket
        TODO: Add sorting logic
        """
        logger.info("Sorting MERIT Instrument File: %s", self.file_key)
        try:
            # self._copy_from_incoming_to_bucket("hermes-merit")
            logger.info("File Sorted Successfully: %s", self.file_key)

        except:
            logger.error("Error when Sorting File: %s", self.file_key)
