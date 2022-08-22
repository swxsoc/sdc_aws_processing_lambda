"""
This Module contains the FileProcessor class that will distinguish
the appropriate HERMES intrument library to use when processing
the file based off which bucket the file is located in.

TODO: Skeleton Code for initial repo, class still needs to be
implemented including logging to DynamoDB + S3 log
file and docstrings expanded
"""

import boto3
import botocore

# The below flake exceptions are to avoid the hermes.log writing
# issue the above line solves
from hermes_core import log  # noqa: E402
from hermes_core.util import util  # noqa: E402

# Starts boto3 session so it gets access to needed credentials
session = boto3.Session()

# Dict with instrument bucket names
INSTRUMENT_BUCKET_NAMES = {
    "eea": "hermes-eea",
    "nemisis": "hermes-nemisis",
    "merit": "hermes-merit",
    "spani": "hermes-spani",
}


class FileProcessor:
    """
    Main FileProcessor class which initializes an object with the data file and the
    bucket event which triggered the lambda function to be called.
    """

    def __init__(self, s3_bucket, s3_object, environment, dry_run=False):
        """
        FileProcessor Constructorlogger
        """

        # Initialize Class Variables
        try:
            self.instrument_bucket_name = s3_bucket
            log.info(
                "Instrument Bucket Name Parsed Successfully:"
                f"{self.instrument_bucket_name}"
            )

        except KeyError:
            error_message = "KeyError when extracting S3 Bucket Name/ARN from dict"
            log.error({"status": "ERROR", "message": error_message})
            raise KeyError(error_message)

        try:
            self.file_key = s3_object

            log.info(
                {
                    "status": "INFO",
                    "message": "Incoming Object Name"
                    f"Parsed Successfully: {self.file_key}",
                }
            )

        except KeyError:
            error_message = "KeyError when extracting S3 Object Name/eTag from dict"
            log.error({"status": "ERROR", "message": error_message})
            raise KeyError(error_message)

        # Variable that determines environment
        self.environment = environment

        # Variable that determines if FileProcessor performs a Dry Run
        self.dry_run = dry_run
        if self.dry_run:
            log.warning("Performing Dry Run - Files will not be copied/removed")

        # Process File
        self._process_file(file_key=self.file_key)

    def _process_file(self, file_key):
        # Verify object exists in instrument bucket
        if (
            self._does_object_exists(
                bucket=self.instrument_bucket_name, file_key=self.file_key
            )
            or self.dry_run
        ):
            try:

                # Parse file key to get instrument name
                parsed_file_key = file_key.replace("unprocessed/", "")
                science_file = util.parse_science_filename(parsed_file_key)

                destination_bucket = INSTRUMENT_BUCKET_NAMES[science_file["instrument"]]

                log.info(
                    f"Destination Bucket Parsed Successfully: {destination_bucket}"
                )

                instrument_calibration = ""

                if destination_bucket == "hermes-eea":
                    from hermes_eea.calibration import calibration

                    log.info("Using hermes_eea module for calibration")
                    instrument_calibration = calibration

                elif destination_bucket == "hermes-nemisis":
                    from hermes_nemisis.calibration import calibration

                    log.info("Using hermes_nemisis module for calibration")
                    instrument_calibration = calibration

                elif destination_bucket == "hermes-merit":
                    from hermes_merit.calibration import calibration

                    log.info("Using hermes_merit module for calibration")
                    instrument_calibration = calibration

                elif destination_bucket == "hermes-spani":
                    from hermes_spani.calibration import calibration

                    log.info("Using hermes_spani module for calibration")
                    instrument_calibration = calibration
                else:
                    log.error({"status": "ERROR", "message": "Instrument Not Found"})
                    raise KeyError("Instrument Not Found")

                # Run Calibration on File (This will cause a ValueError
                # if no calibration is found)
                instrument_calibration.calibrate_file(file_key)

            except ValueError as e:
                # Expected ValueError for Data Flow Test because no calibration
                # files are ready
                log.warning(
                    {
                        "status": "WARNING",
                        "message": f"Expected Value Error for Data Flow Test: {e}",
                    }
                )
                pass

            # Copy File to Instrument Bucket
            new_file_key = file_key.replace("unprocessed/", "processed/")
            self._move_object_directory(
                source_bucket=self.instrument_bucket_name,
                file_key=file_key,
                new_file_key=new_file_key,
            )

            if self._does_object_exists(
                bucket=self.instrument_bucket_name, file_key=new_file_key
            ):
                # Remove File from Unprocessed Bucket
                self._remove_object_from_bucket(
                    bucket=self.instrument_bucket_name, file_key=file_key
                )
                log.info(
                    {
                        "status": "INFO",
                        "message": f"File {file_key} successfully processed",
                    }
                )
        else:
            raise ValueError("File does not exist in bucket")

    def _does_object_exists(self, bucket, file_key):
        """
        Returns wether or not the file exists in the specified bucket
        """
        s3 = boto3.resource("s3")

        try:
            s3.Object(bucket, file_key).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                log.info(f"File {file_key} does not exist in Bucket {bucket}")
                # The object does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise
        else:
            log.info(f"File {file_key} already exists in Bucket {bucket}")
            return True

    def _move_object_directory(
        self, source_bucket=None, file_key=None, new_file_key=None
    ):
        """
        Function to copy file from S3 incoming bucket using bucket key
        to destination bucket
        """
        log.info(f"Moving File From {file_key} to {new_file_key}")

        try:
            # Initialize S3 Client and Copy Source Dict
            s3 = boto3.resource("s3")

            # Move S3 file from one folder to another
            if not self.dry_run:
                print(file_key)
                s3.Object(source_bucket, new_file_key).copy_from(CopySource=file_key)

            log.info(f"File {file_key} Successfully Moved to {new_file_key}")

        except botocore.exceptions.ClientError as e:
            log.error({"status": "ERROR", "message": e})

            raise e

    def _remove_object_from_bucket(self, bucket, file_key):
        """
        Function to copy file from S3 incoming bucket using bucket key
        to destination bucket
        """
        log.info(f"Removing From {file_key} from {bucket}")

        try:
            # Initialize S3 Client and Copy Source Dict
            s3 = boto3.resource("s3")

            # Copy S3 file from incoming bucket to destination bucket
            if not self.dry_run:
                s3.Object(bucket, file_key).delete()

            log.info((f"File {file_key} Successfully Removed from {bucket}"))

        except botocore.exceptions.ClientError as e:
            log.error({"status": "ERROR", "message": e})

            raise e
