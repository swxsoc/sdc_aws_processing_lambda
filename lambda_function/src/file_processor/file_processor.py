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
from datetime import date, datetime
import time
import logging
import yaml

# Initialize constants to be parsed from config.yaml
MISSION_NAME = ""
INSTR_NAMES = []
MISSION_PKG = ""

# Read YAML file and parse variables
try:
    with open("./config.yaml", "r") as f:
        config = yaml.safe_load(f)
        print("config.yaml loaded successfully")
        MISSION_NAME = config["MISSION_NAME"]
        INSTR_NAMES = config["INSTR_NAMES"]
        MISSION_PKG = config["MISSION_PKG"]

except FileNotFoundError:
    print("config.yaml not found. Check to make sure it exists in the root directory.")
    exit(1)


# Initialize other constants after loading YAML file
INSTR_PKG = [f"{MISSION_NAME}_{this_instr}" for this_instr in INSTR_NAMES]
INSTR_TO_BUCKET_NAME = {
    this_instr: f"{MISSION_NAME}-{this_instr}" for this_instr in INSTR_NAMES
}
INSTR_TO_PKG = dict(zip(INSTR_NAMES, INSTR_PKG))


# Import logging from mission package
mission_pkg = __import__(MISSION_PKG)
log = getattr(mission_pkg, "log")

# Import logging and util from mission package
mission_pkg = __import__(MISSION_PKG)
log = getattr(mission_pkg, "log")
util = getattr(mission_pkg, "util").util

# Starts boto3 session so it gets access to needed credentials
session = boto3.Session()

# To remove boto3 noisy debug logging
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)


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
                file_key_array = self.file_key.split("/")
                parsed_file_key = file_key_array[-1]
                science_file = util.parse_science_filename(parsed_file_key)
                this_instr = science_file["instrument"]
                destination_bucket = INSTR_TO_BUCKET_NAME[this_instr]
                log.info(
                    f"Destination Bucket Parsed Successfully: {destination_bucket}"
                )

                # Dynamically import instrument package
                instr_pkg = __import__(
                    f"{INSTR_TO_PKG[this_instr]}.calibration", fromlist=["calibration"]
                )
                calibration = getattr(instr_pkg, "calibration")

                log.info(f"Using {INSTR_TO_PKG[this_instr]} module for calibration")

                # Run Calibration on File (This will cause a ValueError
                # if no calibration is found)
                calibration.calibrate_file(parsed_file_key)

            except ValueError as e:
                # Expected ValueError for Data Flow Test because no calibration
                # files are ready
                log.warning(
                    {
                        "status": "WARNING",
                        "message": f"Expected Value Error for Data Flow Test: {e}",
                    }
                )

                # Copy File to Instrument Bucket
                new_file_key = self._get_new_file_key(file_key)

                self._process_object(
                    source_bucket=self.instrument_bucket_name,
                    file_key=file_key,
                    new_file_key=new_file_key,
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

    def _process_object(self, source_bucket, file_key, new_file_key):
        """
        Function to copy file from S3 incoming bucket using bucket key
        to destination bucket
        """
        log.info(f"Moving File From {file_key} to {new_file_key}")

        try:
            # Initialize S3 Client and Copy Source Dict

            # Move S3 file from one folder to another
            if not self.dry_run:
                s3 = boto3.client("s3")
                copy_source = {"Bucket": source_bucket, "Key": file_key}
                s3.copy(copy_source, source_bucket, new_file_key)
                # Log added file to Incoming Bucket in Timestream
                self._log_to_timestream(
                    action_type="PUT",
                    file_key=file_key,
                    new_file_key=new_file_key,
                    source_bucket=source_bucket,
                    destination_bucket=source_bucket,
                )
                log.info(
                    {
                        "status": "INFO",
                        "message": f"File {file_key} successfully "
                        f"processed to {new_file_key}",
                    }
                )
            log.info(f"File {file_key} Successfully Moved to {new_file_key}")

        except botocore.exceptions.ClientError as e:
            log.error({"status": "ERROR", "message": e})

            raise e

    def _get_datalevel(self, file_key):
        """
        Function to extract data level from file key
        """
        try:
            file_key_array = self.file_key.split("/")
            parsed_level = file_key_array[0]
            return parsed_level
        except IndexError as e:
            log.error({"status": "ERROR", "message": e})
            raise e

    def _get_next_datalevel(self, file_key):
        """
        Function to extract next data level from file key
        """
        try:
            current_level = util.VALID_DATA_LEVELS.index(self._get_datalevel(file_key))
            return util.VALID_DATA_LEVELS[current_level + 1]
        except IndexError as e:
            log.error({"status": "ERROR", "message": e})
            raise e

    def _get_new_file_key(self, file_key):
        """
        Function to create new file key for next data level
        """
        try:
            current_year = date.today().year
            current_month = date.today().month
            if current_month < 10:
                current_month = f"0{current_month}"
            file_key_array = self.file_key.split("/")
            parsed_file_key = file_key_array[-1]
            current_data_level = self._get_datalevel(file_key)
            next_data_level = self._get_next_datalevel(file_key)
            science_file = util.parse_science_filename(parsed_file_key)
            science_file["level"] = next_data_level
            processed_name = util.create_science_filename(
                time=science_file["time"],
                instrument=science_file["instrument"],
                level=science_file["level"],
                version="0.0.1",
            )
            new_file_key = (
                f"{next_data_level}/{current_year}/{current_month}/{processed_name}"
            )
            new_file_key = new_file_key.replace(current_data_level, next_data_level)
            return new_file_key
        except IndexError as e:
            log.error({"status": "ERROR", "message": e})
            raise e

    def _log_to_timestream(
        self,
        action_type,
        file_key,
        new_file_key=None,
        source_bucket=None,
        destination_bucket=None,
    ):
        """
        Function to log to Timestream
        """
        log.info("Logging to Timestream")
        CURRENT_TIME = str(int(time.time() * 1000))
        try:
            # Initialize Timestream Client
            timestream = boto3.client("timestream-write")

            if not source_bucket and not destination_bucket:
                raise ValueError("A Source or Destination Buckets is required")

            # connect to s3 - assuming your creds are all
            # set up and you have boto3 installed
            s3 = boto3.resource("s3")

            # get the bucket

            bucket = s3.Bucket(destination_bucket)
            if action_type == "DELETE":
                bucket = s3.Bucket(source_bucket)

            # use loop and count increment
            count_obj = 0
            for i in bucket.objects.all():
                count_obj = count_obj + 1

            # Write to Timestream
            if not self.dry_run:
                timestream.write_records(
                    DatabaseName="sdc_aws_logs",
                    TableName="sdc_aws_s3_bucket_log_table",
                    Records=[
                        {
                            "Time": CURRENT_TIME,
                            "Dimensions": [
                                {"Name": "action_type", "Value": action_type},
                                {
                                    "Name": "source_bucket",
                                    "Value": source_bucket or "N/A",
                                },
                                {
                                    "Name": "destination_bucket",
                                    "Value": destination_bucket or "N/A",
                                },
                                {"Name": "file_key", "Value": file_key},
                                {
                                    "Name": "new_file_key",
                                    "Value": new_file_key or "N/A",
                                },
                                {
                                    "Name": "current file count",
                                    "Value": str(count_obj) or "N/A",
                                },
                            ],
                            "MeasureName": "timestamp",
                            "MeasureValue": str(datetime.utcnow().timestamp()),
                            "MeasureValueType": "DOUBLE",
                        },
                    ],
                )

            log.info((f"File {file_key} Successfully Logged to Timestream"))

        except botocore.exceptions.ClientError as e:
            log.error({"status": "ERROR", "message": e})

            raise e
