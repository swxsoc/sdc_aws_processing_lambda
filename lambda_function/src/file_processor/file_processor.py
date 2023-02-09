"""
This Module contains the FileProcessor class that will distinguish
the appropriate HERMES intrument library to use when processing
the file based off which bucket the file is located in.
"""
import boto3
import botocore
from datetime import date, datetime
import time
import logging
import yaml
import os
import os.path
from pathlib import Path
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

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
        TSD_REGION = config["TSD_REGION"]

except FileNotFoundError:
    print("config.yaml not found")
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
session = boto3.Session(region_name=TSD_REGION)

# To remove boto3 noisy debug logging
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)


class FileProcessor:
    """
    The FileProcessor class will then determine which instrument
    library to use to process the file.

    :param s3_bucket: The name of the S3 bucket the file is located in
    :type s3_bucket: str
    :param file_key: The name of the S3 object that is being processed
    :type file_key: str
    :param environment: The environment the FileProcessor is running in
    :type environment: str
    :param dry_run: Whether or not the FileProcessor is performing a dry run
    :type dry_run: bool
    """

    def __init__(
        self, s3_bucket: str, file_key: str, environment: str, dry_run: str = None
    ) -> None:
        # Initialize Class Variables
        try:
            self.instrument_bucket_name = s3_bucket
            log.info(
                "Instrument Bucket Name Parsed Successfully:"
                f"{self.instrument_bucket_name}"
            )

        except KeyError:
            error_message = "KeyError when extracting S3 Bucket Name"
            log.error({"status": "ERROR", "message": error_message})
            raise KeyError(error_message)

        try:
            self.file_key = file_key

            log.info(
                {
                    "status": "INFO",
                    "message": "Incoming Object Name"
                    f"Parsed Successfully: {self.file_key}",
                }
            )

        except KeyError:
            error_message = "KeyError when extracting S3 File Key"
            log.error({"status": "ERROR", "message": error_message})
            raise KeyError(error_message)

        # Variable that determines environment
        self.environment = environment

        # Variable that determines if FileProcessor performs a Dry Run
        self.dry_run = dry_run
        if self.dry_run:
            log.warning("Performing Dry Run - Files will not be copied/removed")

        try:
            # Initialize the slack client
            self.slack_client = WebClient(token=os.getenv("SLACK_TOKEN"))

            # Initialize the slack channel
            self.slack_channel = os.getenv("SLACK_CHANNEL")

        except SlackApiError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                log.error(
                    {
                        "status": "ERROR",
                        "message": "Slack Token is invalid",
                    }
                )

        except Exception as e:
            log.error(
                {
                    "status": "ERROR",
                    "message": f"Error when initializing slack client: {e}",
                }
            )

        # Process File
        self._process_file()

    def _process_file(self) -> None:
        """
        This method serves as the main entry point for the FileProcessor class.
        It will then determine which instrument library to use to process the file.

        :return: None
        :rtype: None
        """
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

                # Download file from instrument bucket if not a dry run
                if not self.dry_run:
                    file_path = self._download_file(
                        self.instrument_bucket_name,
                        self.file_key,
                        parsed_file_key,
                    )

                # Parse the science file name
                science_file = util.parse_science_filename(parsed_file_key)
                this_instr = science_file["instrument"]
                destination_bucket = INSTR_TO_BUCKET_NAME[this_instr]
                log.info(
                    f"Destination Bucket Parsed Successfully: {destination_bucket}"
                )

                # Dynamically import instrument package
                instr_pkg = __import__(
                    f"{INSTR_TO_PKG[this_instr]}.calibration",
                    fromlist=["calibration"],
                )
                calibration = getattr(instr_pkg, "calibration")

                log.info(f"Using {INSTR_TO_PKG[this_instr]} module for calibration")

                # Process file
                try:
                    # Get name of new file
                    new_file_path = calibration.process_file(file_path)[0].name

                    # Get new file key
                    new_file_key = self._generate_file_key(new_file_path)

                    # Upload file to destination bucket if not a dry run
                    if not self.dry_run:
                        # Upload file to destination bucket
                        self._upload_file(
                            new_file_path, destination_bucket, new_file_key
                        )

                        if self.slack_client:
                            # Send Slack Notification
                            self._send_slack_notification(
                                slack_client=self.slack_client,
                                slack_channel=self.slack_channel,
                                slack_message=(
                                    f"File ({new_file_key}) "
                                    "has been successfully processed and "
                                    f"uploaded to {destination_bucket}.",
                                ),
                            )

                        # Log to timeseries database
                        self._log_to_timestream(
                            action_type="PUT",
                            file_key=self.file_key,
                            new_file_key=new_file_key,
                            source_bucket=destination_bucket,
                            destination_bucket=destination_bucket,
                        )

                except ValueError as e:
                    log.error(e)

            except Exception as e:
                log.error(f"Error Processing File: {e}")
                if self.slack_client:
                    # Send Slack Notification
                    self._send_slack_notification(
                        slack_client=self.slack_client,
                        slack_channel=self.slack_channel,
                        slack_message=(
                            f"Error Processing File ({new_file_key})"
                            f"from {destination_bucket}.",
                        ),
                        alert_type="error",
                    )
                raise e

    @staticmethod
    def _does_object_exists(bucket: str, file_key: str) -> bool:
        """
        Returns wether or not the file exists in the specified bucket

        :param bucket: The name of the bucket
        :type bucket: str
        :param file_key: The name of the file
        :type file_key: str
        :return: True if the file exists, False if it does not
        :rtype: bool
        """
        s3 = session.resource("s3")

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

    @staticmethod
    def _send_slack_notification(
        slack_client,
        slack_channel: str,
        slack_message: str,
        alert_type: str = "success",
    ) -> None:
        """
        Function to send a Slack Notification
        """
        log.info(f"Sending Slack Notification to {slack_channel}")
        try:
            color = {
                "success": "#2ecc71",
                "error": "#ff0000",
            }
            ct = datetime.datetime.now()
            ts = ct.strftime("%y-%m-%d %H:%M:%S")
            slack_client.chat_postMessage(
                channel=slack_channel,
                text=f"{ts} - {slack_message}",
                attachments=[
                    {
                        "color": color[alert_type],
                        "blocks": [
                            {
                                "type": "section",
                                "text": {
                                    "type": "plain_text",
                                    "text": f"{ts} - {slack_message}",
                                },
                            }
                        ],
                    }
                ],
            )

        except SlackApiError as e:
            log.error(
                {"status": "ERROR", "message": f"Error sending Slack Notification: {e}"}
            )

    @staticmethod
    def _generate_file_key(file_key) -> str:
        """
        Function to generate full s3 file key in the format:
        {level}/{year}/{month}/{file_key}

        :param file_key: The name of the file
        :type file_key: str
        :return: The full s3 file key
        :rtype: str
        """
        try:
            current_year = date.today().year
            current_month = date.today().month
            if current_month < 10:
                current_month = f"0{current_month}"

            science_file = util.parse_science_filename(file_key)

            new_file_key = (
                f"{science_file['level']}/{current_year}/{current_month}/{file_key}"
            )

            return new_file_key

        except IndexError as e:
            log.error({"status": "ERROR", "message": e})
            raise e

    @staticmethod
    def _download_file(source_bucket: str, file_key: str, parsed_file_key: str) -> Path:
        """
        Function to download file from S3

        :param source_bucket: The name of the source bucket
        :type source_bucket: str
        :param file_key: The name of the file
        :type file_key: str
        :param parsed_file_key: The name of the file
        :type parsed_file_key: str
        :return: The path to the downloaded file
        :rtype: Path
        """
        try:
            # Initialize S3 Client
            log.info(f"Downloading file {file_key} from {source_bucket}")
            s3 = session.client("s3")

            # Create tmp directory in root of lambda
            if not os.path.exists("/tmp"):
                os.mkdir("/tmp")

            # Download file to tmp directory
            s3.download_file(source_bucket, file_key, f"/tmp/{parsed_file_key}")

            log.info(f"File {file_key} Successfully Downloaded")

            return Path(f"/tmp/{parsed_file_key}")

        except botocore.exceptions.ClientError as e:
            log.error({"status": "ERROR", "message": e})

            raise e

    @staticmethod
    def _upload_file(filename: str, destination_bucket: str, file_key: str) -> None:
        """
        Function to upload file to S3

        :param filename: The name of the file
        :type filename: str
        :param destination_bucket: The name of the destination bucket
        :type destination_bucket: str
        :param file_key: The name of the file
        :type file_key: str
        :return: None
        :rtype: None
        """
        try:
            # Initialize S3 Client
            log.info(f"Uploading file {file_key} to {destination_bucket}")
            s3 = session.client("s3")

            # Upload file to destination bucket
            s3.upload_file(f"/tmp/{filename}", destination_bucket, file_key)

            log.info(f"File {file_key} Successfully Uploaded")

        except botocore.exceptions.ClientError as e:
            log.error({"status": "ERROR", "message": e})

            raise e

    @staticmethod
    def _log_to_timestream(
        action_type: str,
        file_key: str,
        new_file_key: str = None,
        source_bucket: str = None,
        destination_bucket: str = None,
    ) -> None:
        """
        Function to log to Timestream

        :param action_type: The type of action performed
        :type action_type: str
        :param file_key: The name of the file
        :type file_key: str
        :param new_file_key: The new name of the file
        :type new_file_key: str
        :param source_bucket: The name of the source bucket
        :type source_bucket: str
        :param destination_bucket: The name of the destination bucket
        :type destination_bucket: str
        :return: None
        :rtype: None
        """
        log.info("Logging to Timestream")
        CURRENT_TIME = str(int(time.time() * 1000))
        try:
            # Initialize Timestream Client
            timestream = session.client("timestream-write")

            if not source_bucket and not destination_bucket:
                raise ValueError("A Source or Destination Buckets is required")

            # Write to Timestream
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
