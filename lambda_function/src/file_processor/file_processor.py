"""
This Module contains the FileProcessor class that will distinguish
the appropriate HERMES intrument library to use when processing
the file based off which bucket the file is located in.
"""
import boto3
import botocore
import os
import os.path
import json
from pathlib import Path
from slack_sdk.errors import SlackApiError

from sdc_aws_utils.logging import log, configure_logger
from sdc_aws_utils.config import (
    TSD_REGION,
    INSTR_TO_PKG,
    parser as science_filename_parser,
    get_instrument_bucket,
)
from sdc_aws_utils.aws import (
    create_s3_client_session,
    create_timestream_client_session,
    object_exists,
    download_file_from_s3,
    upload_file_to_s3,
    log_to_timestream,
    create_s3_file_key,
)
from sdc_aws_utils.slack import get_slack_client, send_pipeline_notification
from cdftracker.database import create_engine
from cdftracker.database.tables import create_tables
from cdftracker.tracker import tracker

# Configure logger
configure_logger()


def handle_event(event, context) -> dict:
    """
    Handles the event passed to the lambda function to initialize the FileProcessor

    :param event: Event data passed from the lambda trigger
    :type event: dict
    :param context: Lambda context
    :type context: dict
    :return: Returns a 200 (Successful) / 500 (Error) HTTP response
    :rtype: dict
    """
    try:
        environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")

        # Check if SNS or S3 event
        records = json.loads(event["Records"][0]["Sns"]["Message"])["Records"]

        # Parse message from SNS Notification
        for s3_event in records:
            # Extract needed information from event
            s3_bucket = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]

            FileProcessor(
                s3_bucket=s3_bucket, file_key=file_key, environment=environment
            )

            return {"statusCode": 200, "body": "File Processed Successfully"}

    except Exception as e:
        log.error({"status": "ERROR", "message": e})

        return {
            "statusCode": 500,
            "body": json.dumps(f"Error Processing File: {e}"),
        }


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

        # Initialize S3 Client
        self.s3_client = create_s3_client_session()

        try:
            # Initialize Timestream Client
            self.timestream_client = create_timestream_client_session(TSD_REGION)

        except botocore.exceptions.ClientError:
            self.timestream_client = None
            log.error(
                {
                    "status": "ERROR",
                    "message": "Timestream Client could not be initialized",
                }
            )

        try:
            # Initialize the slack client
            self.slack_client = get_slack_client(
                slack_token=os.getenv("SDC_AWS_SLACK_TOKEN")
            )

            # Initialize the slack channel
            self.slack_channel = os.getenv("SDC_AWS_SLACK_CHANNEL")

        except SlackApiError as e:
            error_code = int(e.response["Error"]["Code"])
            self.slack_client = None
            if error_code == 404:
                log.error(
                    {
                        "status": "ERROR",
                        "message": "Slack Token is invalid",
                    }
                )

        except Exception as e:
            self.slack_client = None
            log.error(
                {
                    "status": "ERROR",
                    "message": f"Error when initializing slack client: {e}",
                }
            )

        secret_arn = os.getenv("RDS_SECRET_ARN", None)
        if secret_arn:
            try:
                # Get Database Credentials
                session = boto3.session.Session()
                client = session.client(service_name="secretsmanager")
                response = client.get_secret_value(SecretId=secret_arn)
                secret = json.loads(response["SecretString"])
                connection_string = (
                    f"postgresql://{secret['username']}:{secret['password']}@"
                    f"{secret['host']}:{secret['port']}/{secret['dbname']}"
                )
                # Initialize the database engine
                self.database_engine = create_engine(connection_string)
                # Setup the database tables if they do not exist
                create_tables(self.database_engine)
                # Set tracker to CDFTracker
                self.tracker = tracker.CDFTracker(
                    self.database_engine, science_filename_parser
                )

            except Exception as e:
                self.tracker = None
                log.error(
                    {
                        "status": "ERROR",
                        "message": f"Error when initializing database engine: {e}",
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
            object_exists(
                s3_client=self.s3_client,
                bucket=self.instrument_bucket_name,
                file_key=self.file_key,
            )
            or self.dry_run
        ):
            try:
                # Parse file key to get instrument name
                file_key_array = self.file_key.split("/")
                parsed_file_key = file_key_array[-1]

                # Download file from instrument bucket if not a dry run
                if not self.dry_run:
                    file_path = download_file_from_s3(
                        self.s3_client,
                        self.instrument_bucket_name,
                        self.file_key,
                        parsed_file_key,
                    )

                # Parse the science file name
                science_file = science_filename_parser(parsed_file_key)
                this_instr = science_file["instrument"]
                destination_bucket = get_instrument_bucket(this_instr, self.environment)
                if self.tracker and science_file["level"] == "l0":
                    # If level is L0 should be tracked in CDF
                    self.tracker.track(file_path)

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
                    new_file_path = calibration.process_file(file_path)[0]
                    new_file_pathname = new_file_path.name

                    # Get new file key
                    new_file_key = create_s3_file_key(
                        science_filename_parser, new_file_pathname
                    )

                    # Upload file to destination bucket if not a dry run
                    if not self.dry_run:
                        # Upload file to destination bucket
                        upload_file_to_s3(
                            s3_client=self.s3_client,
                            destination_bucket=destination_bucket,
                            filename=new_file_pathname,
                            file_key=new_file_key,
                        )

                        if self.tracker:
                            # Track file in CDF
                            self.tracker.track(Path(new_file_path))

                        if self.slack_client:
                            # Send Slack Notification
                            send_pipeline_notification(
                                slack_client=self.slack_client,
                                slack_channel=self.slack_channel,
                                path=new_file_pathname,
                                alert_type="processed",
                            )

                        if self.timestream_client:
                            # Log to timeseries database
                            log_to_timestream(
                                timestream_client=self.timestream_client,
                                action_type="PUT",
                                file_key=self.file_key,
                                new_file_key=new_file_key,
                                source_bucket=destination_bucket,
                                destination_bucket=destination_bucket,
                                environment=self.environment,
                            )

                except ValueError as e:
                    log.error(e)

            except Exception as e:
                log.error(f"Error Processing File: {e}")
                if self.slack_client:
                    # Send Slack Notification
                    send_pipeline_notification(
                        slack_client=self.slack_client,
                        slack_channel=self.slack_channel,
                        path=parsed_file_key,
                        alert_type="processed_error",
                    )
                raise e
