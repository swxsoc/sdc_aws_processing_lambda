"""
This module contains the FileProcessor class, which determines the appropriate
HERMES instrument library to use for processing a file, based on the S3 bucket
in which the file is located.
"""

from enum import Enum
import time
import os
import json
from pathlib import Path
from itertools import combinations
import shutil
import traceback

import swxsoc


from sdc_aws_utils.logging import log, configure_logger
from sdc_aws_utils.config import (
    INSTR_TO_PKG,
    parser as science_filename_parser,
    get_instrument_bucket,
)
from sdc_aws_utils.aws import (
    parse_file_key,
    get_science_file,
    push_science_file,
)

import metatracker
import boto3

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
        log.error(
            {"status": "ERROR", "message": e, "traceback": traceback.format_exc()}
        )

        return {
            "statusCode": 500,
            "body": json.dumps(f"Error Processing File: {e}"),
        }


class Status(Enum):
    SUCCESS = "success"
    PENDING = "pending"
    FAILED = "failed"


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
        self.instrument_bucket_name = s3_bucket

        self.file_key = file_key

        # Variable that determines environment
        self.environment = environment

        # Variable that determines if FileProcessor performs a Dry Run
        self.dry_run = dry_run

        # Process File
        self._process_file()

    def _process_file(self) -> None:
        """
        This method serves as the main entry point for the FileProcessor class.
        It will then determine which instrument library to use to process the file.

        :return: None
        :rtype: None
        """
        log.debug(
            {
                "status": "DEBUG",
                "message": "Processing File",
                "instrument_bucket_name": self.instrument_bucket_name,
                "file_key": self.file_key,
                "environment": self.environment,
                "dry_run": self.dry_run,
            }
        )

        # Parse file key to needed information
        parsed_file_key = parse_file_key(self.file_key)

        # Parse the science file name
        science_file = science_filename_parser(parsed_file_key)
        this_instr = science_file["instrument"]
        destination_bucket = get_instrument_bucket(this_instr, self.environment)

        # Download file from S3 or get local file path
        file_path = get_science_file(
            self.instrument_bucket_name,
            self.file_key,
            parsed_file_key,
            self.dry_run,
        )

        status = self.build_status(
            success=True,
            message="File Processed Successfully",
        )

        # Calibrate/Process file with Instrument Package
        start_time = time.time()
        calibrated_filenames = self._calibrate_file(this_instr, file_path, self.dry_run)
        end_time = time.time()
        total_time = end_time - start_time

        if not calibrated_filenames:

            status = self.build_status(
                status=Status.FAILED,
                message=f"Could Not Process {file_path} Further",
            )

            FileProcessor._track_file_metatracker(
                science_filename_parser,
                Path(file_path),
                self.file_key,
                self.instrument_bucket_name,
                status=status,
            )

            log.warning(
                {
                    "status": "WARNING",
                    "message": f"No calibrated files found for file: {file_path}",
                }
            )
            return
        else:
            status = self.build_status(
                status=Status.SUCCESS,
                message=f"File Processed Successfully",
                total_time=total_time,
                origin_file_id=None,
            )

            science_file_id, science_product_id = FileProcessor._track_file_metatracker(
                science_filename_parser,
                Path(file_path),
                self.file_key,
                self.instrument_bucket_name,
                status=status,
            )

            # Push file to S3 Bucket
            for calibrated_filename in calibrated_filenames:
                status = self.build_status(
                    status=Status.PENDING,
                    message=f"File {calibrated_filename} Needs Further Processing",
                    origin_file_id=science_file_id,
                )

                push_science_file(
                    science_filename_parser,
                    destination_bucket,
                    calibrated_filename,
                    self.dry_run,
                )
                self._track_file_metatracker(
                    science_filename_parser,
                    Path(calibrated_filename),
                    calibrated_filename,
                    destination_bucket,
                    science_product_id,
                    status=status,
                )

    @staticmethod
    def _calibrate_file(instrument, file_path, dry_run=False):
        """
        Calibrates the file using the appropriate instrument library.
        This involves dynamic import of the calibration module and
        processing of the file.

        :param instrument: The name of the instrument used for calibration.
        :type instrument: str
        :param file_path: The path to the file that needs to be calibrated.
        :type file_path: Path
        :param dry_run: Indicates whether the operation is a dry run.
        :type dry_run: bool
        :return: The filename of the calibrated file.
        :rtype: string
        """
        try:
            # Dynamically import instrument package
            instr_pkg = __import__(
                f"{INSTR_TO_PKG[instrument]}.calibration",
                fromlist=["calibration"],
            )
            calibration = getattr(instr_pkg, "calibration")

            # If USE_INSTRUMENT_TEST_DATA is set to True, use test data in package
            if os.getenv("USE_INSTRUMENT_TEST_DATA") == "True":
                log.info("Using test data from instrument package")
                instr_pkg_data = __import__(
                    f"{INSTR_TO_PKG[instrument]}.data",
                    fromlist=["data"],
                )
                # Get all files in test data directory
                test_data_dir = Path(instr_pkg_data.__path__[0])
                test_data_files = list(test_data_dir.glob("**/*"))
                log.info(f"Found {len(test_data_files)} files in test data directory")
                log.info(f"Using {test_data_files} as test data")
                # Get any files ending in .bin or .cdf and calibrate them
                for test_data_file in test_data_files:
                    if test_data_file.suffix in [".bin", ".cdf", ".fits"]:
                        log.info(f"Calibrating {test_data_file}")
                        # Make /test_data directory if it doesn't exist
                        Path("/test_data").mkdir(parents=True, exist_ok=True)
                        # Copy file to /test_data directory using shutil
                        test_data_file_path = Path(test_data_file)
                        file_path = Path(f"/test_data/{test_data_file_path.name}")
                        shutil.copy(test_data_file_path, file_path)
                        # Calibrate file
                        calibrated_filename = calibration.process_file(file_path)[0]
                        # Copy calibrated file to test data directory
                        calibrated_file_path = Path(calibrated_filename)
                        # Return name of calibrated file
                        log.info(f"Calibrated file saved as {calibrated_file_path}")

                        return calibrated_filename

                # If no files ending in .bin or .cdf are found, raise an error
                raise FileNotFoundError(
                    "No files ending in .bin or .cdf found in test data directory"
                )
            log.info(f"Calibrating {file_path}")
            # Get name of new file
            files_list = calibration.process_file(Path(file_path))

            path_list = []
            for generated_file in files_list:
                new_file_path = Path(generated_file)
                calibrated_filename = new_file_path.name
                path_list.append(calibrated_filename)
                log.info(f"Calibrated file saved as {calibrated_filename}")

            return path_list

        except ValueError as e:
            log.error(
                {
                    "status": "ERROR",
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                }
            )

        except FileNotFoundError as e:
            log.error(
                {
                    "status": "ERROR",
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                }
            )

        except Exception as e:
            log.error(
                {
                    "status": "ERROR",
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                }
            )
            raise e

    @staticmethod
    def _track_file_metatracker(
        science_filename_parser, file_path, s3_key, s3_bucket, science_product_id=None
    ) -> int:
        """
        Tracks processed science product in the CDF Tracker file database.
        It involves initializing the database engine, setting up database tables,
        and tracking both the original and processed files.

        :param science_filename_parser: The parser function to process file names.
        :type science_filename_parser: function
        :param file_path: The path of the original file.
        :type file_path: Path
        """
        secret_arn = os.getenv("RDS_SECRET_ARN", None)
        if secret_arn:
            try:
                # Validate file path
                if not file_path or not isinstance(file_path, Path):
                    raise ValueError("Invalid file path provided.")
                # Check if file exists
                if not file_path.exists():
                    raise FileNotFoundError(f"File not found: {file_path}")

                # Get Database Credentials
                session = boto3.session.Session()
                client = session.client(service_name="secretsmanager")
                response = client.get_secret_value(SecretId=secret_arn)
                secret = json.loads(response["SecretString"])
                connection_string = (
                    f"postgresql://{secret['username']}:{secret['password']}@"
                    f"{secret['host']}:{secret['port']}/{secret['dbname']}"
                )

                metatracker_config = FileProcessor.get_metatracker_config(swxsoc.config)

                log.debug(swxsoc.config)

                log.debug(metatracker_config)

                metatracker.set_config(metatracker_config)

                from metatracker.database import create_engine
                from metatracker.database.tables import create_tables
                from metatracker.tracker import tracker

                # Initialize the database engine
                database_engine = create_engine(connection_string)

                # Create tables if they do not exist
                create_tables(database_engine)

                # Set tracker to MetaTracker
                meta_tracker = tracker.MetaTracker(
                    database_engine, science_filename_parser
                )

                if meta_tracker:
                    science_file_id, science_product_id = meta_tracker.track(
                        file_path, s3_key, s3_bucket
                    )

                    return science_file_id, science_product_id

                return None

            except Exception as e:
                log.error(
                    {
                        "status": "ERROR",
                        "message": str(e),
                        "traceback": traceback.format_exc(),
                    }
                )
                return None

    @staticmethod
    def get_metatracker_config(swxsoc_config: dict) -> dict:
        """
        Creates the MetaTracker configuration from the swxsoc configuration.

        :param config: The swxsoc configuration.
        :type config: dict
        :return: The MetaTracker configuration.
        :rtype: dict
        """
        mission_data = swxsoc_config["mission"]
        instruments = mission_data["inst_names"]

        instruments_list = [
            {
                "instrument_id": idx + 1,
                "description": (
                    f"{mission_data['inst_fullnames'][idx]} "
                    f"({mission_data['inst_targetnames'][idx]})"
                ),
                "full_name": mission_data["inst_fullnames"][idx],
                "short_name": mission_data["inst_shortnames"][idx],
            }
            for idx in range(len(instruments))
        ]

        # Generate all possible configurations of the instruments
        instrument_configurations = []
        config_id = 1
        for r in range(1, len(instruments) + 1):
            for combo in combinations(range(1, len(instruments) + 1), r):
                config = {"instrument_configuration_id": config_id}
                config.update(
                    {
                        f"instrument_{i+1}_id": combo[i] if i < len(combo) else None
                        for i in range(len(instruments))
                    }
                )
                instrument_configurations.append(config)
                config_id += 1

        metatracker_config = {
            "mission_name": mission_data["mission_name"],
            "instruments": instruments_list,
            "instrument_configurations": instrument_configurations,
        }

        return metatracker_config

    @staticmethod
    def build_status(
        status: Status,
        message: str,
        total_time: float = None,
        origin_file_id: int = None,
    ) -> dict:
        """
        Builds a status dictionary for MetaTracker tracking.

        :param start_time: Timestamp when processing began (from `time.time()`).
        :type start_time: float
        :param success: Whether processing succeeded.
        :type success: bool
        :param message: Message to include with the status.
        :type message: str
        :param origin_file_id: Optional ID of the original file if this is a processed result.
        :type origin_file_id: int
        :return: Dictionary representing processing status.
        :rtype: dict
        """

        status = {
            "processing_status": status.value,
            "processing_status_message": message,
        }

        if origin_file_id:
            status["origin_file_id"] = origin_file_id

        if total_time:
            status["processing_time_length"] = total_time

        return status
