"""
This Module contains the FileProcessor class that will distinguish
the appropriate HERMES intrument library to use when processing
the file based off which bucket the file is located in.
"""

import os
import json
from pathlib import Path
import shutil


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

        # Calibrate/Process file with Instrument Package
        calibrated_filename = self._calibrate_file(this_instr, file_path, self.dry_run)

        # Push file to S3 Bucket
        push_science_file(
            science_filename_parser,
            destination_bucket,
            calibrated_filename,
            self.dry_run,
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
                    if test_data_file.suffix in [".bin", ".cdf"]:
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

            # Get name of new file
            new_file_path = calibration.process_file(file_path)[0]
            calibrated_filename = new_file_path.name

            return calibrated_filename

        except ValueError as e:
            log.error(e)
