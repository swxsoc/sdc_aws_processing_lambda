"""
FileProcessor class, which determines the appropriate instrument library to use for processing a file.
"""

import json
import os
import shutil
import time
import traceback
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import boto3
import psycopg2
import swxsoc
from metatracker.database import create_engine
from metatracker.database.tables import create_tables
from metatracker.tracker import tracker
from sdc_aws_utils.aws import (get_science_file, parse_file_key,
                               push_science_file)
from sdc_aws_utils.config import get_instrument_bucket, get_instrument_package
from sdc_aws_utils.config import parser as science_filename_parser
from sdc_aws_utils.logging import configure_logger, log
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_random)

# Configure logger
configure_logger()


def handle_event(event: dict[str, Any], context: Any) -> dict[str, int | str]:
    """
    Process a Lambda event and dispatch file processing work.

    Parameters
    ----------
    event : dict[str, Any]
        Triggering AWS Lambda event. Supports S3 ``Records`` events and empty
        events that trigger a full incoming-bucket scan and sorting of all files.
    context : Any
        AWS Lambda context object (accepted for compatibility).

    Returns
    -------
    dict[str, int | str]
        Response dictionary containing ``statusCode`` and serialized ``body``.
    """
    try:
        environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")

        # Extract Records from SNS or S3 notification
        sns_message = event.get("Records", [{}])[0].get("Sns", {}).get("Message", "{}")
        records = json.loads(sns_message).get("Records", [])

        if not records:
            log.info("No records found in SNS event. Reprocessing data from database.")
            fetch_data()
            return {"statusCode": 200, "body": "Reprocessed data from database."}

        # Process each S3 event record
        for s3_event in records:
            s3_bucket = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]

            FileProcessor(
                s3_bucket=s3_bucket, file_key=file_key, environment=environment
            )

        return {"statusCode": 200, "body": "Files processed successfully."}

    except Exception as e:
        log.error("Error Processing Event", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


class Status(Enum):
    SUCCESS = "success"
    PENDING = "pending"
    FAILED = "failed"


class FileProcessor:
    """
    Determine the instrument package to use and process an input science file.
    """

    def __init__(
        self,
        s3_bucket: str,
        file_key: str,
        environment: str,
        dry_run: bool | None = None,
    ) -> None:
        """
        Initialize a processor instance and immediately process the file.

        Parameters
        ----------
        s3_bucket : str
            Name of the S3 bucket containing the file to process.
        file_key : str
            Key of the S3 object to process.
        environment : str
            Deployment environment used for instrument bucket/package lookup.
        dry_run : bool | None, optional
            When set, runs in dry-run mode where supported by dependencies.
            Default is None.
        """
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
        Process one source file through calibration, upload, and tracking steps.

        Returns
        -------
        None
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
        start_time = time.time()
        calibrated_filenames = self._calibrate_file(this_instr, file_path, self.dry_run)
        end_time = time.time()
        total_time = end_time - start_time

        if not calibrated_filenames:
            # If no calibrated files are found, set status to failed
            status = self.build_status(
                status=Status.FAILED,
                message=f"Could Not Process {file_path} Further",
            )

            FileProcessor._track_file_metatracker(
                science_filename_parser=science_filename_parser,
                file_path=Path(file_path),
                s3_key=self.file_key,
                s3_bucket=self.instrument_bucket_name,
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
            # If calibrated files are found, set status to success
            status = self.build_status(
                status=Status.SUCCESS,
                message="File Processed Successfully",
                total_time=total_time,
            )

            # Track the original science file as processed successfully
            science_file_id, science_product_id = FileProcessor._track_file_metatracker(
                science_filename_parser=science_filename_parser,
                file_path=Path(file_path),
                s3_key=self.file_key,
                s3_bucket=self.instrument_bucket_name,
                status=status,
            )

            log.info(
                {
                    "status": "INFO",
                    "message": f"File {self.file_key} processed successfully.",
                    "science_file_id": science_file_id,
                    "science_product_id": science_product_id,
                }
            )

            # Filter out None values from calibrated filenames
            calibrated_filenames = [
                fname for fname in calibrated_filenames if fname is not None
            ]
            # Push file to S3 Bucket
            for calibrated_filename in calibrated_filenames:
                status = self.build_status(
                    status=Status.PENDING,
                    message=f"File {calibrated_filename} Needs Further Processing",
                    origin_file_ids=[science_file_id],
                )

                push_science_file(
                    science_filename_parser,
                    destination_bucket,
                    calibrated_filename,
                    self.dry_run,
                )

                # Track the calibrated file in the CDF Tracker
                self._track_file_metatracker(
                    science_filename_parser=science_filename_parser,
                    file_path=Path("/tmp") / calibrated_filename,
                    s3_key=calibrated_filename,
                    s3_bucket=destination_bucket,
                    science_product_id=science_product_id,
                    status=status,
                )

    @staticmethod
    def _calibrate_file(
        instrument: str, file_path: str | Path, dry_run: bool = False
    ) -> list[str | None] | None:
        """
        Calibrate a file using the selected instrument calibration package.

        Parameters
        ----------
        instrument : str
            Instrument short name used to resolve the package import.
        file_path : str | Path
            Source file path passed to the instrument calibration function.
        dry_run : bool, optional
            Dry-run flag accepted for interface compatibility. Default is False.

        Returns
        -------
        list[str | None] | None
            Generated calibrated file names, preserving None entries when
            downstream calibration returns them. Returns None for handled
            ValueError/FileNotFoundError paths.
        """
        try:
            # Dynamically import instrument package
            instr_pkg = __import__(
                f"{get_instrument_package(instrument)}.calibration",
                fromlist=["calibration"],
            )
            calibration = getattr(instr_pkg, "calibration")

            # If USE_INSTRUMENT_TEST_DATA is set to True, use test data in package
            if os.getenv("USE_INSTRUMENT_TEST_DATA") == "True":
                log.info("Using test data from instrument package")
                instr_pkg_data = __import__(
                    f"{get_instrument_package(instrument)}.data",
                    fromlist=["data"],
                )
                # Get all files in test data directory
                test_data_dir = Path(instr_pkg_data.__path__[0]) / "test"
                log.info(f"Test data directory: {test_data_dir}")
                test_data_files = list(test_data_dir.glob("**/*"))
                log.info(f"Found {len(test_data_files)} files in test data directory")
                log.info(f"Using {test_data_files} as test data")
                # Stub path list for calibrated files
                path_list = []
                # Loop the test data files for calibration
                for test_data_file in test_data_files:
                    if test_data_file.suffix in [".bin", ".cdf", ".fits", ".csv"]:
                        log.info(f"Calibrating {test_data_file.name}")
                        # Make /test_data directory if it doesn't exist
                        Path("/test_data").mkdir(parents=True, exist_ok=True)
                        # Copy file to /test_data directory using shutil
                        test_data_file_path = Path(test_data_file)
                        file_path = Path(f"/test_data/{test_data_file_path.name}")
                        shutil.copy(test_data_file_path, file_path)
                        # Calibrate file
                        files_list = calibration.process_file(file_path)

                        if len(files_list) == 0:
                            log.warning(
                                f"No calibrated files generated for {file_path}"
                            )
                            continue
                        for generated_file in files_list:
                            if generated_file is not None:
                                new_file_path = Path(generated_file)
                                calibrated_filename = new_file_path.name
                                path_list.append(calibrated_filename)
                                log.info(
                                    f"Calibrated file saved as {calibrated_filename}"
                                )
                            else:
                                # Pass-through None values to indicate no file was created
                                path_list.append(None)
                                log.warning(f"'None' file generated for {file_path}")
                # Return list of calibrated files
                return path_list

            log.info(f"Calibrating {file_path}")
            # Get name of new file
            files_list = calibration.process_file(Path(file_path))

            path_list = []
            for generated_file in files_list:
                if generated_file is not None:
                    new_file_path = Path(generated_file)
                    calibrated_filename = new_file_path.name
                    path_list.append(calibrated_filename)
                    log.info(f"Calibrated file saved as {calibrated_filename}")
                else:
                    # Pass-through None values to indicate no file was created
                    path_list.append(None)
                    log.warning(f"'None' file generated for {file_path}")

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
    @retry(
        retry=retry_if_exception_type(psycopg2.OperationalError),
        wait=wait_random(min=2, max=10),
        stop=stop_after_attempt(10),
        reraise=True,
    )
    def _track_file_metatracker(
        science_filename_parser: Callable[[str], dict[str, Any]],
        file_path: Path,
        s3_key: str,
        s3_bucket: str,
        science_product_id: int | None = None,
        status: dict[str, Any] | None = None,
    ) -> tuple[int | None, int | None]:
        """
        Track a science file and status metadata in the MetaTracker database.

        Parameters
        ----------
        science_filename_parser : Callable[[str], dict[str, Any]]
            Parser used by MetaTracker to extract metadata from filenames.
        file_path : Path
            Local file path of the source or calibrated product.
        s3_key : str
            S3 object key for the tracked file.
        s3_bucket : str
            S3 bucket name for the tracked file.
        science_product_id : int, optional
            Existing science product identifier, if already created.
        status : dict[str, Any], optional
            Processing status payload to persist with the file record.

        Returns
        -------
        tuple[int | None, int | None]
            Tuple of (science_file_id, science_product_id). Returns
            (None, None) on missing configuration or handled errors.
        """
        secret_arn = os.getenv("RDS_SECRET_ARN", None)
        if not secret_arn:
            log.error(
                f"Failed to update MetaTracker for file {file_path}. No RDS Secret ARN found in environment variables.",
            )
            return None, None

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

            # Initialize the database engine
            database_engine = create_engine(connection_string)

            # Create tables if they do not exist
            create_tables(database_engine)

            # Set tracker to MetaTracker
            meta_tracker = tracker.MetaTracker(database_engine, science_filename_parser)

            if meta_tracker:
                science_file_id, science_product_id = meta_tracker.track(
                    file_path, s3_key, s3_bucket, status=status
                )

                return science_file_id, science_product_id

            return None, None

        except Exception as e:
            log.error(
                {
                    "status": "ERROR",
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                }
            )
            return None, None

    @staticmethod
    def build_status(
        status: Status,
        message: str,
        total_time: float | None = None,
        origin_file_ids: list[int | None] | None = None,
    ) -> dict[str, Any]:
        """
        Build a status payload for MetaTracker state updates.

        Parameters
        ----------
        status : Status
            Processing state enum value.
        message : str
            Human-readable description of the processing state.
        total_time : float | None, optional
            Processing duration in seconds.
        origin_file_ids : list[int | None] | None, optional
            Source file IDs for downstream derived products.

        Returns
        -------
        dict[str, Any]
            Status payload persisted by MetaTracker.
        """

        status = {
            "processing_status": status.value,
            "processing_status_message": message,
        }

        if origin_file_ids is not None:
            status["origin_file_ids"] = origin_file_ids

        if total_time:
            status["processing_time_length"] = total_time

        return status


@retry(
    retry=retry_if_exception_type(psycopg2.OperationalError),
    wait=wait_random(min=2, max=10),
    stop=stop_after_attempt(10),
    reraise=True,
)
def fetch_data() -> None:
    """
    Requeue previously failed files by invoking this Lambda asynchronously.

    Reads failed records from MetaTracker tables, wraps each file as an SNS-like
    event payload, and invokes the current Lambda function for retry processing.

    Returns
    -------
    None
    """
    mission_name = os.getenv("SWXSOC_MISSION", "swxsoc")
    secret_arn = os.getenv("RDS_SECRET_ARN")
    lambda_function_name = os.getenv("AWS_LAMBDA_FUNCTION_NAME")

    if not secret_arn:
        swxsoc.log.error("No RDS Secret ARN found in environment variables.")
        return

    if not lambda_function_name:
        swxsoc.log.error("No Lambda function name found in environment variables.")
        return

    try:
        # Get RDS credentials
        session = boto3.session.Session()
        sm_client = session.client("secretsmanager")
        rds_secret = json.loads(
            sm_client.get_secret_value(SecretId=secret_arn)["SecretString"]
        )

        connection_string = (
            f"postgresql://{rds_secret['username']}:{rds_secret['password']}@"
            f"{rds_secret['host']}:{rds_secret['port']}/{rds_secret['dbname']}"
        )

        query = f"""
        SELECT
            sf.s3_key,
            sf.s3_bucket,
            array_agg(soa.origin_file_id) as origin_file_ids
        FROM {mission_name}_status s
        JOIN {mission_name}_science_file sf ON s.science_file_id = sf.science_file_id
        LEFT JOIN {mission_name}_status_origin_association soa ON s.status_id = soa.status_id
        WHERE s.processing_status = 'failed'
        GROUP BY sf.s3_key, sf.s3_bucket;
        """

        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        lambda_client = session.client("lambda")

        for s3_key, s3_bucket in results:
            swxsoc.log.info(
                f"Invoking Lambda for file: {s3_key} from bucket: {s3_bucket}"
            )

            payload = {
                "Records": [
                    {"s3": {"bucket": {"name": s3_bucket}, "object": {"key": s3_key}}}
                ]
            }

            # Wrap it as if it were from an SNS message
            sns_event = {
                "Records": [
                    {
                        "Sns": {
                            "Message": json.dumps({"Records": [payload["Records"][0]]})
                        }
                    }
                ]
            }

            lambda_client.invoke(
                FunctionName=lambda_function_name,
                InvocationType="Event",  # Async invocation
                Payload=json.dumps(sns_event).encode("utf-8"),
            )

        cursor.close()
        conn.close()

    except Exception as e:
        swxsoc.log.error(f"Error in fetch_data(): {e}", exc_info=True)
