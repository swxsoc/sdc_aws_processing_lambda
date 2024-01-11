import pytest
import os
import json
from moto import mock_s3
from pathlib import Path


os.environ["SDC_AWS_CONFIG_FILE_PATH"] = "lambda_function/src/config.yaml"
from src.file_processor.file_processor import (  # noqa: E402
    handle_event,  # noqa: E402
    FileProcessor,  # noqa: E402
)  # noqa: E402


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3(aws_credentials):
    with mock_s3():
        yield


def test_file_calibrate():
    # Setup
    intrument = "eea"
    parent_dir = "lambda_function/tests/test_data"
    binary_file = "hermes_EEA_l0_2023042-000000_v0.bin"

    # Calibrate
    file_path = Path(parent_dir, binary_file)

    calibrated_file_path = FileProcessor._calibrate_file(intrument, file_path)

    # Verify
    assert calibrated_file_path == "hermes_eea_l1_20230211T000000_v1.0.0.cdf"

    # Cleanup
    os.remove(Path(parent_dir, calibrated_file_path))


def test_file_calibrate_failure():
    # Setup
    instrument = "eea"
    file_path = "lambda_function/tests/test_data/nonexistent-file.bin"

    # Calibrate
    file_path = Path(file_path)

    calibrated_file_path = FileProcessor._calibrate_file(instrument, file_path)

    # Verify
    assert calibrated_file_path is None


# Test handle event and pass in the test_eea_event.json file as json
def test_handle_event(s3):
    filename = "hermes_EEA_l0_2023042-000000_v0.bin"
    parent_dir = "lambda_function/tests/test_data"
    calibrated_file_path = "hermes_eea_l1_20230211T000000_v1.0.0.cdf"
    # Set the absolute path using file_path as string
    os.environ["SDC_AWS_FILE_PATH"] = f"lambda_function/tests/test_data/{filename}"

    # Setup
    event = json.load(open("lambda_function/tests/test_data/test_eea_event.json"))

    # Exercise
    response = handle_event(event, None)

    # Verify
    assert response["statusCode"] == 200

    # Cleanup
    os.remove(Path(parent_dir, calibrated_file_path))

    # Test unexpected event
    filename = "nonexistent-file.bin"
    os.environ["SDC_AWS_FILE_PATH"] = f"lambda_function/tests/test_data/{filename}"

    # Setup
    event = json.load(open("lambda_function/tests/test_data/test_eea_event.json"))

    # Exercise
    response = handle_event(event, None)

    # Verify
    assert response["statusCode"] == 500
