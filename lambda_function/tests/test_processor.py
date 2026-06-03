import json
import os
from pathlib import Path

import boto3
import pytest
from moto import mock_aws as moto_mock_aws
from src.file_processor.file_processor import FileProcessor  # noqa: E402
from src.file_processor.file_processor import handle_event  # noqa: E402
from swxsoc import log

TEST_REGION = "us-east-1"

log.disable_warnings_logging()  # noqa: E402


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def mock_aws():
    """Mock AWS services using moto."""
    with moto_mock_aws():
        yield


@pytest.fixture(scope="function")
def s3_client(aws_credentials, mock_aws):
    """S3 client fixture"""
    conn = boto3.client("s3", region_name=TEST_REGION)
    yield conn


# fmt: off
@pytest.mark.parametrize(
    ("use_mission", "instrument", "test_file", "n_expected", "expected_output"),
    [
        # Disable HERMES Processing Test - HERMES processing code is out-of-date.
        # ("hermes", "hermes_EEA_l0_2023042-000000_v0.bin", 1, "hermes_eea_l1_20000101T170901_v1.0.0.cdf"),
        # PADRE Craft - No Calibration Expected
        ("padre", "meddea", "padre_get_CUBEADCS_GEN2_OP_STATUS_APP_Data_1761936771334_1762106179414.csv", 0, None),
        # PADRE MEDDEA Photon File
        ("padre", "meddea", "padreMDA0_240916122901.dat", 1, "padre_meddea_l0_photon_20240916T122901_v1.0.0.fits"),
        # PADRE MEDDEA Spectrum 
        ("padre", "meddea", "padreMDA2_240916122851.dat", 1, "padre_meddea_l0_spectrum_20240916T122851_v1.0.0.fits"),
        # PADRE MEDDEA Housekeeping
        ("padre", "meddea", "padreMDU8_240916122904.dat", 1, "padre_meddea_l0_housekeeping_20240916T122904_v1.0.0.fits"),
        # REACH - UDL JSON Download
        ("swxsoc_pipeline", "reach", "REACH-ALL_20251201T013010_20251205T060517.json", 1, "reach_all_l1c_prelim_20251201T000000_v1.0.0.cdf"),
        # REACH - UDL CSV Download - Single Spacecraft
        ("swxsoc_pipeline", "reach", "REACH-ALL_20251205T060517_20251205T060517.csv", 1, "reach_all_l1c_prelim_20251201T070010_v1.0.0.cdf"),
        # REACH - UDL CSV Download - Multiple Spacecraft
        ("swxsoc_pipeline", "reach", "REACH-ALL_20250901T000000_20250902T000000.csv", 1, "reach_all_l1c_prelim_20250901T000000_v1.0.0.cdf"),
        # REACH - UDL CSV Download - Multiple Spacecraft
        ("swxsoc_pipeline", "reach", "REACH-TEST_20250904T000000_20250904T010000.csv", 1, "reach_all_l1c_prelim_20250904T000000_v1.0.0.cdf"),
    ],
    indirect=["use_mission"],
)
#fmt: on
def test_file_calibrate(
    use_mission,
    instrument,
    test_file,
    n_expected,
    expected_output,
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)

    # Setup
    parent_dir = Path(__file__).parent / "test_data"
    binary_file = test_file

    # Calibrate
    file_path = parent_dir / binary_file
    calibrated_files = FileProcessor._calibrate_file(instrument, file_path)
    print(calibrated_files)

    # Verify
    assert len(calibrated_files) == n_expected
    if n_expected > 0:
        assert calibrated_files[0] == expected_output

@pytest.mark.parametrize("use_mission", ["swxsoc_pipeline"], indirect=True)
def test_file_calibrate_failure(use_mission):
    # Setup
    instrument = "reach"
    file_path = "lambda_function/tests/test_data/nonexistent-file.bin"

    # Calibrate
    file_path = Path(file_path)

    calibrated_file_path = FileProcessor._calibrate_file(instrument, file_path)

    # Verify
    assert calibrated_file_path is None


# Test handle event and pass in the test_eea_event.json file as json
@pytest.mark.parametrize("use_mission", ["swxsoc_pipeline"], indirect=True)
def test_handle_event(use_mission, s3_client, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    filename = "REACH-TEST_20250904T000000_20250904T010000.csv"
    parent_dir = Path(__file__).parent / "test_data"
    
    # Set the absolute path using file_path as string
    os.environ["SDC_AWS_FILE_PATH"] = str(parent_dir / filename)

    # Setup
    event = json.load(open(parent_dir / "test_reach_event.json"))

    # Exercise
    response = handle_event(event, None)

    # Verify
    assert response["statusCode"] == 200

    # Test unexpected event
    filename = "nonexistent-file.bin"
    os.environ["SDC_AWS_FILE_PATH"] = str(parent_dir / filename)

    # Setup
    event = json.load(open(parent_dir / "test_reach_event.json"))

    # Exercise
    response = handle_event(event, None)

    # Verify
    assert response["statusCode"] == 200
