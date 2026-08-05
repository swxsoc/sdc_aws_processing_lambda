# AGENTS.md

This file helps AI coding agents understand the repository structure, build/test conventions, and key architecture decisions for the **Space Weather SOC (SWSOC) AWS Lambda File Processing Function**.

## Project Overview

This is an AWS Lambda function that processes Space Weather Science Operations Center files through instrument-specific calibration and analysis pipelines. The function:
- Receives S3 event notifications via SNS (or performs full incoming-bucket scans)
- Parses filenames to identify source instruments (HERMES, PADRE, REACH, etc.)
- Routes files to the appropriate instrument package for processing (calibration, analysis, format conversion)
- Uploads processed results back to S3 in instrument-specific buckets
- Tracks all files and processing results in a PostgreSQL metatracker database

See [README.md](README.md) for detailed documentation and local testing instructions.

## Essential Commands

### Testing
```bash
# Run all tests with coverage
pytest lambda_function/tests --cov=lambda_function/src --cov-report=html

# Build Docker container for local Lambda testing
cd lambda_function && docker build -t sdc_aws_processing_lambda:latest .

# Run Lambda locally with test data
docker run -p 9000:8080 \
  -v "$(pwd)/lambda_function/tests/test_data:/test_data" \
  -e SDC_AWS_FILE_PATH=/test_data/test_padre_get_CUBEADCS.csv \
  sdc_aws_processing_lambda:latest

# Test Lambda endpoint from another terminal
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d @lambda_function/tests/test_data/test_eea_event.json
```

### CI/CD & Linting
```bash
# Lint with Black and Flake8 (as per buildspec.yml)
black --check --diff lambda_function
flake8 --count --max-line-length 88 lambda_function

# Build and push Docker image (run locally to test CI/CD logic)
cd lambda_function && docker build -t sdc_aws_processing_lambda:latest .
```

## Project Structure

```
lambda_function/
├── src/
│   ├── lambda.py              # Handler entry point; delegates to FileProcessor
│   ├── file_processor/
│   │   ├── __init__.py
│   │   └── file_processor.py  # Core FileProcessor class; orchestrates:
│   │                           #  - S3 file download
│   │                           #  - Instrument package dispatch
│   │                           #  - Result upload to S3
│   │                           #  - Metatracker database logging
│   └── config/
│       ├── README.md          # Config baking strategy & rationale
│       └── ccsdspy/
│           └── config.yml     # Package config file (mirrored to /tmp at runtime)
├── tests/
│   ├── conftest.py            # Shared pytest fixtures (default_test_mission)
│   ├── test_processor.py      # Test suite for FileProcessor
│   └── test_data/             # Sample S3 event payloads and raw files
├── Dockerfile                 # Lambda container image
├── entry_script.sh            # Seeds /tmp/config/ at Lambda startup
├── hermes-requirements.txt    # HERMES package dependencies
├── padre-requirements.txt     # PADRE package dependencies
├── swxsoc_pipeline-requirements.txt  # REACH Mission requirements
└── impax-requirements.txt     # IMPAX package dependencies (optional)

lambda_function/src/file_processor/file_processor.py imports from:
  - swxsoc: File I/O (get_science_file, parse_file_key, push_science_file)
  - swxsoc: Config utilities (get_instrument_bucket, get_instrument_package)
  - swxsoc: Parsing (parse_science_filename)
  - metatracker: Database engine and tracker for file/product tracking
  - Instrument packages (hermes_eea, padre_meddea, etc.) dynamically via get_instrument_package()
  - tenacity: Retry logic with exponential backoff
```

## Key Technical Details

**Python Version**: 3.12 (must match AWS Lambda runtime)

**Environment Variables**:
- `LAMBDA_ENVIRONMENT` (default: `"DEVELOPMENT"`): Not directly used in this processing lambda (unlike the sorting function); retained for consistency
- `SWXSOC_MISSION` (default: `"hermes"` in tests): Configures mission-specific behavior; set by conftest fixture
- `SDC_AWS_FILE_PATH` (optional): Used in local Docker testing; specifies input file path
- `USE_INSTRUMENT_TEST_DATA` (optional): When set to `True`, uses test data from instrument packages instead of S3

**Core Dependencies**:
- `swxsoc` (from git): S3 I/O, config/parsing utilities, mission lookup
- `metatracker` (from git): Database ORM and tracker for file/product provenance
- Instrument packages (dynamic): `hermes_eea`, `padre_meddea`, `padre_sharp`, `padre_craft`, `swxsoc_reach`
- `tenacity==9.1.2`: Retry mechanism with exponential backoff for resilience
- `psycopg2`: PostgreSQL driver for metatracker connection
- `boto3`: AWS SDK (for S3, Timestream, etc.)
- `moto==5.0.15`: Mocks AWS services in tests
- `pytest`, `pytest-astropy`, `pytest-cov`: Testing framework
- `ruff`, `black`, `flake8`: Code linting

**Linting**: Uses `ruff` with specific ignores defined in [ruff.toml](ruff.toml); also runs `black` and `flake8` in CI/CD via [buildspec.yml](buildspec.yml)

## Config Baking Strategy

This project uses a **config baking** pattern to handle packages (like `ccsdspy`) that require writable config directories at import time:

- **Repo**: Config files live in `lambda_function/src/config/<pkg>/` (versioned, reviewed)
- **Build time**: Dockerfile copies the tree to `/lambda_function/config/<pkg>/` in the image
- **Runtime**: `entry_script.sh` mirrors `/lambda_function/config/` to `/tmp/config/` (the only writable directory in Lambda)
- **Import time**: `ENV` variables point packages at `/tmp/config/<pkg>/` so they can initialize successfully

See [lambda_function/src/config/README.md](lambda_function/src/config/README.md) for full rationale and Dockerfile wiring requirements.

## Testing Conventions

- **Fixtures**: `conftest.py` provides:
  - `default_test_mission`: Auto-applied fixture that sets `SWXSOC_MISSION=hermes` for all tests (can be overridden per test)
  - `use_mission`: Fixture for tests that need a specific mission configuration

- **Mocking AWS**: Use `moto` to mock S3, RDS/databases, and other AWS services
  
- **Test Data**: Sample S3 events and raw files are in `lambda_function/tests/test_data/` (CSV files, CDF files, JSON events)
  - `test_eea_event.json`: Example SNS/S3 event for testing
  - `test_reach_event.json`, `test_padre_*`: Mission-specific test inputs

- **FileProcessor Testing**: When mocking dependencies, patch at the import location in `file_processor.py`, not upstream libraries

## Processing Workflow

The `FileProcessor` class in [lambda_function/src/file_processor/file_processor.py](lambda_function/src/file_processor/file_processor.py) orchestrates:

1. **Parse**: Extract instrument and file metadata from S3 key and filename
2. **Download**: Fetch file from source S3 bucket (or load from local path in testing)
3. **Calibrate**: Dispatch to instrument-specific package (e.g., `hermes_eea.processing`)
   - Instrument package performs calibration, analysis, or format conversion
   - Returns list of calibrated output filenames
4. **Upload**: Push calibrated results back to instrument-specific S3 bucket
5. **Track**: Log file and product records to metatracker PostgreSQL database
6. **Return**: Status response (success, failed, pending) with timing/error details

The `Status` enum (`SUCCESS`, `FAILED`, `PENDING`) tracks file processing state.

## CI/CD Workflows

See [buildspec.yml](buildspec.yml) for CodeBuild pipeline:
- **Pre-build**: Install dependencies, run `black` and `flake8` linting
- **Build**: Log into AWS ECR, build Docker image, tag with timestamp, push to ECR
- **Post-build**: Trigger downstream `build_sdc_aws_pipeline_architecture` CodeBuild to deploy

The function is deployed as a Docker image to AWS ECR and invoked by SNS S3 event notifications.

## Common Development Tasks

| Task | Command/Approach |
|------|------------------|
| Run tests | `pytest lambda_function/tests --cov=lambda_function/src --cov-report=html` |
| Build Lambda image | `cd lambda_function && docker build -t sdc_aws_processing_lambda:latest .` |
| Test Lambda locally | See [README.md](README.md) section "Testing Locally" |
| Add new test | Place in `lambda_function/tests/test_*.py`; conftest fixtures auto-apply |
| Modify processing logic | Edit `lambda_function/src/file_processor/file_processor.py` (core `_process_file()` method) |
| Add instrument package | Add to `lambda_function/*-requirements.txt`, import in `_calibrate_file()`, extend test fixtures |
| Update config for package | Add/update `lambda_function/src/config/<pkg>/config.yml`, wire ENV in Dockerfile |
| Update dependencies | Edit `lambda_function/<mission>-requirements.txt` (prod) or `requirements.dev.txt` (dev) |

## Key Decisions & Patterns

1. **Instrument dispatch via dynamic lookup**: Rather than hardcoding instrument logic, `get_instrument_package()` dynamically selects the processor based on parsed filename. This scales to new instruments without modifying the core handler.

2. **Config baking for read-only filesystems**: Lambda's filesystem is read-only except `/tmp`. Packages requiring writable config directories are seeded at startup via `entry_script.sh`, solving initialization failures at import time.

3. **Retry logic with tenacity**: Network/transient failures are retried with exponential backoff to improve robustness in production.

4. **Comprehensive database tracking**: All files and calibrated products are logged to metatracker for provenance, auditing, and downstream pipeline tracking.

5. **Modular mission/instrument support**: Multiple instrument packages coexist in the same Lambda, differentiated at runtime by filename parsing and configuration lookup.

6. **Docker for parity**: Local testing uses the exact Lambda container image to catch deployment issues early.

## Deployment

The function is deployed as a Docker image to AWS ECR:
- **Production**: Latest GitHub release (tagged image in ECR)
- **Development/Testing**: Latest commit on `main` branch

Triggered via SNS notifications from incoming S3 bucket events.

## Questions or Issues?

- For Lambda/processing logic questions, refer to [README.md](README.md)
- For swxsoc/metatracker library details, see their respective GitHub repositories
- For instrument package details, consult `hermes-requirements.txt`, `padre-requirements.txt`, etc.
- For CI/CD questions, check [buildspec.yml](buildspec.yml)
- For config baking rationale, see [lambda_function/src/config/README.md](lambda_function/src/config/README.md)
