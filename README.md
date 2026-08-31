# SWSOC File Processing Lambda Container

| **CodeBuild Status** |![aws build status](https://codebuild.us-east-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiNi9WaG5pa1V4MUpoVURjRXlWc0w5d1lKR293RWJPSGtudmUzNHljd2JWaHZaQ09TVE12UTVOMWdFdU9rMFA1QWs0eCtLTW9vblV1emNwQ01HN0hqMm9vPSIsIml2UGFyYW1ldGVyU3BlYyI6IjdUVHlYZUZsc0dCV2lnUDAiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main)|
|-|-|

### **Base Image Used For Container:** https://github.com/HERMES-SOC/docker-lambda-base 

### **Description**:
This repository is to define the image to be used for the SWSOC file processing Lambda function container. This container will be built and and stored in the appropriate development/production ECR Repo. 

The container will contain the latest release code as the production environment and the latest code on `main` as the development.

### **Testing Locally (Using own Test Data)**:
1. Build the lambda container image (from within the lambda_function folder) you'd like to test: 
    
```sh
docker build \
    --build-arg BASE_IMAGE=$BASE_IMAGE \                  # Optional: specify base image
    --build-arg REQUIREMENTS_FILE=$REQUIREMENTS_FILE \    # Optional: specify requirements file
    -t sdc_aws_processing_lambda:latest . \
    --network host
```

2. Run the lambda container image you've built, this will start the lambda runtime environment:
    
```sh
docker run \
  -p 9000:8080 \
  -v <directory_for_processed_files>:/test_data \
  -e SDC_AWS_FILE_PATH=/test_data/<file_to_process_name> \
  sdc_aws_processing_lambda:latest
```

3. From a `separate` terminal, make a curl request to the running lambda function:

```sh
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d @lambda_function/tests/test_data/test_eea_event.json
```

4. Close original terminal running the docker image.

5. Clean up dangling images and containers:

    `docker system prune`

### **Testing Locally (Using own Instrument Package Test Data)**:
1. Build the lambda container image (from within the lambda_function folder) you'd like to test: 
    
    `docker build -t processing_function:latest . --no-cache`

2. Run the lambda container image you've built, this will start the lambda runtime environment:
    
    `docker run -p 9000:8080 -v <directory_for_processed_files>:/test_data -e USE_INSTRUMENT_TEST_DATA=True processing_function:latest`

3. From a `separate` terminal, make a curl request to the running lambda function:

    `curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @lambda_function/tests/test_data/test_eea_event.json`

4. Close original terminal running the docker image.

5. Clean up dangling images and containers:

    `docker system prune`


### **How this Lambda Function is deployed**
This lambda function is part of the main SWxSOC Pipeline ([Architecture Repo Link](https://github.com/swxsoc/sdc_aws_architecture)). It is deployed via AWS CodeBuild within that repository. It is first built and tagged within the appropriate production or development repository (depending on whether it is a release or a commit). View the CodeBuild CI/CD file [here](buildspec.yml).

CodeBuild publishes only an exact current `main` commit or a release tag. Pull
requests, stale commits, and other branches validate without pushing an image.
The mission is derived from the CodeBuild project name. Development is the
default; `CDK_ENVIRONMENT=PRODUCTION` or a release tag selects production.

When a mission base-image build starts this project, it passes a versioned
`PUBLIC_ECR_REPO` URI and the normalized `CDK_ENVIRONMENT`. The build verifies
that the URI belongs to the expected mission and environment and rejects
`latest` before using it. Direct Lambda builds fall back to the matching
mission base image's `latest` tag. Successful image pushes start the mission's
architecture project from its `main` branch with the immutable Lambda tag.
