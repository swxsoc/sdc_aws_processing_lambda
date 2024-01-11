# SWSOC File Processing Lambda Container

| **CodeBuild Status** |![aws build status](https://codebuild.us-east-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiNi9WaG5pa1V4MUpoVURjRXlWc0w5d1lKR293RWJPSGtudmUzNHljd2JWaHZaQ09TVE12UTVOMWdFdU9rMFA1QWs0eCtLTW9vblV1emNwQ01HN0hqMm9vPSIsIml2UGFyYW1ldGVyU3BlYyI6IjdUVHlYZUZsc0dCV2lnUDAiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main)|
|-|-|

### **Base Image Used For Container:** https://github.com/HERMES-SOC/docker-lambda-base 

### **Description**:
This repository is to define the image to be used for the SWSOC file processing Lambda function container. This container will be built and and stored in the appropriate development/production ECR Repo. 

The container will contain the latest release code as the production environment and the latest code on master as the development. 

### **Testing Locally (Using own Test Data)**:
1. Build the lambda container image (from within the lambda_function folder) you'd like to test: 
    
    `docker build -t processing_function:latest . --no-cache`

2. Run the lambda container image you've built, this will start the lambda runtime environment:
    
    `docker run -p 9000:8080 -v <directory_for_processed_files>:/test_data -e SDC_AWS_FILE_PATH=/test_data/<file_to_process_name> processing_function:latest`

3. From a `separate` terminal, make a curl request to the running lambda function:

    `curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d @lambda_function/tests/test_data/test_eea_event.json`

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
This lambda function is part of the main SWxSOC Pipeline ([Architecture Repo Link](https://github.com/HERMES-SOC/sdc_aws_pipeline_architecture)). It is deployed via AWS Codebuild within that repository. It is first built and tagged within the appropriate production or development repository (depending if it is a release or commit). View the Codebuild CI/CD file [here](buildspec.yml).