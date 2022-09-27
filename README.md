# SWSOC File Processing Lambda Container

| **CodeBuild Status** |![aws build status](https://codebuild.us-east-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiNi9WaG5pa1V4MUpoVURjRXlWc0w5d1lKR293RWJPSGtudmUzNHljd2JWaHZaQ09TVE12UTVOMWdFdU9rMFA1QWs0eCtLTW9vblV1emNwQ01HN0hqMm9vPSIsIml2UGFyYW1ldGVyU3BlYyI6IjdUVHlYZUZsc0dCV2lnUDAiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main)|
|-|-|

### **Base Image Used For Container:** https://github.com/HERMES-SOC/docker-lambda-base 

### **Description**:
This repository is to define the image to be used for the SWSOC file processing Lambda function container. This container will be built and and stored in an ECR Repo. 
The container will contain the latest release code as the production environment and the latest code on master as the development. Files with the appropriate naming convention will be handled in production while files prefixed with `dev_` will be handled using the development environment.

### **Testing Locally**:
1. Build the lambda container image you'd like to test: 
    
    `docker build -t myfunction:latest .`

2. Run the lambda container image you've built (After using your mfa script), this will start the lambda runtime environment:
    
    `docker run -p 9000:8080 -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID myfunction:latest`

3. From a `separate` terminal, make a curl request to the running lambda function:

    `curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2022-07-25T09:35:08.284Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AIDAVD4XLJ3QS3NKL4PSC"},"requestParameters":{"sourceIPAddress":"109.175.193.59"},"responseElements":{"x-amz-request-id":"4H9A5X20QMSB7B5B","x-amz-id-2":"EHz4G7hn4dAREyeI5yQMYzkDYyfuowiwjMbG/KVsxeRGRmf3bS4DoQ2EY617fASV9FzhCviD6nPTcYJQeeyUvk8JY/WV7WXp"},"s3":{"s3SchemaVersion":"1.0","configurationId":"arn:aws:cloudformation:us-east-1:351967858401:stack/SDCAWSSortingLambdaStack/57843660-0bfc-11ed-86da-1231e463b7cd--7645585606264049987","bucket":{"name":"hermes-merit","ownerIdentity":{"principalId":"A3V7OORH2511GS"},"arn":"arn:aws:s3:::swsoc-incoming"},"object":{"key":"hermes_EEA_l0_2022269-030143_v01.bin","size":68221,"eTag":"32d82e8a2e72af004c557c4e369e89ff","sequencer":"0062DE63CC330B223E"}}}]}'`


# Information on working with a CDK Project

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands for CDK

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation