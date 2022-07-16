# SWSOC File Processing Lambda Container

### **Base Image Used For Container:** https://github.com/HERMES-SOC/docker-lambda-base 

### **Description**:
This repository is to define the image to be used for the SWSOC file processing Lambda function container. This container will be built and and stored in an ECR Repo. 
The container will contain the latest release code as the production environment and the latest code on master as the development. Files with the appropriate naming convention will be handled in production while files prefixed with `dev_` will be handled using the development environment.

### **Testing Locally**:
1. Build the lambda container image you'd like to test: 
    
    `docker build -t myfunction:latest .`

2. Run the lambda container image you've built, this will start the lambda runtime environment:
    
    `docker run -p 9000:8080  myfunction:latest`

3. From a `separate` terminal, make a curl request to the running lambda function:

    `curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"Bucket":"merit", "FileKey":"/keyname.csv"}'`


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