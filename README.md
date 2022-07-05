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
