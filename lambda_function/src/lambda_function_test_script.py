"""
Script to be used for testing the lambda handler locally
"""
from lambda_function import handler

if __name__ == "__main__":
    # Set Event to Lambda Handler With
    event = {"FileKey": "dev_merit_test.key"}
    context = {}
    print(handler(event, context))
