#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk_deployment.sdc_aws_processing_lambda import SDCAWSProcessingLambdaStack


app = cdk.App()
SDCAWSProcessingLambdaStack(
    app,
    "SDCAWSProcessingLambdaStack",
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region="us-east-1"),
)

app.synth()
