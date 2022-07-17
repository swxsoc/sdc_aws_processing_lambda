import os
from aws_cdk import Stack, aws_lambda, aws_ecr
from constructs import Construct
import logging


class SortingLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Container Image ECR Function
        sdc_aws_processing_function = aws_lambda.DockerImageFunction(
            scope=self,
            id="sdc_aws_sorting_lambda_function",
            function_name="sdc_aws_sorting_lambda_function",
            description=(
                "SWSOC Processing Lambda function deployed using AWS CDK Python"
            ),
            code=aws_lambda.AssetCode("lambda_function"),
        )

        logging.info("Function created successfully: %s", sdc_aws_processing_function)
