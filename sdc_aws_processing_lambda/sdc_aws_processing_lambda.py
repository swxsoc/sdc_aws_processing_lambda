from aws_cdk import (
    Stack,
    aws_lambda,
    aws_ecr,
    aws_ecr_assets,
    aws_iam as iam,
)

from constructs import Construct

class SDCAWSProcessingLambdaStack(Stack):

   def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ecr_repository = aws_ecr.Repository(
                            self, 
                            id="Repo", 
                            repository_name="sdc_aws_processing_lambda_function",
                            )

        docker_image_asset = aws_ecr_assets.DockerImageAsset(self, "sdc_aws_processing_lambda",
            directory="./sdc_aws_processing_lambda/assets",
            file="Dockerfile"
            )

        ### Create Cognito Remediator Lambda function
        sdc_aws_processing_function = aws_lambda.DockerImageFunction(
            scope=self,
            id="sdc_aws_processing_lambda_function",
            function_name="sdc_aws_processing_lambda_function",
            description="SWSOC Processing Lambda function deployed using AWS CDK Python",
            code=aws_lambda.DockerImageCode.from_ecr(ecr_repository),
        )



