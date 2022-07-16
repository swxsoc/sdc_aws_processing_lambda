from aws_cdk import Stack, aws_lambda, aws_ecr
from constructs import Construct
import logging


class SDCAWSProcessingLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ECR Repo Name
        repo_name = "sdc_aws_processing_lambda"

        # Use existing ecr repo or create new one
        try:
            logging.info("Using Existing %s repo", repo_name)
            # Get SDC Processing Lambda ECR Repo
            ecr_repository = aws_ecr.Repository.from_repository_name(
                self, id=f"{repo_name}_repo", repository_name=repo_name
            )
        except BaseException as error:
            logging.error("Error %s trying to get repo: %s", error, repo_name)
            # Get SDC Processing Lambda ECR Repo
            ecr_repository = aws_ecr.Repository(
                self, id=f"{repo_name}_repo", repository_name=repo_name
            )

        # Create Container Image ECR Function
        sdc_aws_processing_function = aws_lambda.DockerImageFunction(
            scope=self,
            id=f"{repo_name}_function",
            function_name=f"{repo_name}_function",
            description=(
                "SWSOC Processing Lambda function deployed using AWS CDK Python"
            ),
            code=aws_lambda.DockerImageCode.from_ecr(ecr_repository),
        )

        logging.info("Function created successfully: %s", sdc_aws_processing_function)
