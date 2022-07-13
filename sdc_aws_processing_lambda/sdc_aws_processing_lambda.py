from aws_cdk import (
    Stack,
    aws_lambda,
    aws_iam as iam,
)

from constructs import Construct

class SDCAWSProcessingLambdaStack(Stack):

   def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create IAM Lambda execution role
        lambda_exec_role = iam.Role(
            self,
            'swsoc-lambda-processing-function',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            role_name='swsoc-lambda-processing-function',
            description='Role used by SWSOC Processing Lambda Function.',
            managed_policies=[
                iam.ManagedPolicy.from_managed_policy_arn(self,
                    'admin_role',
                    'arn:aws:iam::aws:policy/AdministratorAccess'
                 )
            ]
        )

        ### Create Cognito Remediator Lambda function
        sdc_aws_processing_function = aws_lambda.DockerImageFunction(
            scope=self,
            id="sdc_aws_processing_lambda_function",
            function_name="sdc_aws_processing_lambda_function",
            description="SWSOC Processing Lambda function deployed using AWS CDK Python",
            code=aws_lambda.DockerImageCode.from_image_asset("./sdc_aws_processing_lambda/assets/"),
        )



