import json
import time
import boto3
import logging
import cfnresponse
from botocore.exceptions import ClientError

client = boto3.client('sagemaker')


def lambda_handler(event, context):
    try:
        if event['RequestType'] == 'Create':
            handle_create(event, context)
        elif event['RequestType'] == 'Delete':
            handle_delete(event, context)
    except ClientError as exception:
        logging.error(exception)
        cfnresponse.send(event, context, cfnresponse.FAILED,
                         {}, error=str(exception))


def handle_create(event, context):
    print("**Starting running the App setup code")
    resource_config = event['ResourceProperties']

    print("**Creating App")
    response_data = create_app(resource_config)
    cfnresponse.send(event, context, cfnresponse.SUCCESS,
                     {'AppName': response_data['AppName']}, physicalResourceId=response_data['AppName'])


def handle_delete(event, context):
    print('Received delete event')
    app_name = event['PhysicalResourceId']
    domain_id = event['ResourceProperties']['DomainId']
    user_profile_name = event['ResourceProperties']['UserProfileName']
    try:
        client.ddescribe_app(
            DomainId=domain_id, UserProfileName=user_profile_name,
            AppType='KernelGateway',
            AppName=app_name)
    except ClientError as exception:
        cfnresponse.send(event, context, cfnresponse.SUCCESS,
                         {}, physicalResourceId=event['PhysicalResourceId'])
        return
    delete_app(domain_id, user_profile_name, app_name)
    cfnresponse.send(event, context, cfnresponse.SUCCESS, {},
                     physicalResourceId=event['PhysicalResourceId'])


def create_app(config):
    domain_id = config['DomainId']
    user_profile_name = config['UserProfileName']
    user_settings = config['UserSettings']
    app_name = f'sagemaker-data-wrang-ml-m5-4xlarge-{time.time_ns}'

    response = client.create_user_profile(
        DomainId=domain_id,
        UserProfileName=user_profile_name,
        UserSettings=user_settings,
    )
    response = client.create_app(
        DomainId=domain_id,
        UserProfileName=user_profile_name,
        AppType='KernelGateway',
        AppName=app_name,
        ResourceSpec={
            'SageMakerImageArn': 'arn:aws:sagemaker:us-west-2:174368400705:image/sagemaker-data-wrangler-1.0',
            'InstanceType': 'ml.m5.4xlarge'
        }
    )

    created = False
    while not created:
        response = client.describe_app(
            DomainId=domain_id, UserProfileName=user_profile_name,
            AppType='KernelGateway',
            AppName=app_name
        )
        time.sleep(5)
        if response['Status'] == 'InService':
            created = True

    logging.info("**SageMaker app created successfully: %s", domain_id)
    return response


def delete_app(domain_id, user_profile_name, app_name):
    response = client.delete_app(
        DomainId=domain_id,
        UserProfileName=user_profile_name,
        AppType='KernelGateway',
        AppName=app_name
    )
    deleted = False
    while not deleted:
        try:
            client.describe_app(
                DomainId=domain_id, UserProfileName=user_profile_name)
        except ClientError as error:
            if error.response['Error']['Code'] == 'ResourceNotFound':
                print('Deleted')
                deleted = True
                return
        time.sleep(5)
    return response
