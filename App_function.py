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
    app_type = event['ResourceProperties']['AppType']
    try:
        client.describe_app(
            DomainId=domain_id, UserProfileName=user_profile_name,
            AppType=app_type,
            AppName=app_name)
    except ClientError as exception:
        cfnresponse.send(event, context, cfnresponse.SUCCESS,
                         {}, physicalResourceId=event['PhysicalResourceId'])
        return
    delete_app(domain_id, user_profile_name, app_type, app_name)
    cfnresponse.send(event, context, cfnresponse.SUCCESS, {},
                     physicalResourceId=event['PhysicalResourceId'])


def create_app(config):
    domain_id = config['DomainId']
    user_profile_name = config['UserProfileName']
    app_name = config['AppName']
    app_type = config['AppType']
    resource_spec = config['ResourceSpec']

    response = client.create_app(
        DomainId=domain_id,
        UserProfileName=user_profile_name,
        AppType=app_type,
        AppName=app_name,
        ResourceSpec=resource_spec
    )

    created = False
    while not created:
        response = client.describe_app(
            DomainId=domain_id, UserProfileName=user_profile_name,
            AppType=app_type,
            AppName=app_name
        )
        time.sleep(5)
        if response['Status'] == 'InService':
            created = True

    logging.info("**SageMaker app created successfully: %s", domain_id)
    return response


def delete_app(domain_id, user_profile_name, app_type, app_name):
    deleted = False
    try:
        response = client.delete_app(
            DomainId=domain_id,
            UserProfileName=user_profile_name,
            AppType=app_type,
            AppName=app_name
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'ValidationException':
            print('Deleted')

    while not deleted:
        try:
            client.describe_app(
                DomainId=domain_id, UserProfileName=user_profile_name,
                AppType=app_type,
                AppName=app_name)
        except ClientError as error:
            if error.response['Error']['Code'] == 'ResourceNotFound':
                print('Deleted')
                deleted = True
                return
        time.sleep(5)
    return response
