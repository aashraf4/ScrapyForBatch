import os
import boto3
from dotenv import load_dotenv
from datetime import datetime

parent_directory = os.path.dirname(os.getcwd())
load_dotenv(parent_directory)

today = datetime.today()
date = today.strftime("%Y-%m-%d")
output_date = today.strftime("%Y%m%d")

def lambda_handler(event, context):
    # Retrieve AWS credentials from environment variables
    aws_access_key_id = os.environ.get("aws_key")
    aws_secret_access_key = os.environ.get("aws_secret")
    
    # Define the DynamoDB table name
    table_name = os.environ.get("ddb_ARN")
    
    # Create a DynamoDB resource
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="eu-central-1"
    )

    # Get the DynamoDB table
    table = dynamodb.Table(table_name)

    # Retrieve data from the DynamoDB table
    response = table.scan(
        FilterExpression="contains(job_name, :output_date)",
        ExpressionAttributeValues={":output_date": output_date}
    )
    
    # Process retrieved items
    statuses = []
    for item in response.get('Items', []):
        status = item.get('Status')
        name = item.get('job_name', '').split("-")[0]
        if status and name:
            statuses.append(f"{name}: {status}")

    # Create an SNS client
    sns_client = boto3.client(
        'sns',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="eu-central-1"
    )

    # Publish the message to the specified topic
    sns_topic_arn = os.environ.get("sns_ARN")
    message = "Scrapy Batch Job Ran and the result is:\n" + "\n".join(statuses)
    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message
    )

    return {
        'statusCode': 200,
        'body': "Message published successfully"
    }
