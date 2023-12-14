import boto3
import json
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Specify your Lambda function name and region
    target_lambda_name = 'YOUR_SPOTIFY_API_DATA_RETRIEVAL_LAMBDA_NAME'
    region_name = 'YOUR_AWS_REGION'

    # Create a CloudWatch Events client
    cloudwatch_events = boto3.client('events', region_name=region_name)

    try:
        # Create a rule that schedules the Lambda function to run every 24 hours
        response = cloudwatch_events.put_rule(
            Name='SpotifyDataRetrievalSchedule',
            ScheduleExpression='rate(24 hours)',
            State='ENABLED'
        )

        # Add the Lambda function as a target to the CloudWatch Events rule
        cloudwatch_events.put_targets(
            Rule='SpotifyDataRetrievalSchedule',
            Targets=[
                {
                    'Id': '1',
                    'Arn': f'arn:aws:lambda:{region_name}:{context.invoked_function_arn.split(":")[4]}:function:{target_lambda_name}',
                },
            ]
        )

        return {
            'statusCode': 200,
            'body': 'Lambda scheduled successfully'
        }

    except ClientError as e:
        return {
            'statusCode': 500,
            'body': f'Error scheduling Lambda: {e.response["Error"]["Message"]}'
        }
