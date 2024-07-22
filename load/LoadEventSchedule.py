import fastf1
import boto3
import datetime
from F1DataIngestion import F1DataIngestion

dynamodb = boto3.client('dynamodb')
events_table = 'F1EventsSchedule'
lambda_client = boto3.client('lambda')
cloudwatch_events = boto3.client('events')
bucket_name = 'your-s3-bucket'
prefix = 'f1_data'


def load_event_schedule_to_dynamodb(start_year, end_year):
    f1_ingestion = F1DataIngestion(bucket_name, prefix)

    for year in range(start_year, end_year + 1):
        schedule = fastf1.get_event_schedule(year)
        for _, row in schedule.iterrows():
            event_date = row['Session5DateUtc']
            event_name = row['EventName']
            event_processed = event_date < datetime.datetime.utcnow()

            dynamodb.put_item(
                TableName=events_table,
                Item={
                    'EventDate': {'S': event_date.strftime('%Y-%m-%dT%H:%M:%SZ')},
                    'EventName': {'S': event_name},
                    'Processed': {'BOOL': event_processed}
                }
            )

def schedule_next_race_trigger():
    # Fetch unprocessed events from DynamoDB
    response = dynamodb.scan(
        TableName=events_table,
        FilterExpression='Processed = :p',
        ExpressionAttributeValues={':p': {'BOOL': False}}
    )

    events = response['Items']

    if not events:
        return

    # Find the next race event
    next_event = min(events, key=lambda x: x['EventDate']['S'])
    next_event_date = datetime.datetime.strptime(next_event['EventDate']['S'], '%Y-%m-%dT%H:%M:%SZ')

    # Schedule the Lambda function to run the day after the race
    next_event_date += datetime.timedelta(days=1)

    rule_name = 'F1DataIngestionTrigger'
    rule_arn = cloudwatch_events.put_rule(
        Name=rule_name,
        ScheduleExpression=f'cron({next_event_date.minute} {next_event_date.hour} {next_event_date.day} {next_event_date.month} ? {next_event_date.year})',
        State='ENABLED'
    )['RuleArn']

    # Add permission for CloudWatch to invoke the Lambda function
    lambda_client.add_permission(
        FunctionName='F1DataIngestionLambda',
        StatementId='AllowExecutionFromCloudWatch',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com',
        SourceArn=rule_arn
    )

    # Add the Lambda function as a target for the CloudWatch rule
    cloudwatch_events.put_targets(
        Rule=rule_name,
        Targets=[{
            'Id': '1',
            'Arn': 'ARN_number'
        }]
    )


def schedule_lambda_handler(event, context):
    schedule_next_race_trigger()
    return {
        'statusCode': 200,
        'body': 'Next race trigger scheduled successfully'
    }