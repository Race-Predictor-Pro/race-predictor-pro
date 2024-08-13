import fastf1
import boto3
import datetime
from load.data_loader import DataIngestion
from logger import Logger
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
events_table = 'F1EventsSchedule'
lambda_client = boto3.client('lambda')
cloudwatch_events = boto3.client('events')
bucket_name = 'race-predictor-pro'
prefix = 'f1_data'

logger = Logger.get_logger()


def create_dynamoDB_table():
    try:
        existing_table = dynamodb.list_tables()['TableNames']

        if events_table not in existing_table:
            dynamodb.create_table(
                TableName=events_table,
                KeySchema=[
                    {'AttributeName': 'EventDate', 'KeyType': 'HASH'},
                    {'AttributeName': 'EventName', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'EventDate', 'AttributeType': 'S'},
                    {'AttributeName': 'EventName', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            logger.info('Creating table' + events_table + '...')
            table_resources = dynamodb_resource.Table(events_table)
            table_resources.wait_until_exists()
            logger.info(f'Table {events_table} created successfully')

    except Exception as e:
        logger.error(f"Error creating table {e}")


def load_event_schedule_to_dynamodb(start_year, end_year):
    for year in range(start_year, end_year + 1):
        schedule = fastf1.get_event_schedule(year)
        for _, row in schedule.iterrows():
            event_date = row['Session5DateUtc']
            event_name = row['EventName']
            if row['EventFormat'] == 'testing':
                continue
            event_processed = event_date < datetime.datetime.utcnow()

            dynamodb.put_item(
                TableName=events_table,
                Item={
                    'EventDate': {'S': event_date.strftime('%Y-%m-%dT%H:%M:%SZ')},
                    'EventName': {'S': event_name},
                    'Processed': {'BOOL': event_processed}
                }
            )
    logger.info('Done loading event schedule')


def schedule_next_race_trigger(rule_name='F1DataIngestionTrigger', lambda_function_name='F1DataIngestionLambda'):
    try:
        table = dynamodb_resource.Table(events_table)

        # Scan the table for the next unprocessed event
        response = table.scan(
            FilterExpression=Attr('Processed').eq(False)
        )

        events = response['Items']
        if not events:
            logger.warning("No unprocessed events found in the schedule.")
            return {
                'statusCode': 404,
                'body': 'No unprocessed events found in the schedule.'
            }

        # Find the event with the earliest date
        next_event = min(events, key=lambda x: x['EventDate'])
        next_event_date = datetime.datetime.strptime(next_event['EventDate'], '%Y-%m-%dT%H:%M:%SZ')
        logger.info(f"Next event is {next_event['EventName']} on {next_event_date}")

        # Schedule the Lambda function to run the day after the race
        trigger_date = next_event_date + datetime.timedelta(days=1)

        # Define the cron expression
        cron_expression = f'cron({trigger_date.minute} {trigger_date.hour} {trigger_date.day} {trigger_date.month} ? {trigger_date.year})'

        # Create or update the CloudWatch rule
        rule_arn = cloudwatch_events.put_rule(
            Name=rule_name,
            ScheduleExpression=cron_expression,
            State='ENABLED'
        )['RuleArn']

        # Add permission for CloudWatch to invoke the Lambda function
        lambda_client.add_permission(
            FunctionName=lambda_function_name,
            StatementId=f'AllowExecutionFromCloudWatch_{rule_name}',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=rule_arn
        )

        # Add the Lambda function as a target for the CloudWatch rule
        cloudwatch_events.put_targets(
            Rule=rule_name,
            Targets=[{
                'Id': '1',
                'Arn': lambda_client.get_function(FunctionName=lambda_function_name)['Configuration']['FunctionArn']
            }]
        )

        logger.info(f"Scheduled next race trigger for {trigger_date}")

        return {
            'statusCode': 200,
            'body': 'Next race trigger scheduled successfully'
        }

    except Exception as e:
        logger.error(f"Error scheduling next race trigger: {e}")
        return {
            'statusCode': 500,
            'body': f"Error scheduling next race trigger: {e}"
        }


def schedule_lambda_handler(event, context):
    return schedule_next_race_trigger()