import io
import fastf1
import boto3
import pandas as pd
import datetime
from logger import Logger
from boto3.dynamodb.conditions import Attr

dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
events_table = 'F1EventsSchedule'
lambda_client = boto3.client('lambda')
cloudwatch_events = boto3.client('events')
bucket_name = 'race-predictor-pro'
prefix = 'f1_data'

logger = Logger.get_logger()


def data_ingestion_lambda_handler(event, context):
    try:
        data_ingestion = DataIngestion(bucket_name, prefix)
        eventName = data_ingestion.fetch_and_load_latest_race()
        data_ingestion.mark_latest_events_as_processed(eventName)
        lambda_client.invoke(
            FunctionName='F1RaceSchedulerLambda',
            InvocationType='Event'
        )
        return {
            'statusCode': 200,
            'body': 'latest race date uploaded successfully'
        }

    except Exception as e:
        logger.error(f'Error processing data: {e}')
        return {
            'statusCode': 500,
            'body': f'Error processing data: {e}'
        }


class DataIngestion:

    def __init__(self, bucket, prefix):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix

    def fetch_and_upload_race(self, year, race_name, session_types):
        for session_type in session_types:
            f1_session = fastf1.get_session(year, race_name, session_type)
            f1_session.load()

            # Define S3 paths
            base_path = f"{self.prefix}/{year}/{race_name.replace(' ', '-').lower()}"

            if session_type == 'R':
                drivers = f1_session.drivers
                dummy_df = []

                for driver in drivers:
                    df = pd.DataFrame(f1_session.get_driver(driver)).T
                    dummy_df.append(df)

                driver_df = pd.concat(dummy_df, ignore_index=True)
                drivers_info_s3_path = f"{base_path}/{session_type.lower()}_drivers_info.parquet"
                self.upload_parquet_to_s3(driver_df, drivers_info_s3_path)

            # Individual lap information
            laps_df = f1_session.laps
            weather_df = f1_session.weather_data

            # s3 paths
            lap_s3_path = f"{base_path}/{session_type.lower()}_laps.parquet"
            weather_s3_path = f"{base_path}/{session_type.lower()}_weather.parquet"

            # Convert DataFrames to Parquet and upload to S3
            self.upload_parquet_to_s3(laps_df, lap_s3_path)
            self.upload_parquet_to_s3(weather_df, weather_s3_path)

    def upload_parquet_to_s3(self, df, s3_path):
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            self.s3_client.put_object(Bucket=self.bucket, Key=s3_path, Body=buffer.getvalue())
            print(f"Successfully uploaded {s3_path} to S3.")
        except Exception as e:
            print(f"Failed to upload {s3_path} to S3. Error: {e}")

    def initial_load(self, start_year, end_year):
        for year in range(start_year, end_year + 1):
            f1_schedule = fastf1.get_event_schedule(year)
            for _, row in f1_schedule.iterrows():
                race_name = row['EventName']
                if row['EventFormat'] == 'testing':
                    continue
                race_date = row['Session5DateUtc']
                if race_date < datetime.datetime.utcnow():
                    if row['F1ApiSupport']:
                        session_types = ['FP1', 'FP2', 'FP3', 'Q', 'R']
                        self.fetch_and_upload_race(year, race_name, session_types)

    def fetch_and_load_latest_race(self) -> str:
        table = dynamodb_resource.Table(events_table)

        response = table.scan(
            FilterExpression=Attr('Processed').eq(False)
        )

        events = response['Items']

        if not events:
            return ""

        # Find the latest race event
        latest_event = min(events, key=lambda x: x['EventDate'])
        session_types = ['FP1', 'FP2', 'FP3', 'Q', 'R']
        self.fetch_and_upload_race(datetime.datetime.now().year, latest_event['EventName'], session_types)
        return latest_event['EventName']

    def mark_latest_events_as_processed(self, event_name):
        table = dynamodb_resource.Table(events_table)

        response = table.scan(
            FilterExpression=Attr('EventName').eq(event_name) & Attr('Processed').eq(False)
        )
        if response['Items']:
            event = response['Items'][0]
            dynamodb_client.update_item(
                tableName='F1EventsSchedule',
                key={'EventName': event['EventName'], 'EventDate': event['EventDate']},
                UpdateExpression='SET Processed = :val1',
                ExpressionAttributeValues={':val1': True},
            )

