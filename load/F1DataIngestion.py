import fastf1
import pandas as pd
import boto3
import io
import datetime

class F1DataIngestion:
    """Data ingestion class for fetching and uploading"""
    def __init__(self, bucket, prefix):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix

    def fetch_and_upload_race_data(self, year, race_name, session_types):
        """Fetch data from open f1 for the race and upload to S3"""
        for session_type in session_types:
            # Load the session data
            session = fastf1.get_session(year, race_name, session_type)
            session.load()

            # Prepare the dataframes
            if session_type in ['FP1', 'FP2', 'FP3', 'Q', 'R']:
                lap_df = session.laps
                telemetry_df = session.car_data
                weather_df = session.weather_data
                track_status_df = session.track_status

                # Define S3 paths
                base_path = f"{self.prefix}/{year}/{race_name.replace(' ', '-').lower()}"
                lap_s3_path = f"{base_path}/{session_type.lower()}_laps.parquet"
                telemetry_s3_path = f"{base_path}/{session_type.lower()}_telemetry.parquet"
                weather_s3_path = f"{base_path}/{session_type.lower()}_weather.parquet"
                track_status_s3_path = f"{base_path}/{session_type.lower()}_track_status.parquet"

                # Convert DataFrames to Parquet and upload to S3
                self.upload_parquet_to_s3(lap_df, lap_s3_path)
                self.upload_parquet_to_s3(telemetry_df, telemetry_s3_path)
                self.upload_parquet_to_s3(weather_df, weather_s3_path)
                self.upload_parquet_to_s3(track_status_df, track_status_s3_path)

    def upload_parquet_to_s3(self, df, s3_path):
        """Util function, takes in the df from the predictor and converts it to parquet format and then upload to S3"""
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        self.s3_client.put_object(Bucket=self.bucket, Key=s3_path, Body=parquet_buffer.getvalue())

    def fetch_latest_race_data(self):
        """Fetch the data for latest date"""
        # Get the current year
        year = datetime.datetime.now().year
        schedule = fastf1.get_event_schedule(year)

        # Get the last race that has occurred
        now = datetime.datetime.now()
        last_race = None
        for _, row in schedule.iterrows():
            race_date = pd.to_datetime(row['Session5DateUtc'])
            if race_date < now:
                last_race = row

        if last_race is not None and last_race['F1ApiSupport']:
            race_name = last_race['EventName']
            session_types = ['FP1', 'FP2', 'FP3', 'Q', 'R']
            self.fetch_and_upload_race_data(year, race_name, session_types)

    def initial_load(self, start_year, end_year):
        for year in range(start_year, end_year + 1):
            schedule = fastf1.get_event_schedule(year)
            for _, row in schedule.iterrows():
                race_name = row['EventName']
                if row['F1ApiSupport']:  # Ensure the API supports the event
                    session_types = ['FP1', 'FP2', 'FP3', 'Q', 'R']
                    self.fetch_and_upload_race_data(year, race_name, session_types)

def data_ingestion_lambda_handler(event, context):
    bucket_name = 'your-s3-bucket'
    prefix = 'f1_data'

    f1_ingestion = F1DataIngestion(bucket_name, prefix)
    f1_ingestion.fetch_latest_race_data()

    # Mark the event as processed in DynamoDB
    event_date = datetime.datetime.now() - datetime.timedelta(days=1)
    dynamodb = boto3.client('dynamodb')
    dynamodb.update_item(
        TableName='F1EventsSchedule',
        Key={'EventDate': {'S': event_date.strftime('%Y-%m-%dT%H:%M:%SZ')}},
        UpdateExpression='SET Processed = :p',
        ExpressionAttributeValues={':p': {'BOOL': True}}
    )

    return {
        'statusCode': 200,
        'body': 'Latest race data ingested successfully'
    }