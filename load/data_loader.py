import io
import fastf1
import boto3
import pandas as pd
import datetime


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

