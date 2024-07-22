import io
import fastf1
import boto3
import pandas as pd


class DataIngestion:

    def __init__(self, bucket, prefix):
        self.s3_client = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix

    def fetch_and_upload_race(self, year, race_name, session_types):
        for session_type in session_types:
            f1_session = fastf1.get_session(year, race_name, session_type)
            f1_session.load()

            # Driver information
            drivers = f1_session.drivers
            dummy_df = []

            for driver in drivers:
                df = pd.DataFrame(session.get_driver(driver)).T
                dummy_df.append(df)

            driver_df = pd.concat(dummy_df, ignore_index=True)

            # Individual lap information
            laps_df = f1_session.laps

    def initial_load(self, start_year, end_year):
        for year in range(start_year, end_year + 1):
            f1_schedule = fastf1.get_event_schedule(year)
            for _, row in f1_schedule.iterrows():
                race_name = row['EventName']

                if row['F1ApiSupport']:
                    session_types = ['FP1', 'FP2', 'FP3', 'Q', 'R']
                    self.fetch_and_upload_race(year, race_name, session_types)


if __name__ == '__main__':
    session = fastf1.get_session(2024, 'Silverstone', 'Race')
    session.load()
    lap_df = session.laps
    lap_df.to_parquet('laps.parquet', index=False, engine='fastparquet')
