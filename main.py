from load.LoadEventSchedule import load_event_schedule_to_dynamodb
from load.data_loader import DataIngestion

def main():

    bucket_name = 'race-predictor-pro'
    prefix = 'f1_data'
    f1_data_ingestion = DataIngestion(bucket_name, prefix)

    start_year = 2022
    end_year = 2024

    #f1_data_ingestion.initial_load(start_year, end_year)

    load_event_schedule_to_dynamodb(start_year, end_year)

if __name__ == "__main__":
    main()