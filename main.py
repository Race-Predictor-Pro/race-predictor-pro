from load.LoadEventSchedule import load_event_schedule_to_dynamodb, schedule_next_race_trigger
from load.F1DataIngestion import F1DataIngestion

def main():

    bucket_name = 'your-s3-bucket'
    prefix = 'f1_data'
    f1_ingestion = F1DataIngestion(bucket_name, prefix)

    start_year = 2022
    end_year = 2024

    f1_ingestion.initial_load(start_year, end_year)

    load_event_schedule_to_dynamodb(start_year, end_year)

    schedule_next_race_trigger()


if __name__ == "__main__":
    main()