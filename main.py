from load.LoadEventSchedule import load_event_schedule_to_dynamodb, create_dynamoDB_table, schedule_next_race_trigger
from load.data_loader import DataIngestion
from logger import Logger

logger = Logger.get_logger()

def main():

    bucket_name = 'race-predictor-pro'
    prefix = 'f1_data'
    f1_data_ingestion = DataIngestion(bucket_name, prefix)

    start_year = 2023
    end_year = 2024

    f1_data_ingestion.initial_load(start_year, end_year)
   # create_dynamoDB_table()
   # load_event_schedule_to_dynamodb(start_year, end_year)
   # schedule_next_race_trigger()

if __name__ == "__main__":
    main()