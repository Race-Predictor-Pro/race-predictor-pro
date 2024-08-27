import os
import boto3

s3_client = boto3.client('s3')
bucket = "race-predictor-pro"

def _fetch_data_from_s3(s3_directory: str, local_directory: str):
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        bucket = 'race-predictor-pro'  # Replace with your bucket name

        # Ensure local directory exists
        if not os.path.exists(local_directory):
            os.makedirs(local_directory)

        # List all objects in the specified S3 directory
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_directory)
        if 'Contents' not in response:
            print(f"No files found in S3 directory: {s3_directory}")
            return

        # Iterate over each file in the directory
        for obj in response['Contents']:
            s3_file_path = obj['Key']
            if s3_file_path.endswith('.parquet'):
                local_file_path = os.path.join(local_directory, os.path.basename(s3_file_path))

                # Download the parquet file from S3 directly to local
                s3_client.download_file(Bucket=bucket, Key=s3_file_path, Filename=local_file_path)
                print(f"Successfully downloaded {s3_file_path} to {local_file_path}")

    except Exception as e:
        print(f"Error downloading files from {s3_directory}: {e}")

if __name__ == "__main__":
    _fetch_data_from_s3('f1_data/2023/abu-dhabi-grand-prix/', 'data/')