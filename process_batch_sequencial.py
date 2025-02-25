import json
import time
import concurrent.futures
from google.cloud import bigquery, storage
from datetime import datetime

# Initialize clients
bq_client = bigquery.Client()
gcs_client = storage.Client()


# Configuration
PROJECT_ID = "sample-poc-452011"
DATASET_ID = "test_dataset"
TABLE_ID = "dummy_users"
BQ_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
BUCKET_NAME = "sample-poc-452011"
BLOB_PREFIX = "bq_exports/"
BATCH_SIZE = 50
JSON_FILENAME_TEMPLATE = "batch_{batch_num}.json"

# Function to create bucket if it doesn't exist
def create_bucket_if_not_exists(bucket_name):
    bucket = gcs_client.bucket(bucket_name)
    if not bucket.exists():
        bucket.create(location="US")
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

# Custom JSON serializer
def custom_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Converts datetime to "YYYY-MM-DDTHH:MM:SS" format
    raise TypeError(f"Type {type(obj)} not serializable")

# Function to fetch data from BigQuery in batches
def fetch_data_in_batches():
    query = f"SELECT * FROM `{BQ_TABLE}`"
    job = bq_client.query(query)
    iterator = job.result(page_size=BATCH_SIZE)

    total_batches = 0
    overall_start_time = time.time()

    for batch_num, page in enumerate(iterator.pages):
        batch_start_time = time.time()
        
        # Convert to JSON
        records = [dict(row) for row in page]
        json_data = json.dumps(records, indent=2, default=custom_serializer)  # ✅ Fixed

        # Save JSON to a local file
        filename = JSON_FILENAME_TEMPLATE.format(batch_num=batch_num)
        with open(filename, "w") as f:
            f.write(json_data)

        # Upload to GCS
        upload_to_gcs(BUCKET_NAME, filename, BLOB_PREFIX + filename)

        batch_end_time = time.time()
        print(f"Batch {batch_num + 1} processed in {batch_end_time - batch_start_time:.2f} seconds")
        total_batches += 1

    overall_end_time = time.time()
    print(f"Total {total_batches} batches processed in {overall_end_time - overall_start_time:.2f} seconds")

# Function to upload a file to GCS
def upload_to_gcs(bucket_name, local_file, blob_name):
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file)
    print(f"Uploaded {local_file} to gs://{bucket_name}/{blob_name}")

# Main execution
if __name__ == "__main__":
    create_bucket_if_not_exists(BUCKET_NAME)
    fetch_data_in_batches()

