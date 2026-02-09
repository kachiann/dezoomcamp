import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden, Conflict
import time

BUCKET_NAME = "kachi_dezoomcamp_hw3_2026"

# Uses GOOGLE_APPLICATION_CREDENTIALS automatically
client = storage.Client()

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def create_bucket(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' already exists. Proceeding...")
        return
    except NotFound:
        try:
            client.create_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'")
        except Conflict:
            print(f"Bucket name '{bucket_name}' is already taken globally. Pick a different name.")
            sys.exit(1)
        except Forbidden:
            print(f"No permission to create bucket '{bucket_name}'. Check IAM roles.")
            sys.exit(1)

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(upload_to_gcs, filter(None, file_paths)))

    print("All files processed and verified.")