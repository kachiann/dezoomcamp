import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden, Conflict
import time

BUCKET_NAME = "kachi_dezoomcamp_hw4_2026"

# Uses GOOGLE_APPLICATION_CREDENTIALS automatically
client = storage.Client()

# DataTalksClub NYC TLC Data repository (GitHub Releases)
# Pattern:
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/<taxi>/<taxi>_tripdata_<year>-<month>.csv.gz
BASE_URL_TMPL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    "{taxi}/{taxi}_tripdata_{year}-{month}.csv.gz"
)

TAXIS = ["yellow", "green"]
YEARS = [2019, 2020]
MONTHS = [f"{i:02d}" for i in range(1, 13)]

DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)

def iter_tasks():
    """Yield (taxi, year, month) tuples for all required files."""
    for taxi in TAXIS:
        for year in YEARS:
            for month in MONTHS:
                yield taxi, year, month

def download_file(task):
    taxi, year, month = task
    url = BASE_URL_TMPL.format(taxi=taxi, year=year, month=month)

    filename = f"{taxi}_tripdata_{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {url} ...")
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

def upload_to_gcs(file_path, max_retries=3, delete_local=True):
    base = os.path.basename(file_path)  # yellow_tripdata_2019-01.csv.gz
    taxi = base.split("_", 1)[0]        
    year = base.split("_tripdata_", 1)[1].split("-", 1)[0]
    blob_name = f"{taxi}/{year}/{base}"

    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to gs://{BUCKET_NAME}/{blob_name} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")

                # 🔥 SAFE DELETE
                if delete_local and os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Deleted local file: {file_path}")

                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")

        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    tasks = list(iter_tasks())

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, tasks))

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(upload_to_gcs, filter(None, file_paths)))

    print("All files processed and verified.")
