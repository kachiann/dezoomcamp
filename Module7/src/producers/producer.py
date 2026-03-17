import json
import time
import pandas as pd
from kafka import KafkaProducer

# Kafka config
KAFKA_SERVER = "localhost:9092"
TOPIC = "green-trips"

# Load parquet file
# Load data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
df = pd.read_parquet(url)

# Select required columns
columns = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]
df = df[columns]

# Convert datetime to string (IMPORTANT)
df["lpep_pickup_datetime"] = df["lpep_pickup_datetime"].astype(str)
df["lpep_dropoff_datetime"] = df["lpep_dropoff_datetime"].astype(str)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Sending data to Kafka...")

t0 = time.time()

# Send all rows
for record in df.to_dict(orient="records"):
    producer.send(TOPIC, value=record)

producer.flush()

t1 = time.time()

print(f"Finished sending {len(df)} records")
print(f"Took {(t1 - t0):.2f} seconds")