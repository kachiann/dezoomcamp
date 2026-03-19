import pandas as pd
import json
from kafka import KafkaProducer
from time import time

# 1. Read parquet and keep only needed columns
df = pd.read_parquet("/workspaces/dezoomcamp/Module7/data/green_tripdata_2025-10.parquet")
df = df[[
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount"
]]

# 2. Convert datetime to string
df["lpep_pickup_datetime"] = df["lpep_pickup_datetime"].astype(str)
df["lpep_dropoff_datetime"] = df["lpep_dropoff_datetime"].astype(str)

# 3. Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 4. Send all rows
t0 = time()
for record in df.to_dict(orient="records"):
    producer.send("green-trips", record)

producer.flush()
t1 = time()
print(f"Sent {len(df)} records in {t1 - t0:.2f} seconds")