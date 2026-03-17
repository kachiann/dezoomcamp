import pandas as pd
from kafka import KafkaProducer
import json
from time import time


# Load data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID','passenger_count', 'trip_distance', 'total_amount', 'tip_amount']
df = pd.read_parquet(url, columns=columns)
df.head()

df["lpep_pickup_datetime"] = df["lpep_pickup_datetime"].astype(str)
df["lpep_dropoff_datetime"] = df["lpep_dropoff_datetime"].astype(str)

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send data to Kafka topic  
# Send data
t0 = time()

for record in df.to_dict(orient="records"):
    producer.send("green-trips", value=record)

producer.flush()

t1 = time()
print(f"took {(t1 - t0):.2f} seconds")

