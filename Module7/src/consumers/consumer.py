import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="q3-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

count = 0
total = 0
MAX_RECORDS = 49416  # total number of records in the dataset

for message in consumer:
    trip = message.value
    total += 1
    if float(trip["trip_distance"]) > 5:
        count += 1

    # stop after reading all messages
    if total >= MAX_RECORDS:
        break

print("Trips > 5km:", count)