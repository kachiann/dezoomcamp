import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",   # VERY IMPORTANT
    enable_auto_commit=True,
    group_id="q3-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

count = 0
total = 0

for message in consumer:
    trip = message.value
    total += 1

    if float(trip["trip_distance"]) > 5:
        count += 1

    # stop after reading all records (important!)
    if total >= 500000:   # approx dataset size
        break

print("Trips > 5km:", count)