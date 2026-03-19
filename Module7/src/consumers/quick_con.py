from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

for i, msg in enumerate(consumer):
    if i < 5:
        print(msg.value)
    else:
        break