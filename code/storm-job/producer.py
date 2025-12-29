import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

BOOTSTRAP_SERVERS = "49.52.27.49:9092"
TOPIC = "storm-test-topic"
TOTAL_MESSAGES = 30000

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    linger_ms=10
)

print("Start producing messages...")

for i in range(TOTAL_MESSAGES):
    msg = {
        "msg_id": i,
        "timestamp": datetime.utcnow().isoformat(),
        "content": {
            "sensor_id": f"sensor_{i % 10}",
            "value": random.random() * 100
        }
    }

    producer.send(TOPIC, value=msg)

    if i % 1000 == 0:
        print(f"Produced {i} messages")

    time.sleep(0.001)  # 控制速率，防止压垮 Storm

producer.flush()
producer.close()

print("Finished producing 30,000 messages.")
