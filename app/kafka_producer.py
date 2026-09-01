import json
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def send_event(topic: str, message: dict):
    producer = get_producer()
    producer.send(topic, value=message)
    producer.flush()
    print(f"[PRODUCER] Sent to {topic}: {message}")