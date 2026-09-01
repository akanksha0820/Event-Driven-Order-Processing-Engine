import json
from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Order

success_consumer = KafkaConsumer(
    "inventory.updated",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="order-status-success-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

failed_consumer = KafkaConsumer(
    "order.failed",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="order-status-failed-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("[ORDER STATUS CONSUMER] Listening for inventory.updated and order.failed events...")

def mark_completed(event: dict):
    db: Session = SessionLocal()
    try:
        order = db.query(Order).filter(Order.id == event["order_id"]).first()
        if order:
            order.status = "COMPLETED"
            db.commit()
            print(f"[ORDER STATUS CONSUMER] Order {order.id} marked COMPLETED")
    finally:
        db.close()

def mark_failed(event: dict):
    db: Session = SessionLocal()
    try:
        order = db.query(Order).filter(Order.id == event["order_id"]).first()
        if order:
            order.status = "FAILED"
            db.commit()
            print(f"[ORDER STATUS CONSUMER] Order {order.id} marked FAILED")
    finally:
        db.close()

import threading

def consume_success():
    for message in success_consumer:
        event = message.value
        print(f"[ORDER STATUS CONSUMER] Success event: {event}")
        mark_completed(event)

def consume_failure():
    for message in failed_consumer:
        event = message.value
        print(f"[ORDER STATUS CONSUMER] Failure event: {event}")
        mark_failed(event)

thread1 = threading.Thread(target=consume_success)
thread2 = threading.Thread(target=consume_failure)

thread1.start()
thread2.start()

thread1.join()
thread2.join()