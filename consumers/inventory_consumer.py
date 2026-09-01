import json
from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Product
from app.kafka_producer import send_event

consumer = KafkaConsumer(
    "payment.processed",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="inventory-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("[INVENTORY CONSUMER] Listening for payment.processed events...")

for message in consumer:
    event = message.value
    print(f"[INVENTORY CONSUMER] Received: {event}")

    db: Session = SessionLocal()
    try:
        product = db.query(Product).filter(Product.id == event["product_id"]).first()

        if not product:
            send_event("order.failed", {
                "order_id": event["order_id"],
                "reason": "Product not found during inventory check"
            })
            continue

        if product.stock < event["quantity"]:
            send_event("order.failed", {
                "order_id": event["order_id"],
                "reason": "Insufficient stock"
            })
            continue

        product.stock -= event["quantity"]
        db.commit()

        send_event("inventory.updated", {
            "order_id": event["order_id"]
        })

    except Exception as e:
        print(f"[INVENTORY CONSUMER] Error: {e}")
        db.rollback()
    finally:
        db.close()