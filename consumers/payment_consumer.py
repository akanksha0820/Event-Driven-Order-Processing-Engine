import json
from kafka import KafkaConsumer
from app.kafka_producer import send_event
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Order

consumer = KafkaConsumer(
    "order.created",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="payment-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("[PAYMENT CONSUMER] Listening for order.created events...")

def update_balance(event: dict):
    db: Session = SessionLocal()
    try:
        order = db.query(Order).filter(Order.id == event["order_id"]).first()
        if order:
            order.balance -= order.total_amount
            db.commit()
            print(f"[ORDER BALANCE UPDATE] Balance updated for Order {order.id}")
    finally:
        db.close()

for message in consumer:
    event = message.value
    print(f"[PAYMENT CONSUMER] Received: {event}")

    try:
        payment_success = True

        if (event["balance"] < event["total_amount"]):
            payment_success = False

        if payment_success:
            update_balance(event)
            
            send_event("payment.processed", {
                "order_id": event["order_id"],
                "product_id": event["product_id"],
                "quantity": event["quantity"]
            })
        else:
            send_event("order.failed", {
                "order_id": event["order_id"],
                "reason": "Payment failed"
            })
    except Exception as e:
        print(f"[PAYMENT CONSUMER] Error: {e}")