from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models import Product, Order
from app.schemas import OrderCreate, OrderResponse
from app.kafka_producer import send_event

router = APIRouter(prefix="/orders", tags=["Orders"])

@router.post("", response_model=OrderResponse)
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    if order.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be greater than 0")

    product = db.query(Product).filter(Product.id == order.product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    total_amount = product.price * order.quantity
    
    db_order = Order(
        product_id=order.product_id,
        quantity=order.quantity,
        balance=order.balance,
        total_amount=total_amount,
        status="PENDING"
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)

    event = {
        "order_id": db_order.id,
        "product_id": db_order.product_id,
        "quantity": db_order.quantity,
        "balance": db_order.balance,
        "total_amount": db_order.total_amount
    }
    send_event("order.created", event)

    return db_order

@router.get("/{order_id}", response_model=OrderResponse)
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order