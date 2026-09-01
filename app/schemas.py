from pydantic import BaseModel

class ProductCreate(BaseModel):
    name: str
    price: float
    stock: int

class ProductResponse(BaseModel):
    id: int
    name: str
    price: float
    stock: int

    class Config:
        from_attributes = True

class OrderCreate(BaseModel):
    product_id: int
    quantity: int
    balance: float

class OrderResponse(BaseModel):
    id: int
    product_id: int
    quantity: int
    total_amount: float
    balance: float
    status: str

    class Config:
        from_attributes = True