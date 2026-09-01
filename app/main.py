from fastapi import FastAPI
from app.database import Base, engine
from app.routes.products import router as products_router
from app.routes.orders import router as orders_router

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Event-Driven Order Processing Engine")

app.include_router(products_router)
app.include_router(orders_router)


@app.get("/")
def health_check():
    return {"message": "All the services are available and running"}