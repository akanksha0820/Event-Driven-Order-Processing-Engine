# Event\_Driven\_Order\_Processing (using FastAPI and Kafka)

An **event-driven backend** built using **FastAPI and Apache Kafka** to facilate scalable communication among distributed producers and consumers.

\---

## Overview

This project simulates a real-world **order processing system** where:

* Users create products and place orders through FastAPI endpoints
* Orders are processed asynchronously using Kafka topics and multi-threading
* Separate consumers handle **payment, inventory, and order status**
* Microservices are **decoupled** using multiple Kafka topics

\---

## Architecture

```text
Client (Swagger / curl)
        ↓
FastAPI (Producer)
        ↓
Kafka (Message Queue)
        ↓
Consumers (Workers)
   ├── Payment Service
   ├── Inventory Service
   └── Order Status Service
        ↓
Database (SQLite)
```

\---

## Event Flow

1. User places an order → `POST /orders`
2. FastAPI:

   * Creates order with a default (`PENDING`) status
   * Publishes event to the `order.created` topic
3. Payment Consumer:

   * Processes payment
   * Publishes to `payment.processed` or `order.failed` topic
4. Inventory Consumer:

   * Updates stock
   * Publishes to `inventory.updated` or `order.failed` topic
5. Order Status Consumer:

   * Updates order status to `COMPLETED` or `FAILED`

\---

## Technologies used

* **Backend:** FastAPI
* **Messaging:** Apache Kafka
* **Database:** SQLite
* **ORM:** SQLAlchemy
* **Containerization:** Docker

\---

## Setup Instructions

### 1\. Clone the repository

```bash
git clone https://github.com/AnjanaNallanagula/E-Commerce\\\_Inventory\\\_Processing
cd ecommerce-kafka-mvp
```

\---

### 2\. Create virtual environment

```bash
python -m venv .venv
.\\\\.venv\\\\Scripts\\\\activate
```

\---

### 3\. Install dependencies

```bash
pip install -r requirements.txt
```

\---

### 4\. Start Kafka using Docker

```bash
docker compose up -d
```

\---

### 5\. Run FastAPI server

```bash
python -m uvicorn app.main:app --reload
```

Open Swagger UI:

```
http://127.0.0.1:8000/docs
```

\---

### 6\. Start consumers (separate terminals)

```bash
python -m consumers.payment\\\_consumer
```

```bash
python -m consumers.inventory\\\_consumer
```

```bash
python -m consumers.order\\\_status\\\_consumer
```

\---

## API Endpoints

### Products

* `POST /products` → Create product
* `GET /products` → List products
* `GET /products/{id}` → Get product

### Orders

* `POST /orders` → Create order
* `GET /orders/{id}` → Get order status

