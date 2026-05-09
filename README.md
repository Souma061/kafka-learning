# Kafka Learning: Order, Inventory, and Email Microservices

Backend-only learning project for understanding how Kafka handles real-life traffic in a microservice system.

The system models a small ecommerce flow with three FastAPI services:

- `order-service`: accepts customer orders and tracks order state
- `inventory-service`: reserves stock or rejects orders when stock is unavailable
- `email-service`: sends or records notification events after order confirmation/rejection

Kafka is used for asynchronous communication between services. Redis is used as the service data store. Kafka runs in KRaft mode, so there is no Zookeeper.

## Architecture

```txt
Client
  |
  | REST
  v
order-service
  |
  | Kafka: orders.created
  v
inventory-service
  |
  | Kafka: inventory.reserved / inventory.rejected
  v
order-service
  |
  | Kafka: orders.confirmed / orders.rejected
  v
email-service
```

## Tech Stack

- Python
- FastAPI
- Kafka with KRaft mode
- Redis
- aiokafka
- Docker Compose
- pytest

## Services

### Order Service

Responsible for:

- creating new orders
- storing order state in Redis
- publishing `OrderCreated` events
- consuming inventory result events
- publishing final order events

Expected endpoints:

```txt
POST /orders
GET  /orders
GET  /orders/{order_id}
```

### Inventory Service

Responsible for:

- storing product stock in Redis
- consuming `OrderCreated` events
- reserving stock atomically
- publishing `InventoryReserved` or `InventoryRejected`

Expected endpoints:

```txt
GET  /inventory
POST /inventory/adjust
```

### Email Service

Responsible for:

- consuming final order events
- sending or recording email notifications
- storing notification history in Redis

Expected endpoints:

```txt
GET /notifications
```

## Kafka Topics

```txt
orders.created
inventory.reserved
inventory.rejected
orders.confirmed
orders.rejected
notifications.email
orders.created.dlq
inventory.reserved.dlq
inventory.rejected.dlq
orders.confirmed.dlq
orders.rejected.dlq
```

## Event Flow

1. A client creates an order using `POST /orders`.
2. `order-service` stores the order as `PENDING`.
3. `order-service` publishes an `OrderCreated` event to Kafka.
4. `inventory-service` consumes the event.
5. If stock is available, inventory is reserved and `InventoryReserved` is published.
6. If stock is unavailable, `InventoryRejected` is published.
7. `order-service` consumes the inventory result.
8. The order becomes `CONFIRMED` or `REJECTED`.
9. `order-service` publishes `OrderConfirmed` or `OrderRejected`.
10. `email-service` consumes the final order event and records/sends a notification.

## Redis Key Pattern

Use one Redis instance with service-specific prefixes:

```txt
order:{order_id}
order:processed:{event_id}

inventory:stock
inventory:reservation:{order_id}
inventory:processed:{event_id}

email:notification:{notification_id}
email:processed:{event_id}
```

## Requirements

Recommended `requirements.txt`:

```txt
fastapi
uvicorn[standard]
pydantic[email]
pydantic-settings>=2.2.1
python-dotenv
python-multipart>=0.0.9

aiokafka>=0.13.0
redis>=5.0.0
httpx>=0.27.0

pytest>=8.2.0
pytest-asyncio>=0.23.6
fakeredis>=2.20

resend
```

## Running Locally

Start Kafka, Redis, and the services:

```bash
docker compose up --build
```

Service URLs:

```txt
order-service:     http://localhost:8001/docs
inventory-service: http://localhost:8002/docs
email-service:     http://localhost:8003/docs
```

## Example Requests

Create an order:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_email": "customer@example.com",
    "product_id": "product-1",
    "quantity": 2
  }'
```

List orders:

```bash
curl http://localhost:8001/orders
```

Check inventory:

```bash
curl http://localhost:8002/inventory
```

List notifications:

```bash
curl http://localhost:8003/notifications
```

## Load Testing

The goal of the load test is to create many concurrent orders and observe:

- Kafka partitioning
- consumer groups
- message lag
- duplicate-safe event handling
- Redis stock consistency
- order rejection when stock runs out

Example:

```bash
python scripts/load_test.py
```

## Kafka Concepts Practiced

- topics
- partitions
- event keys
- consumer groups
- retries
- dead-letter queues
- idempotency
- eventual consistency
- choreography-based saga flow
- backpressure under traffic

## Email Provider

For local learning, the email service can simply store notification records in Redis.

For real email sending, use Resend first because it has a simple API and fits this learning project well.

Environment variable:

```txt
RESEND_API_KEY=your_api_key_here
```

## Project Goal

This project is not only about making Kafka send messages. The goal is to understand what happens when real traffic enters a distributed backend:

- services do not update at the same time
- events can be retried
- consumers can fail
- duplicate messages can happen
- stock can run out under concurrency
- users need a final order state even when processing is async

That is the real Kafka learning surface.
