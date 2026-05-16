# Outbox Pattern Progress

This document captures what has been implemented so far for the outbox pattern in this Kafka learning project.

## Why I Added It

Before this change, `order-service` created an order in Redis and then directly published `OrderCreated` to Kafka.

That has a failure gap:

```txt
1. Save order in Redis
2. Publish event to Kafka
```

If Kafka is down, or the service crashes between those two steps, the order may exist but no event is sent. Then `inventory-service` never sees the order.

The outbox pattern fixes that by saving the order and the event together first.

## Current Flow

```txt
Client
  |
  | POST /orders
  v
order-service
  |
  | Redis transaction
  | - order:{order_id}
  | - outbox:{event_id}
  v
Redis
  |
  | outbox-service scans outbox:* keys
  v
Kafka: orders.created
  |
  v
inventory-service
```

## What I Implemented

### 1. Order Write Uses Redis Transaction

`order-service` now writes two things in one Redis transaction:

```txt
order:{order_id}
outbox:{event_id}
```

The outbox entry stores:

```txt
topic -> orders.created
key   -> {order_id}
value -> serialized Event JSON
```

This means `POST /orders` can succeed even if Kafka is down, as long as Redis is available.

### 2. Separate Outbox Service

Added a new service:

```txt
services/outbox_services/main.py
```

It runs a background loop that:

1. Scans Redis for `outbox:*` keys.
2. Reads the saved event.
3. Publishes it to Kafka.
4. Deletes the outbox key only after Kafka publish succeeds.

The service exposes:

```txt
GET http://localhost:8004/health
```

### 3. Docker Compose Service

Added `outbox-service` to `docker-compose.yml`.

It runs on port:

```txt
8004
```

## Redis Key Note

There is no single Redis hash named `outbox`.

Each event gets its own Redis key:

```txt
outbox:{event_id}
```

To list pending outbox entries:

```bash
docker exec kafka-learning-redis redis-cli --scan --pattern 'outbox:*'
```

To inspect one entry:

```bash
docker exec kafka-learning-redis redis-cli HGETALL outbox:{event_id}
```

## Manual Test Result

We manually tested the Kafka-down case:

1. Started the stack.
2. Stopped Kafka.
3. Created an order with `POST /orders`.
4. Confirmed an `outbox:{event_id}` entry existed in Redis.
5. Restarted Kafka.
6. Confirmed the outbox entry was removed.
7. Confirmed `inventory-service` received the event.

Result:

```txt
Outbox pattern worked for the Kafka outage case.
```

The test order was rejected because `product-1` had no stock, which proves the event reached inventory and was processed.

## Manual Test Commands

Use this test to prove that order creation still works while Kafka is down, and that the saved outbox event is published after Kafka comes back.

Start the stack:

```bash
docker compose up -d --build
```

Check that the services are reachable:

```bash
curl http://localhost:8001/docs
curl http://localhost:8004/health
```

Stop Kafka only:

```bash
docker compose stop kafka
```

Create an order while Kafka is down:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_email": "test@test.com", "product_id": "product-1", "quantity": 1}'
```

Expected result:

```json
{
  "order_id": "b62be7d0-04d7-4193-ac4e-6c179df8c737",
  "message": "Order created successfully"
}
```

Check pending outbox entries:

```bash
docker exec kafka-learning-redis redis-cli --scan --pattern 'outbox:*'
```

Expected result:

```txt
outbox:f3265eab-23d5-4b99-b90d-ea5e6604aff8
```

Inspect the outbox entry:

```bash
docker exec kafka-learning-redis redis-cli HGETALL outbox:f3265eab-23d5-4b99-b90d-ea5e6604aff8
```

Expected fields:

```txt
value
{"event_id":"f3265eab-23d5-4b99-b90d-ea5e6604aff8","event_type":"OrderCreated","order_id":"b62be7d0-04d7-4193-ac4e-6c179df8c737",...}
topic
orders.created
key
b62be7d0-04d7-4193-ac4e-6c179df8c737
```

Restart Kafka:

```bash
docker compose start kafka
```

Wait a few seconds:

```bash
sleep 5
```

Check the outbox again:

```bash
docker exec kafka-learning-redis redis-cli --scan --pattern 'outbox:*'
```

Expected result:

```txt
no output
```

Check the outbox service counters:

```bash
curl http://localhost:8004/health
```

Expected result:

```json
{
  "status": "ok",
  "published_count": 1,
  "failed_count": 1,
  "poll_interval_seconds": 0.1
}
```

`failed_count` can increase while Kafka is down. The important part is that `published_count` increases after Kafka restarts and the `outbox:*` key disappears.

Check inventory:

```bash
curl http://localhost:8002/inventory
```

If `product-1` has no stock, this can return:

```json
{}
```

Check the order status:

```bash
curl http://localhost:8001/orders/b62be7d0-04d7-4193-ac4e-6c179df8c737
```

Expected result when there is no stock:

```json
{
  "order_id": "b62be7d0-04d7-4193-ac4e-6c179df8c737",
  "customer_email": "test@test.com",
  "product_id": "product-1",
  "quantity": "1",
  "status": "REJECTED",
  "reason": "OUT_OF_STOCK"
}
```

This proves the event reached `inventory-service` and was processed.

## Current Status

Done:

- Atomic order + outbox write in `order-service`.
- Separate `outbox-service`.
- Redis scan and Kafka publish relay.
- Delete outbox entry only after successful publish.
- Manual Kafka-down test passed.

Still to do:

- Unit test: order creation writes outbox correctly.
- Unit test: relay publishes and clears outbox.
- Automated chaos test: Kafka down.
- Integration test: full end-to-end order flow.
- Optional cleanup: remove unused `OUTBOX_PREFIX_CONSTANT`.

## Recommended Test Plan

```txt
1. Manual docker test      done
2. Unit: outbox write      next
3. Unit: relay publishes   next
4. Chaos: Kafka down       automate later
5. Integration: e2e flow   final confidence test
```
