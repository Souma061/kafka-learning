# Saga Rollback Manual Test

This document describes the manual test process for the timeout-based saga rollback implementation.

## What We Added

The saga rollback flow has one new service:

```txt
saga-service
```

Its job is to detect orders stuck in `PENDING` for too long.

Current behavior:

```txt
1. order-service creates order with status=PENDING and created_at timestamp
2. saga-service scans order:* keys
3. if an order is still PENDING after SAGA_TIMEOUT_SECONDS
4. saga-service writes a SagaTimeout event to the outbox
5. outbox-service publishes saga.timeout to Kafka
6. order-service consumes saga.timeout and marks the order CANCELLED
7. order-service writes OrderCancelled to the outbox
8. outbox-service publishes orders.cancelled to Kafka
9. email-service consumes orders.cancelled
10. inventory-service consumes saga.timeout and releases any reservation if one exists
```

Important design choice:

```txt
saga-service does not directly update order status.
```

It only emits the timeout event. `order-service` owns order state changes.

## Topics

```txt
saga.timeout
orders.cancelled
```

## Redis Keys

Saga-related fields are stored on the existing order hash:

```txt
order:{order_id}
  status=PENDING | CONFIRMED | REJECTED | CANCELLED
  created_at={iso datetime}
  saga_timeout_published=1
  saga_timeout_at={iso datetime}
```

Timeout and cancellation events are still stored through the outbox:

```txt
outbox:{event_id}
```

## Manual Test 1: Pending Order Times Out

This test proves that an order stuck in `PENDING` becomes `CANCELLED`.

Use a short timeout for local testing:

```bash
SAGA_TIMEOUT_SECONDS=5 SAGA_MONITOR_INTERVAL_SECONDS=1 docker compose up -d --build
```

If services race Kafka during startup, restart the app services after Kafka is ready:

```bash
docker compose up -d order-service inventory-service outbox-service saga-service
```

Check health:

```bash
curl http://localhost:8001/docs
curl http://localhost:8004/health
curl http://localhost:8005/health
```

Stop Kafka so the order stays `PENDING` and cannot reach inventory:

```bash
docker compose stop kafka
```

Create an order:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_email": "timeout@test.com", "product_id": "product-1", "quantity": 1}'
```

Save the returned `order_id`.

Confirm the order is pending:

```bash
curl http://localhost:8001/orders/{order_id}
```

Expected status:

```json
{
  "status": "PENDING"
}
```

Wait longer than the timeout:

```bash
sleep 8
```

Check saga-service:

```bash
curl http://localhost:8005/health
```

Expected:

```txt
timeouts_published should be at least 1
```

Check outbox while Kafka is still down:

```bash
docker exec kafka-learning-redis redis-cli --scan --pattern 'outbox:*'
```

Expected:

```txt
At least one outbox:{event_id} key should exist.
```

Restart Kafka:

```bash
docker compose start kafka
```

Wait for the outbox relay and consumers:

```bash
sleep 8
```

Check the outbox again:

```bash
docker exec kafka-learning-redis redis-cli --scan --pattern 'outbox:*'
```

Expected:

```txt
no output
```

Check the order:

```bash
curl http://localhost:8001/orders/{order_id}
```

Expected final status:

```json
{
  "status": "CANCELLED",
  "reason": "SAGA_TIMEOUT"
}
```

This proves:

```txt
PENDING order -> saga.timeout -> OrderCancelled -> CANCELLED
```

## Manual Test 2: Normal Order Should Not Be Cancelled

This test proves that the saga monitor does not cancel an order that finishes before the timeout.

Start Kafka:

```bash
docker compose start kafka
```

Seed stock for `product-1`:

```bash
docker exec kafka-learning-redis redis-cli HSET inventory product-1 10
```

Create an order:

```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_email": "success@test.com", "product_id": "product-1", "quantity": 1}'
```

Wait for normal processing:

```bash
sleep 5
```

Check the order:

```bash
curl http://localhost:8001/orders/{order_id}
```

Expected:

```json
{
  "status": "CONFIRMED"
}
```

Wait longer than the saga timeout:

```bash
sleep 8
```

Check the order again:

```bash
curl http://localhost:8001/orders/{order_id}
```

Expected:

```json
{
  "status": "CONFIRMED"
}
```

This proves the saga monitor only targets orders that are still `PENDING`.

## Useful Debug Commands

List pending outbox entries:

```bash
docker exec kafka-learning-redis redis-cli --scan --pattern 'outbox:*'
```

List order keys:

```bash
docker exec kafka-learning-redis redis-cli --scan --pattern 'order:*'
```

Inspect one order:

```bash
docker exec kafka-learning-redis redis-cli HGETALL order:{order_id}
```

View service logs:

```bash
docker compose logs --tail=120 saga-service
docker compose logs --tail=120 outbox-service
docker compose logs --tail=120 order-service
docker compose logs --tail=120 inventory-service
docker compose logs --tail=120 email-service
```

## Expected Caveats

If `failed_count` increases in `outbox-service`, that is normal while Kafka is down.

The important thing is:

```txt
outbox entries remain while Kafka is down
outbox entries disappear after Kafka comes back
the order eventually becomes CANCELLED
```
