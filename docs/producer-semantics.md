# Producer Semantics — At-Most-Once, At-Least-Once, Exactly-Once

This document explains the three Kafka producer delivery semantics and how to test them in this project.

## The Three Semantics

### At-Most-Once (`acks=0`)

The producer sends the message and does **not wait** for a broker acknowledgement. If the broker is down or the message fails, it is **lost permanently** — no retry.

```
Producer                          Broker
   |                                |
   |--- send() ------------------->|   (fire-and-forget, no ack)
   |                                |
```

**Use when:** loss is acceptable but latency matters (metrics, heartbeat, audit logs).

**Consumer behaviour:** `enable_auto_commit=True`, `auto_offset_reset="latest"`, no retry on handler failure. If the consumer crashes after reading but before committing, the message is skipped.

---

### At-Least-Once (`acks=all`, retries > 0)

The producer waits for acknowledgement from **all in-sync replicas**. If the ack is not received (network timeout, broker failure), the producer **retries**. The same message may be delivered **multiple times**.

```
Producer                          Broker
   |                                |
   |--- send() ------------------->|
   |<-- ack from all ISRs ---------|
   |                                |
   | (if ack lost, retry)          |
   |--- send() ------------------->|
   |<-- ack from all ISRs ---------|
   |                                |
```

**Use when:** data loss is unacceptable but duplicates can be handled downstream (logs, events, CDC).

**Consumer behaviour:** `enable_auto_commit=False`, offset committed only after handler succeeds. Failed messages are retried (up to `max_retries`), then sent to a DLQ (`<topic>.dlq`).

---

### Exactly-Once (idempotent producer, `enable.idempotence=True`)

The producer assigns a unique **producer ID (PID)** and **sequence number** to each message. The broker deduplicates by `(PID, partition, sequence_number)`. Even if the producer retries, the broker recognises the duplicate and ignores it.

```
Producer                          Broker
   |                                |
   |--- send(seq=1) ------------>  |   stored
   |<-- ack ----------------------|
   |                                |
   | (ack lost, retry same msg)    |
   |--- send(seq=1) ------------>  |   deduped (ignored)
   |<-- ack (from memory) --------|
   |                                |
```

**Use when:** duplicates cause real harm — payments, billing, inventory, order processing.

**Consumer behaviour:** same as at-least-once (manual commit, retry, DLQ) — the consumer must still be idempotent.

---

## Implementation

All code lives in two files:

| File | Purpose |
|------|---------|
| `services/shared/semantics.py` | Producer factories + consumer loops for each semantics |
| `scripts/semantics_demo.py` | Runnable demo that exercises all three |

### Producer Factories (`semantics.py`)

```
create_producer_at_most_once()     → AIOKafkaProducer(acks=0)
create_producer_at_least_once()    → AIOKafkaProducer(acks="all", retry_backoff_ms=500)
create_producer_idempotent()       → AIOKafkaProducer(enable_idempotence=True)
create_producer_transactional(id)  → AIOKafkaProducer(enable_idempotence=True, transactional_id=id)
```

### Consumer Loops (`semantics.py`)

```
consume_at_most_once(topic, group_id, handler)
    auto-commit, no retries, "latest" offset reset

consume_at_least_once(topic, group_id, handler, max_retries=3)
    manual commit, retry + DLQ on failure, "earliest" offset reset
```

---

## How to Test

### Prerequisites

```bash
# Start all services (includes Kafka)
docker compose up -d

# Activate Python virtual environment
source venv/bin/activate
```

### Run Full Demo

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 PYTHONPATH=. python scripts/semantics_demo.py
```

This runs all three demos sequentially. Each demo:
1. Creates a topic (`demo.semantics.{amo,alo,eo}`)
2. Publishes 3 events
3. Consumes them back

### Expected Output

**At-most-once:**
```
AT-MOST-ONCE  (acks=0, fire-and-forget, no retry)
sent (fire-and-forget) order_id=amo-0
sent (fire-and-forget) order_id=amo-1
sent (fire-and-forget) order_id=amo-2
handled event=demo.amo order_id=amo-0
handled event=demo.amo order_id=amo-2
handled event=demo.amo order_id=amo-1
```

If the consumer joins after the producer sends, those messages are lost (consumers starts at `latest` offset).

**At-least-once (with `fail_once_handler`):**
```
AT-LEAST-ONCE (acks=all, retry+DLQ on failure)
sent (acked) order_id=alo-0
sent (acked) order_id=alo-1
sent (acked) order_id=alo-2
simulating failure for order_id=alo-1    ← first attempt fails
simulating failure for order_id=alo-0    ← first attempt fails
simulating failure for order_id=alo-2    ← first attempt fails
succeeded on retry for order_id=alo-1    ← second attempt succeeds
succeeded on retry for order_id=alo-0
succeeded on retry for order_id=alo-2
```

Each message is processed twice — fail on 1st attempt, succeed on 2nd retry. This demonstrates the retry mechanism. Each message was **delivered at least once**.

**Exactly-once (idempotent):**
```
EXACTLY-ONCE  (enable.idempotence=True, broker dedupes on retry)
sent order_id=eo-0
sent order_id=eo-1
sent order_id=eo-2
handled event=demo.eo order_id=eo-1
handled event=demo.eo order_id=eo-2
handled event=demo.eo order_id=eo-0
```

Exactly 3 messages consumed — no duplicates. If the producer had retried at the network level, the broker would have deduplicated the retry.

---

## Key Config Details

| | At-most-once | At-least-once | Exactly-once |
|---|---|---|---|
| `acks` | `0` | `"all"` | `"all"` (auto) |
| `enable.idempotence` | `false` | `false` | `true` |
| `enable.auto.commit` | `true` | `false` | `false` |
| `auto.offset.reset` | `latest` | `earliest` | `earliest` |
| Retry on handler failure | No | Yes (3x + DLQ) | Yes (3x + DLQ) |

---

## Files Reference

- `services/shared/semantics.py` — Producer factories + consumer implementations with backpressure, circuit breaker, metrics, timeout, and transactional consumer
- `scripts/semantics_demo.py` — Demo script
- `services/shared/events.py` — `Event` model
- `services/shared/config.py` — `KAFKA_BOOTSTRAP_SERVERS`


## Enhanced Features

### 1. Backpressure (`Semaphore`)

All consumer loops accept `max_concurrent: int = 10`. An `asyncio.Semaphore` limits how many handler invocations run simultaneously, preventing resource exhaustion when a topic has many partitions.

```python
async def consume_at_least_once(
    ...,
    max_concurrent: int = 10,
):
    semaphore = asyncio.Semaphore(max_concurrent)
    async with semaphore:
        await handler(event)
```

### 2. Circuit Breaker (`DLQ` producer)

`CircuitBreaker` tracks consecutive failures when sending to the DLQ. After `failure_threshold` (default 5) consecutive failures, it **opens** and stops sending to the DLQ for `recovery_timeout` (default 30s). After the timeout, it transitions to **half-open** and allows a single probe.

```
State machine:
  CLOSED ──(threshold failures)──► OPEN ──(timeout)──► HALF_OPEN ──(success)──► CLOSED
                                  │                                              │
                                  └──(failure)──► OPEN (reset timer)             │
                                              └──(failure)──► OPEN               │
```

### 3. Metrics (`ConsumerMetrics`)

`ConsumerMetrics` records per-consumer stats:
- `processed` — successful handler invocations
- `failed` — handler exceptions / timeouts
- `dlq_sent` — messages routed to dead-letter queue
- `total_time` — cumulative processing time
- `avg_time` — average processing time per message
- `uptime` — wall-clock time since consumer started

Logged as:
```
metrics topic=... processed=3 failed=3 dlq=0 avg_ms=0.0 uptime=12.0s
```

### 4. Max Processing Time (`asyncio.wait_for`)

Consumers accept `max_processing_time: float | None`. When set, the handler is wrapped in `asyncio.wait_for()`. If the handler hangs (e.g., deadlock, slow DB), the coroutine is cancelled after the timeout.

```python
coro = handler(event)
if max_processing_time is not None:
    coro = asyncio.wait_for(coro, timeout=max_processing_time)
await coro
```

This prevents **stuck handlers** from blocking partition processing indefinitely.

### 5. Transactional Consumer (`consume_transform_produce`)

Reads from `source_topic`, transforms via `processor`, and writes to `target_topic` — all **atomically** within a single Kafka transaction.

```
Consumer                          Producer (transactional)
   |                                   |
   | msg (offset 5)                    |
   |                                   |
   | begin_transaction()               |
   | send(target, result)              |
   | send_offsets_to_transaction()     |
   | commit_transaction()              |
   |                                   |
   | → Both the produced message and   |
   |   the consumer offset commit are  |
   |   committed together, or neither  |
   |   is (atomic).                    |
```

**Key details:**
- Consumer uses `isolation_level="read_committed"` — only reads committed transactions
- `send_offsets_to_transaction()` includes the consumer offset in the transaction
- If processing or producing fails, `abort_transaction()` rolls back everything
- Processes messages **sequentially** (transactions are per-producer, no concurrency)

### Demo Output (transactional)

```
TRANSACTIONAL  (consume-transform-produce)
sent source order_id=txn-0
sent source order_id=txn-1
sent source order_id=txn-2
...
metrics topic=demo.semantics.txn.source→demo.semantics.txn.target processed=3 failed=0
```

3 source messages → processed (uppercased) → 3 target messages committed atomically. Zero failures.


## Run All Enhanced Demos

```bash
source venv/bin/activate
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 PYTHONPATH=. python scripts/semantics_demo.py
```

This runs 4 demos consecutively:
1. **At-most-once** — with `max_concurrent=2`, `max_processing_time=3s`, `ConsumerMetrics`
2. **At-least-once** — with `CircuitBreaker(failure_threshold=5)`, retry + DLQ
3. **Idempotent** — exactly-once broker-side deduplication
4. **Transactional** — atomic consume-transform-produce
