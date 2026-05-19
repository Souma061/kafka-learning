import asyncio
import logging

from services.shared.events import Event, new_event
from services.shared.semantics import (
    ConsumerMetrics,
    CircuitBreaker,
    create_producer_at_least_once,
    create_producer_at_most_once,
    create_producer_idempotent,
    create_producer_transactional,
    consume_at_least_once,
    consume_at_most_once,
    consume_transform_produce,
    run_transaction,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("kafka-learning")

TOPIC = "demo.semantics"


async def echo_handler(event: Event) -> None:
    logger.info("handled event=%s order_id=%s", event.event_type, event.order_id)


async def slow_handler(event: Event) -> None:
    await asyncio.sleep(5)
    logger.info("handled (slow) event=%s order_id=%s", event.event_type, event.order_id)


async def fail_once_handler(event: Event) -> None:
    retries = int(event.payload.get("_retries", 0))
    if retries == 0:
        logger.info("simulating failure for order_id=%s", event.order_id)
        raise RuntimeError("simulated transient failure")
    logger.info("succeeded on retry for order_id=%s", event.order_id)


def uppercase_processor(event: Event) -> Event:
    return event.model_copy(update={
        "event_type": event.event_type.upper(),
        "payload": {k: str(v).upper() if isinstance(v, str) else v for k, v in event.payload.items()},
    })


async def run_consumer(
    consumer_factory, topic: str, group: str, event_handler, timeout: float,
    **kwargs,
):
    task = asyncio.create_task(
        consumer_factory(topic, group, event_handler, **kwargs)
    )
    await asyncio.sleep(timeout)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ── Demo sections ─────────────────────────────────────────────────────────────

async def demo_at_most_once() -> None:
    logger.info("=" * 50)
    logger.info("AT-MOST-ONCE  (acks=0, semaphore, timeout, metrics)")
    logger.info("=" * 50)

    metrics = ConsumerMetrics()
    consumer_task = asyncio.create_task(
        consume_at_most_once(
            TOPIC + ".amo", "group-amo", slow_handler,
            max_concurrent=2, max_processing_time=3, metrics=metrics,
        )
    )
    await asyncio.sleep(4)

    producer = await create_producer_at_most_once()
    for i in range(3):
        e = new_event("demo.amo", f"amo-{i}", {"data": i})
        await producer.send(TOPIC + ".amo", value=e, key=e.order_id)
        logger.info("sent (fire-and-forget) order_id=%s", e.order_id)
    await producer.stop()

    await asyncio.sleep(10)
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass


async def demo_at_least_once() -> None:
    logger.info("=" * 50)
    logger.info("AT-LEAST-ONCE (circuit breaker, retry, DLQ)")
    logger.info("=" * 50)

    producer = await create_producer_at_least_once()
    for i in range(3):
        e = new_event("demo.alo", f"alo-{i}", {"data": i})
        await producer.send_and_wait(TOPIC + ".alo", value=e, key=e.order_id)
        logger.info("sent (acked) order_id=%s", e.order_id)
    await producer.stop()

    await run_consumer(
        consume_at_least_once, TOPIC + ".alo", "group-alo",
        fail_once_handler, timeout=15,
        max_concurrent=2, circuit_breaker=CircuitBreaker(failure_threshold=5),
    )


async def demo_idempotent() -> None:
    logger.info("=" * 50)
    logger.info("EXACTLY-ONCE  (idempotent producer)")
    logger.info("=" * 50)

    producer = await create_producer_idempotent()
    for i in range(3):
        e = new_event("demo.eo", f"eo-{i}", {"data": i})
        await producer.send_and_wait(TOPIC + ".eo", value=e, key=e.order_id)
        logger.info("sent order_id=%s", e.order_id)
    await producer.stop()

    await run_consumer(
        consume_at_least_once, TOPIC + ".eo", "group-eo",
        echo_handler, timeout=8,
    )


async def demo_transactional() -> None:
    logger.info("=" * 50)
    logger.info("TRANSACTIONAL  (consume-transform-produce)")
    logger.info("=" * 50)

    producer = await create_producer_at_least_once()
    for i in range(3):
        e = new_event("demo.txn.source", f"txn-{i}", {"data": i})
        await producer.send_and_wait(TOPIC + ".txn.source", value=e, key=e.order_id)
        logger.info("sent source order_id=%s", e.order_id)
    await producer.stop()

    task = asyncio.create_task(
        consume_transform_produce(
            TOPIC + ".txn.source",
            TOPIC + ".txn.target",
            uppercase_processor,
            transactional_id="txn-demo",
            group_id="group-txn",
        )
    )
    await asyncio.sleep(15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def main() -> None:
    logger.info("Make sure Kafka is running (docker compose up -d)")
    logger.info("Using topics: %s.{amo,alo,eo,txn.source}", TOPIC)
    logger.info("")

    await demo_at_most_once()
    logger.info("")
    await demo_at_least_once()
    logger.info("")
    await demo_idempotent()
    logger.info("")
    await demo_transactional()


if __name__ == "__main__":
    asyncio.run(main())
