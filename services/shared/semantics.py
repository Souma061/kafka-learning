import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
from services.shared.config import KAFKA_BOOTSTRAP_SERVERS
from services.shared.events import Event
from services.shared.topics import DLQ_SUFFIX

logger = logging.getLogger("kafka-learning")


# ── Metrics collector ─────────────────────────────────────────────────────────

@dataclass
class ConsumerMetrics:
    processed: int = 0
    failed: int = 0
    dlq_sent: int = 0
    total_time: float = 0.0
    _start: float = field(default_factory=time.monotonic, init=False)

    def record_success(self, elapsed: float) -> None:
        self.processed += 1
        self.total_time += elapsed

    def record_failure(self) -> None:
        self.failed += 1

    def record_dlq(self) -> None:
        self.dlq_sent += 1

    @property
    def avg_time(self) -> float:
        n = self.processed + self.failed
        return self.total_time / n if n else 0.0

    @property
    def uptime(self) -> float:
        return time.monotonic() - self._start

    def log_summary(self, topic: str) -> None:
        n = self.processed + self.failed
        logger.info(
            "metrics topic=%s processed=%s failed=%s dlq=%s avg_ms=%.1f uptime=%.1fs",
            topic, self.processed, self.failed, self.dlq_sent,
            self.avg_time * 1000, self.uptime,
        )


# ── Circuit breaker ───────────────────────────────────────────────────────────

class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = CircuitState.CLOSED

    def __call__(self) -> bool:
        now = time.monotonic()
        if self.state == CircuitState.OPEN:
            if now - self.last_failure_time >= self.recovery_timeout:
                logger.info("circuit breaker half-open — allowing probe")
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True

    def success(self) -> None:
        if self.state == CircuitState.HALF_OPEN:
            logger.info("circuit breaker closed — probe succeeded")
            self.state = CircuitState.CLOSED
        self.failure_count = 0

    def failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        if self.failure_count >= self.failure_threshold:
            logger.warning(
                "circuit breaker open — %s consecutive failures", self.failure_count
            )
            self.state = CircuitState.OPEN


# ── At-most-once ──────────────────────────────────────────────────────────────

async def create_producer_at_most_once() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=0,
    )
    await producer.start()
    return producer


async def consume_at_most_once(
    topic: str,
    group_id: str,
    handler: Callable[[Event], Awaitable[None]],
    max_concurrent: int = 10,
    max_processing_time: float | None = None,
    metrics: ConsumerMetrics | None = None,
) -> None:
    semaphore = asyncio.Semaphore(max_concurrent)
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        auto_offset_reset="latest",
        value_deserializer=lambda v: Event.model_validate_json(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8"),
    )
    await consumer.start()
    m = metrics or ConsumerMetrics()

    async def _handle(msg) -> None:
        async with semaphore:
            start = time.monotonic()
            try:
                coro = handler(msg.value)
                if max_processing_time is not None:
                    coro = asyncio.wait_for(coro, timeout=max_processing_time)
                await coro
                m.record_success(time.monotonic() - start)
            except Exception:
                logger.exception("at-most-once consumer skipping failed msg")
                m.record_failure()

    try:
        async for msg in consumer:
            asyncio.ensure_future(_handle(msg))
    finally:
        await consumer.stop()
        m.log_summary(topic)


# ── At-least-once ─────────────────────────────────────────────────────────────

async def create_producer_at_least_once() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retry_backoff_ms=500,
    )
    await producer.start()
    return producer


async def consume_at_least_once(
    topic: str,
    group_id: str,
    handler: Callable[[Event], Awaitable[None]],
    max_retries: int = 3,
    max_concurrent: int = 10,
    max_processing_time: float | None = None,
    metrics: ConsumerMetrics | None = None,
    circuit_breaker: CircuitBreaker | None = None,
) -> None:
    semaphore = asyncio.Semaphore(max_concurrent)
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: Event.model_validate_json(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8"),
    )
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    await dlq_producer.start()
    await consumer.start()
    m = metrics or ConsumerMetrics()
    cb = circuit_breaker or CircuitBreaker()

    async def _handle(msg) -> None:
        async with semaphore:
            event = msg.value
            retries = int(event.payload.get("_retries", 0))
            start = time.monotonic()

            try:
                coro = handler(event)
                if max_processing_time is not None:
                    coro = asyncio.wait_for(coro, timeout=max_processing_time)
                await coro
                m.record_success(time.monotonic() - start)
                await consumer.commit()
            except asyncio.TimeoutError:
                logger.error("handler timed out topic=%s order_id=%s", topic, event.order_id)
                m.record_failure()
                await _retry_or_dlq(msg, event, retries, m, cb)
            except Exception:
                logger.exception("at-least-once handler failed topic=%s", topic)
                m.record_failure()
                await _retry_or_dlq(msg, event, retries, m, cb)

    async def _retry_or_dlq(msg, event, retries, m, cb) -> None:
        if not cb():
            logger.warning(
                "circuit open — skipping DLQ for order_id=%s", event.order_id
            )
            return

        if retries >= max_retries:
            try:
                event = event.model_copy(update={
                    "payload": {**event.payload, "error": "max retries exceeded"},
                })
                await dlq_producer.send_and_wait(
                    topic + DLQ_SUFFIX, value=event, key=event.order_id
                )
                m.record_dlq()
                cb.success()
            except Exception:
                logger.exception("DLQ send failed topic=%s", topic)
                cb.failure()
            await consumer.commit()
        else:
            event = event.model_copy(update={
                "payload": {**event.payload, "_retries": retries + 1},
            })
            await dlq_producer.send_and_wait(
                msg.topic, value=event, key=event.order_id
            )
            await consumer.commit()

    try:
        async for msg in consumer:
            asyncio.ensure_future(_handle(msg))
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        m.log_summary(topic)


# ── Exactly-once (idempotent producer) ───────────────────────────────────────

async def create_producer_idempotent() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        enable_idempotence=True,
    )
    await producer.start()
    return producer


# ── Exactly-once (transactional) ──────────────────────────────────────────────

async def create_producer_transactional(transactional_id: str) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        enable_idempotence=True,
        transactional_id=transactional_id,
    )
    await producer.start()
    return producer


async def run_transaction(
    producer: AIOKafkaProducer,
    order_id: str,
    events: list[tuple[str, Event]],
) -> None:
    await producer.begin_transaction()
    try:
        for topic, event in events:
            await producer.send(topic, value=event, key=order_id)
        await producer.commit_transaction()
    except Exception:
        await producer.abort_transaction()
        raise


async def consume_transform_produce(
    source_topic: str,
    target_topic: str,
    processor: Callable[[Event], Event],
    transactional_id: str,
    group_id: str,
    max_processing_time: float | None = None,
    metrics: ConsumerMetrics | None = None,
) -> None:
    producer = await create_producer_transactional(transactional_id)
    consumer = AIOKafkaConsumer(
        source_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        value_deserializer=lambda v: Event.model_validate_json(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8"),
    )
    await consumer.start()
    m = metrics or ConsumerMetrics()

    try:
        async for msg in consumer:
            start = time.monotonic()
            try:
                result = await asyncio.to_thread(processor, msg.value)

                await producer.begin_transaction()
                try:
                    await producer.send(
                        target_topic, value=result, key=result.order_id
                    )
                    tp = TopicPartition(msg.topic, msg.partition)
                    await producer.send_offsets_to_transaction(
                        {tp: msg.offset + 1}, group_id
                    )
                    await producer.commit_transaction()
                    m.record_success(time.monotonic() - start)
                except Exception:
                    await producer.abort_transaction()
                    raise
            except Exception:
                logger.exception("consume-transform-produce failed")
                m.record_failure()
    finally:
        await consumer.stop()
        await producer.stop()
        m.log_summary(f"{source_topic}→{target_topic}")
