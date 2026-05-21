import asyncio
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from opentelemetry import propagate, trace
from opentelemetry.context import attach, detach

from services.shared.config import KAFKA_BOOTSTRAP_SERVERS
from services.shared.events import Event
from services.shared.topics import DLQ_SUFFIX

logger = logging.getLogger("kafka-learning")
_tracer = trace.get_tracer("kafka-learning")


async def create_producer() -> AIOKafkaProducer:
    last_exc: Exception | None = None
    for attempt in range(1, 11):
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        try:
            await producer.start()
            return producer
        except Exception as exc:
            last_exc = exc
            logger.warning("kafka not ready (attempt %s/10): %s", attempt, exc)
            await asyncio.sleep(min(2 ** attempt, 30))
    raise RuntimeError("Could not connect to Kafka after 10 attempts") from last_exc


async def publish(producer: AIOKafkaProducer, topic: str, event: Event) -> None:
    logger.info(
        "publishing topic=%s event_type=%s order_id=%s correlation_id=%s",
        topic,
        event.event_type,
        event.order_id,
        event.correlation_id,
    )

    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    kafka_headers = [(k, v.encode("utf-8")) for k, v in carrier.items()]

    await producer.send_and_wait(
        topic,
        value=event,
        key=event.order_id,
        headers=kafka_headers,
    )


async def consume_forever(
    topic: str,
    group_id: str,
    handler: Callable[[Event], Awaitable[None]],
) -> None:
    producer = await create_producer()

    last_exc: Exception | None = None
    for attempt in range(1, 11):
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: Event.model_validate_json(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8"),
            consumer_timeout_ms=60000,
        )
        try:
            await consumer.start()
            break
        except Exception as exc:
            last_exc = exc
            logger.warning("kafka not ready (attempt %s/10): %s", attempt, exc)
            await asyncio.sleep(min(2 ** attempt, 30))
    else:
        raise RuntimeError("Could not connect to Kafka after 10 attempts") from last_exc

    try:
        async for msg in consumer:
            event = msg.value
            retries = int(event.payload.get("_retries", 0))

            carrier = {}
            if msg.headers:
                for k, v in msg.headers:
                    if v is not None:
                        carrier[k] = v.decode("utf-8")

            ctx = propagate.extract(carrier)

            token = attach(ctx)
            try:
                with _tracer.start_as_current_span(
                    f"consume {msg.topic}",
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": msg.topic,
                        "messaging.kafka.partition": msg.partition,
                        "messaging.kafka.message_offset": msg.offset,
                        "messaging.consumer_group": group_id,
                    },
                ):
                    try:
                        logger.info(
                            "consumed topic=%s event_type=%s order_id=%s correlation_id=%s retries=%s",
                            msg.topic,
                            event.event_type,
                            event.order_id,
                            event.correlation_id,
                            retries,
                        )
                        await handler(event)
                        await consumer.commit()
                    except Exception as exc:
                        logger.exception("failed topic=%s event=%s", msg.topic, event)

                        updated_payload = dict(event.payload)
                        updated_payload["error"] = str(exc)

                        if retries >= 3:
                            event = event.model_copy(
                                update={"payload": updated_payload}
                            )
                            await publish(producer, topic + DLQ_SUFFIX, event)
                            await consumer.commit()
                        else:
                            await asyncio.sleep(1)
                            updated_payload["_retries"] = retries + 1
                            event = event.model_copy(
                                update={"payload": updated_payload}
                            )
                            await publish(producer, msg.topic, event)
                            await consumer.commit()
            finally:
                detach(token)
    finally:
        await consumer.stop()
        await producer.stop()
