import asyncio
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from services.shared.config import KAFKA_BOOTSTRAP_SERVERS
from services.shared.events import Event
from services.shared.topics import DLQ_SUFFIX

logger = logging.getLogger("kafka-learning")


async def create_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    await producer.start()
    return producer


async def publish(producer: AIOKafkaProducer, topic: str, event: Event) -> None:
    logger.info(
        "publishing topic=%s event_type=%s order_id=%s correlation_id=%s",
        topic,
        event.event_type,
        event.order_id,
        event.correlation_id,
    )
    await producer.send_and_wait(topic, value=event, key=event.order_id)


async def consume_forever(
    topic: str,
    group_id: str,
    handler: Callable[[Event], Awaitable[None]],
) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: Event.model_validate_json(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8"),
    )
    producer = await create_producer()
    await consumer.start()

    try:
        async for msg in consumer:
            event = msg.value
            retries = int(event.payload.get("_retries", 0))

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
                    event = event.model_copy(update={"payload": updated_payload})
                    await publish(producer, topic + DLQ_SUFFIX, event)
                    await consumer.commit()
                else:
                    await asyncio.sleep(1)
                    updated_payload["_retries"] = retries + 1
                    event = event.model_copy(update={"payload": updated_payload})
                    await publish(producer, msg.topic, event)
                    await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
