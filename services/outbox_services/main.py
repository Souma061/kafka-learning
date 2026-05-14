import asyncio
import logging
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from redis.asyncio import Redis, from_url

from services.shared.config import OUTBOX_POLL_INTERVAL, REDIS_URL
from services.shared.events import Event
from services.shared.kafka import create_producer, publish
from services.shared.topics import OUTBOX_PREFIX

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("outbox-service")

redis_client: Redis | None = None
producer: AIOKafkaProducer | None = None
tasks: list[asyncio.Task] = []

_published_count = 0
_failed_count = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, producer

    redis_client = from_url(REDIS_URL, decode_responses=True)
    producer = await create_producer()

    tasks.append(asyncio.create_task(relay_outbox_forever()))

    yield

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    if producer:
        await producer.stop()

    if redis_client:
        await redis_client.aclose()


app = FastAPI(title="Outbox Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, int | float | str]:
    return {
        "status": "ok",
        "published_count": _published_count,
        "failed_count": _failed_count,
        "poll_interval_seconds": OUTBOX_POLL_INTERVAL,
    }


async def relay_outbox_forever() -> None:
    while True:
        try:
            await relay_outbox_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("outbox relay iteration failed")

        await asyncio.sleep(OUTBOX_POLL_INTERVAL)


async def relay_outbox_once() -> None:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    async for key in redis_client.scan_iter(match=f"{OUTBOX_PREFIX}*"):
        await publish_outbox_entry(key)


async def publish_outbox_entry(key: str) -> None:
    global _published_count, _failed_count

    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    lock_key = f"outbox-lock:{key}"
    lock_acquired = await redis_client.set(lock_key, "1", nx=True, ex=30)
    if not lock_acquired:
        return

    try:
        entry = await redis_client.hgetall(key)
        if not entry:
            return

        topic = entry.get("topic")
        value = entry.get("value")

        if not topic or not value:
            _failed_count += 1
            logger.error("invalid outbox entry key=%s entry=%s", key, entry)
            return

        event = Event.model_validate_json(value)

        logger.info(
            "relaying outbox key=%s topic=%s event_type=%s order_id=%s correlation_id=%s",
            key,
            topic,
            event.event_type,
            event.order_id,
            event.correlation_id,
        )

        await publish(producer, topic, event)
        await redis_client.delete(key)
        _published_count += 1
    except Exception:
        _failed_count += 1
        logger.exception("failed to relay outbox entry key=%s", key)
    finally:
        await redis_client.delete(lock_key)
