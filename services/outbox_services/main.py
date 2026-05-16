import asyncio
import logging
from contextlib import asynccontextmanager

import uuid

import asyncpg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from redis.asyncio import Redis, from_url

from services.shared.config import (
    OUTBOX_POLL_INTERVAL,
    OUTBOX_POSTGRES_BATCH_SIZE,
    OUTBOX_REDIS_MAX_EVENTS,
    POSTGRES_DSN,
    REDIS_URL,
)
from services.shared.events import Event
from services.shared.kafka import create_producer, publish
from services.shared.topics import OUTBOX_PREFIX

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("outbox-service")

redis_client: Redis | None = None
producer: AIOKafkaProducer | None = None
pg_pool: asyncpg.Pool | None = None
tasks: list[asyncio.Task] = []

INSTANCE_ID = str(uuid.uuid4())
LEADER_LOCK_KEY = "outbox:leader_lock"
LEADER_LOCK_TTL = 5

_published_count = 0
_failed_count = 0
_spilled_count = 0
_postgres_published_count = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, pg_pool

    redis_client = from_url(REDIS_URL, decode_responses=True)
    pg_pool = await create_postgres_pool()
    await init_postgres()

    tasks.append(asyncio.create_task(relay_outbox_forever()))

    yield

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    if producer:
        await producer.stop()

    if pg_pool:
        await pg_pool.close()

    if redis_client:
        await redis_client.aclose()


app = FastAPI(title="Outbox Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, int | float | str | None]:
    redis_backlog = await count_redis_outbox_entries()
    postgres_backlog = await count_postgres_outbox_entries()

    return {
        "status": "ok",
        "instance_id": INSTANCE_ID,
        "is_leader": (await get_current_leader()) == INSTANCE_ID,
        "published_count": _published_count,
        "failed_count": _failed_count,
        "spilled_count": _spilled_count,
        "postgres_published_count": _postgres_published_count,
        "redis_backlog_count": redis_backlog,
        "postgres_backlog_count": postgres_backlog,
        "redis_max_events": OUTBOX_REDIS_MAX_EVENTS,
        "poll_interval_seconds": OUTBOX_POLL_INTERVAL,
    }

async def check_and_acquire_leadership() -> bool:
    if redis_client is None:
        return False
        
    acquired = await redis_client.set(
        LEADER_LOCK_KEY,
        INSTANCE_ID,
        nx=True,
        ex=LEADER_LOCK_TTL
    )
    if acquired:
        return True
        
    current_leader = await redis_client.get(LEADER_LOCK_KEY)
    if current_leader == INSTANCE_ID:
        await redis_client.expire(LEADER_LOCK_KEY, LEADER_LOCK_TTL)
        return True
        
    return False

async def get_current_leader() -> str | None:
    if redis_client is None:
        return None
    return await redis_client.get(LEADER_LOCK_KEY)


async def relay_outbox_forever() -> None:
    while True:
        try:
            is_leader = await check_and_acquire_leadership()
            if is_leader:
                await relay_outbox_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("outbox relay iteration failed")

        await asyncio.sleep(OUTBOX_POLL_INTERVAL)


async def relay_outbox_once() -> None:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    redis_keys = [
        key async for key in redis_client.scan_iter(match=f"{OUTBOX_PREFIX}*")
    ]

    if len(redis_keys) > OUTBOX_REDIS_MAX_EVENTS:
        await spill_redis_overflow_to_postgres(redis_keys[OUTBOX_REDIS_MAX_EVENTS:])
        redis_keys = redis_keys[:OUTBOX_REDIS_MAX_EVENTS]

    for key in redis_keys:
        await publish_outbox_entry(key)

    if not redis_keys:
        await publish_postgres_outbox_entries()


async def publish_outbox_entry(key: str) -> None:
    global _published_count, _failed_count

    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

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
        kafka_producer = await get_producer()

        logger.info(
            "relaying outbox key=%s topic=%s event_type=%s order_id=%s correlation_id=%s",
            key,
            topic,
            event.event_type,
            event.order_id,
            event.correlation_id,
        )

        await publish(kafka_producer, topic, event)
        await redis_client.delete(key)
        _published_count += 1
    except Exception:
        _failed_count += 1
        logger.exception("failed to relay outbox entry key=%s", key)
    finally:
        await redis_client.delete(lock_key)


async def get_producer() -> AIOKafkaProducer:
    global producer

    if producer is None:
        producer = await create_producer()

    return producer


async def create_postgres_pool() -> asyncpg.Pool:
    last_error: Exception | None = None

    for attempt in range(1, 31):
        try:
            return await asyncpg.create_pool(POSTGRES_DSN)
        except OSError as exc:
            last_error = exc
            logger.warning(
                "Postgres is not ready yet attempt=%s error=%s",
                attempt,
                exc,
            )
            await asyncio.sleep(1)

    raise RuntimeError("Postgres did not become ready") from last_error


async def init_postgres() -> None:
    if pg_pool is None:
        raise RuntimeError("Postgres is not initialized")

    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox_overflow (
                id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                event_key TEXT NOT NULL,
                payload JSONB NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processing_started_at TIMESTAMPTZ,
                published_at TIMESTAMPTZ
            )
            """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_outbox_overflow_pending
            ON outbox_overflow (status, next_attempt_at, created_at)
            """
        )


async def spill_redis_overflow_to_postgres(keys: list[str]) -> None:
    global _spilled_count

    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    if pg_pool is None:
        raise RuntimeError("Postgres is not initialized")

    for key in keys:
        lock_key = f"outbox-spill-lock:{key}"
        lock_acquired = await redis_client.set(lock_key, "1", nx=True, ex=30)
        if not lock_acquired:
            continue

        try:
            entry = await redis_client.hgetall(key)
            if not entry:
                continue

            topic = entry.get("topic")
            event_key = entry.get("key")
            value = entry.get("value")
            if not topic or not event_key or not value:
                logger.error("invalid outbox entry for spill key=%s entry=%s", key, entry)
                continue

            event = Event.model_validate_json(value)
            async with pg_pool.acquire() as conn:
                inserted = await conn.fetchval(
                    """
                    INSERT INTO outbox_overflow (id, topic, event_key, payload)
                    VALUES ($1, $2, $3, $4::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    """,
                    event.event_id,
                    topic,
                    event_key,
                    value,
                )

            await redis_client.delete(key)
            if inserted:
                _spilled_count += 1
                logger.info("spilled outbox key=%s to postgres", key)
        finally:
            await redis_client.delete(lock_key)


async def publish_postgres_outbox_entries() -> None:
    global _postgres_published_count, _failed_count

    if pg_pool is None:
        raise RuntimeError("Postgres is not initialized")

    rows = await fetch_postgres_batch()
    if not rows:
        return

    for row in rows:
        try:
            event = parse_event(row["payload"])
            kafka_producer = await get_producer()
            await publish(kafka_producer, row["topic"], event)
            await mark_postgres_published(row["id"])
            _postgres_published_count += 1
        except Exception:
            _failed_count += 1
            logger.exception("failed to publish postgres outbox id=%s", row["id"])
            await mark_postgres_retry(row["id"])


async def fetch_postgres_batch() -> list[asyncpg.Record]:
    if pg_pool is None:
        raise RuntimeError("Postgres is not initialized")

    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            return await conn.fetch(
                """
                WITH next_events AS (
                    SELECT id
                    FROM outbox_overflow
                    WHERE (
                        status = 'PENDING'
                        AND next_attempt_at <= NOW()
                    )
                    OR (
                        status = 'PROCESSING'
                        AND processing_started_at < NOW() - INTERVAL '5 minutes'
                    )
                    ORDER BY created_at
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_overflow
                SET status = 'PROCESSING',
                    processing_started_at = NOW()
                FROM next_events
                WHERE outbox_overflow.id = next_events.id
                RETURNING outbox_overflow.id,
                          outbox_overflow.topic,
                          outbox_overflow.event_key,
                          outbox_overflow.payload
                """,
                OUTBOX_POSTGRES_BATCH_SIZE,
            )


async def mark_postgres_published(event_id: str) -> None:
    if pg_pool is None:
        raise RuntimeError("Postgres is not initialized")

    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE outbox_overflow
            SET status = 'PUBLISHED',
                published_at = NOW()
            WHERE id = $1
            """,
            event_id,
        )


async def mark_postgres_retry(event_id: str) -> None:
    if pg_pool is None:
        raise RuntimeError("Postgres is not initialized")

    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE outbox_overflow
            SET retry_count = retry_count + 1,
                status = 'PENDING',
                processing_started_at = NULL,
                next_attempt_at = NOW()
                    + LEAST(60, POWER(2, LEAST(retry_count + 1, 6))) * INTERVAL '1 second'
            WHERE id = $1
            """,
            event_id,
        )


def parse_event(payload: object) -> Event:
    if isinstance(payload, str):
        return Event.model_validate_json(payload)

    return Event.model_validate(payload)


async def count_redis_outbox_entries() -> int:
    if redis_client is None:
        return 0

    count = 0
    async for _ in redis_client.scan_iter(match=f"{OUTBOX_PREFIX}*"):
        count += 1
    return count


async def count_postgres_outbox_entries() -> int | None:
    if pg_pool is None:
        return None

    async with pg_pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM outbox_overflow WHERE status = 'PENDING'"
        )
