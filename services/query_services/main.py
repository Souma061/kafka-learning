import asyncio
import logging
from contextlib import asynccontextmanager

from redis.asyncio import Redis, from_url
from fastapi import FastAPI, HTTPException, Query

from services.shared.config import REDIS_URL
from services.shared.telemetry import setup_tracing
from services.shared.events import Event
from services.shared.kafka import consume_forever
from services.shared.topics import (
    ORDERS_CREATED,
    ORDERS_CONFIRMED,
    ORDERS_REJECTED,
)

logging.basicConfig(level=logging.INFO)

QUERY_REDIS_URL = REDIS_URL.rsplit("/", 1)[0] + "/1"

redis_client: Redis | None = None
tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client

    redis_client = from_url(QUERY_REDIS_URL, decode_responses=True)

    tasks.append(
        asyncio.create_task(
            consume_forever(ORDERS_CREATED, "query-service", handle_order_created)
        )
    )
    tasks.append(
        asyncio.create_task(
            consume_forever(ORDERS_CONFIRMED, "query-service", handle_order_confirmed)
        )
    )
    tasks.append(
        asyncio.create_task(
            consume_forever(ORDERS_REJECTED, "query-service", handle_order_rejected)
        )
    )

    yield

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    if redis_client:
        await redis_client.aclose()


setup_tracing("query-service")
app = FastAPI(title="Query Service (CQRS read model)", lifespan=lifespan)


# ── Projection builders ───────────────────────────────────────────────────────

async def handle_order_created(event: Event) -> None:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    await redis_client.hset(
        f"order:{event.order_id}",
        mapping={
            "order_id": event.order_id,
            "customer_email": event.payload.get("customer_email", ""),
            "product_id": event.payload.get("product_id", ""),
            "quantity": str(event.payload.get("quantity", "")),
            "status": "PENDING",
            "created_at": event.created_at.isoformat(),
        },
    )
    await redis_client.zadd("orders:index", {event.order_id: event.created_at.timestamp()})


async def handle_order_confirmed(event: Event) -> None:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    await redis_client.hset(
        f"order:{event.order_id}",
        mapping={"status": "CONFIRMED"},
    )


async def handle_order_rejected(event: Event) -> None:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    reason = event.payload.get("reason", "INVENTORY_REJECTED")
    await redis_client.hset(
        f"order:{event.order_id}",
        mapping={"status": "REJECTED", "reason": reason},
    )


# ── Query endpoints ───────────────────────────────────────────────────────────

@app.get("/orders/{order_id}")
async def get_order(order_id: str) -> dict:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis is not initialized")

    order = await redis_client.hgetall(f"order:{order_id}")
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.get("/orders")
async def list_orders(
    status: str | None = Query(None, pattern="^(PENDING|CONFIRMED|REJECTED)$"),
    limit: int = Query(20, ge=1, le=100),
) -> list[dict]:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis is not initialized")

    order_ids = await redis_client.zrevrange("orders:index", 0, limit - 1)
    results: list[dict] = []
    for oid in order_ids:
        order = await redis_client.hgetall(f"order:{oid}")
        if order and (status is None or order.get("status") == status):
            results.append(order)
    return results
