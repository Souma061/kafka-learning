import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

from redis.asyncio import Redis, from_url
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from services.shared.config import REDIS_URL
from services.shared.events import Event, new_event
from services.shared.kafka import create_producer, publish, consume_forever
from services.shared.topics import (
    ORDERS_CREATED,
    INVENTORY_RESERVED,
    INVENTORY_REJECTED,
    ORDERS_CONFIRMED,
    ORDERS_REJECTED,
    OUTBOX_PREFIX,
)

logging.basicConfig(level=logging.INFO)

redis_client: Redis | None = None
producer = None
tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, producer

    redis_client = from_url(REDIS_URL, decode_responses=True)
    producer = await create_producer()

    tasks.append(
        asyncio.create_task(
            consume_forever(
                INVENTORY_RESERVED,
                "order-service",
                handle_inventory_reserved,
            )
        )
    )

    tasks.append(
        asyncio.create_task(
            consume_forever(
                INVENTORY_REJECTED,
                "order-service",
                handle_inventory_rejected,
            )
        )
    )

    yield  # application runs here

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    if producer:
        await producer.stop()

    if redis_client:
        await redis_client.aclose()


app = FastAPI(title="Order Service", lifespan=lifespan)


class CreateOrderRequest(BaseModel):
    customer_email: str = Field(..., examples=["john.doe@example.com"])
    product_id: str = Field(..., examples=["product-1"])
    quantity: int = Field(..., gt=0, examples=[1, 2, 3])


@app.post("/orders")
async def create_order(request: CreateOrderRequest) -> dict[str, str]:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis is not initialized")

    order_id = str(uuid4())

    order = {
        "order_id": order_id,
        "customer_email": request.customer_email,
        "product_id": request.product_id,
        "quantity": str(request.quantity),
        "status": "PENDING",
        "created_at": datetime.now(UTC).isoformat(),
    }

    event = new_event(
        "OrderCreated",
        order_id,
        {
            "customer_email": request.customer_email,
            "product_id": request.product_id,
            "quantity": request.quantity,
        },
    )

    async with redis_client.pipeline(transaction=True) as pipe:
        await pipe.hset(f"order:{order_id}", mapping=order)
        await pipe.hset(
            f"{OUTBOX_PREFIX}{event.event_id}",
            mapping={
                "topic": ORDERS_CREATED,
                "key": order_id,
                "value": event.model_dump_json(),
            },
        )
        await pipe.execute()

    return {
        "order_id": order_id,
        "message": "Order created successfully",
    }


@app.get("/orders/{order_id}")
async def get_order(order_id: str) -> dict[str, str]:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis is not initialized")

    order = await redis_client.hgetall(f"order:{order_id}")

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    return order


async def already_processed(event: Event) -> bool:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    was_set = await redis_client.set(
        f"order:processed:{event.event_id}",
        "1",
        nx=True,
        ex=86400,
    )

    return was_set is None


async def handle_inventory_reserved(event: Event) -> None:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    if await already_processed(event):
        return

    order = await redis_client.hgetall(f"order:{event.order_id}")
    if not order:
        logging.warning("InventoryReserved for missing order %s", event.order_id)
        return

    if order.get("status") != "PENDING":
        logging.info(
            "InventoryReserved ignored for order %s with status=%s",
            event.order_id,
            order.get("status"),
        )
        return

    await redis_client.hset(
        f"order:{event.order_id}",
        mapping={"status": "CONFIRMED"},
    )

    confirmed_event = new_event(
        "OrderConfirmed",
        event.order_id,
        {
            "customer_email": order["customer_email"],
            "product_id": order["product_id"],
            "quantity": int(order["quantity"]),
        },
        correlation_id=event.correlation_id,
    )

    await publish(producer, ORDERS_CONFIRMED, confirmed_event)


async def handle_inventory_rejected(event: Event) -> None:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")

    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    if await already_processed(event):
        return

    order = await redis_client.hgetall(f"order:{event.order_id}")
    if not order:
        logging.warning("InventoryRejected for missing order %s", event.order_id)
        return

    if order.get("status") != "PENDING":
        logging.info(
            "InventoryRejected ignored for order %s with status=%s",
            event.order_id,
            order.get("status"),
        )
        return

    reason = event.payload.get("reason", "INVENTORY_REJECTED")

    await redis_client.hset(
        f"order:{event.order_id}",
        mapping={
            "status": "REJECTED",
            "reason": reason,
        },
    )

    rejected_event = new_event(
        "OrderRejected",
        event.order_id,
        {
            "customer_email": order["customer_email"],
            "product_id": order["product_id"],
            "quantity": int(order["quantity"]),
            "reason": reason,
        },
        correlation_id=event.correlation_id,
    )

    await publish(producer, ORDERS_REJECTED, rejected_event)


