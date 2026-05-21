import asyncio
import logging
from contextlib import asynccontextmanager

from redis.asyncio import Redis, from_url
from fastapi import FastAPI, HTTPException
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel, Field

from services.shared.config import REDIS_URL
from services.shared.telemetry import setup_tracing
from services.shared.events import Event, new_event
from services.shared.kafka import consume_forever, create_producer, publish
from services.shared.topics import (
    ORDERS_CONFIRMED,
    ORDERS_CREATED,
    ORDERS_REJECTED,
    INVENTORY_RESERVED,
    INVENTORY_REJECTED,
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
                ORDERS_CREATED,
                "inventory-service",
                handle_order_created,
            )
        )
    )
    tasks.append(
        asyncio.create_task(
            consume_forever(
                ORDERS_CONFIRMED,
                "inventory-service",
                handle_order_confirmed,
            )
        )
    )
    tasks.append(
        asyncio.create_task(
            consume_forever(
                ORDERS_REJECTED,
                "inventory-service",
                handle_order_rejected,
            )
        )
    )

    yield  # application runs here

    # --- shutdown ---
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    if producer:
        await producer.stop()

    if redis_client:
        await redis_client.aclose()


setup_tracing("inventory-service")
app = FastAPI(title="Inventory Service", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


class AdjustInventoryRequest(BaseModel):
    product_id: str
    quantity: int = Field(..., gt=0)


@app.get("/inventory")
async def get_inventory():
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis is not initialized")
    inventory = await redis_client.hgetall("inventory")
    return inventory


@app.post("/inventory/adjust")
async def adjust_inventory(request: AdjustInventoryRequest):
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis is not initialized")

    current_stock = int(await redis_client.hget("inventory", request.product_id) or 0)
    if current_stock < request.quantity:
        raise HTTPException(
            status_code=409,
            detail=f"Insufficient stock: available={current_stock}, requested={request.quantity}",
        )

    await redis_client.hincrby("inventory", request.product_id, -request.quantity)
    return {"message": "Inventory adjusted successfully"}


async def already_processed(event: Event) -> bool:
    assert redis_client is not None
    return not await redis_client.set(
        f"processed:{event.event_id}", "true", ex=3600, nx=True
    )


async def handle_order_created(event: Event):
    if await already_processed(event):
        logging.info(f"Event {event.event_id} already processed, skipping")
        return

    assert producer is not None

    # BUG FIX: was incorrectly reading `event.payload["producer_id"]` (KeyError).
    # The correct field is `event.payload["product_id"]`.
    product_id = event.payload["product_id"]
    quantity = int(event.payload["quantity"])

    order = await redis_client.hgetall(f"order:{event.order_id}")
    if order.get("status") != "PENDING":
        logging.info(
            "OrderCreated ignored for order %s with status=%s",
            event.order_id,
            order.get("status"),
        )
        return

    current_stock = int(await redis_client.hget("inventory", product_id) or 0)

    if current_stock >= quantity:
        new_stock = await redis_client.hincrby("inventory", product_id, -quantity)

        # BUG FIX: was using `event.payload['order_id']` (KeyError).
        # order_id is a top-level field on the Event model, not inside payload.
        await redis_client.hset(
            f"InventoryReserved:{event.order_id}",
            mapping={
                "product_id": product_id,
                "quantity": str(quantity),
            },
        )

        reserved = new_event(
            "InventoryReserved",
            event.order_id,
            {
                "product_id": product_id,
                "quantity": quantity,
                "remaining_stock": new_stock,
            },
            correlation_id=event.correlation_id,
        )
        await publish(producer, INVENTORY_RESERVED, reserved)
        return

    rejected = new_event(
        "InventoryRejected",
        event.order_id,
        {
            "product_id": product_id,
            "quantity": quantity,
            "reason": "OUT_OF_STOCK",
        },
        correlation_id=event.correlation_id,
    )
    await publish(producer, INVENTORY_REJECTED, rejected)


async def handle_order_confirmed(event: Event):
    if await already_processed(event):
        logging.info(f"Event {event.event_id} already processed, skipping")
        return

    await redis_client.delete(f"InventoryReserved:{event.order_id}")


async def handle_order_rejected(event: Event):
    if await already_processed(event):
        logging.info(f"Event {event.event_id} already processed, skipping")
        return

    # BUG FIX: was using `event.payload['order_id']` (KeyError in original, consistent fix here).
    # Using `event.order_id` which is the correct top-level field.
    reservation = await redis_client.hgetall(f"InventoryReserved:{event.order_id}")
    if not reservation:
        logging.warning(f"No inventory reservation found for order {event.order_id}")
        return

    product_id = reservation["product_id"]
    quantity = int(reservation["quantity"])

    # Restore the reserved stock on rejection
    await redis_client.hincrby("inventory", product_id, quantity)
    await redis_client.delete(f"InventoryReserved:{event.order_id}")
