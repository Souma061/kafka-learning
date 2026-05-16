"""
Email Service
=============
Consumes order events from Kafka and sends real transactional emails via Resend.

Throttling experiment
---------------------
This service exposes TWO paths to send an order-confirmation email:

  1. Kafka path  (POST /orders → order-service → Kafka → this consumer)
     The consumer processes at EMAIL_RATE_LIMIT_PER_SECOND (default 2/s).
     No matter how many orders arrive simultaneously, emails are sent
     in a controlled, steady stream.  The HTTP caller gets an instant 200.

  2. Direct path  (POST /direct/send-email on THIS service)
     Calls Resend synchronously, in-request, with no buffer.
     Under load the event loop blocks, response times spike, and Resend
     will start returning 429 Too Many Requests.

Run the load test to see the difference:
  python scripts/load_test.py
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from uuid import uuid4

import resend
from redis.asyncio import Redis, from_url
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr

from services.shared.config import (
    REDIS_URL,
    RESEND_API_KEY,
    EMAIL_FROM,
    EMAIL_RATE_LIMIT_PER_SECOND,
)
from services.shared.events import Event
from services.shared.kafka import consume_forever
from services.shared.topics import ORDERS_CONFIRMED, ORDERS_REJECTED

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("email-service")

# Minimum seconds between emails = 1 / rate_limit
_MIN_INTERVAL = 1.0 / EMAIL_RATE_LIMIT_PER_SECOND
_last_sent_at: float = 0.0        # wall-clock time of the last send
_emails_sent: int = 0             # counter for the health endpoint
_emails_failed: int = 0

_resend_api_calls: int = 0
_resend_api_window_start: float = 0.0

redis_client: Redis | None = None
tasks: list[asyncio.Task] = []


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client

    if not RESEND_API_KEY:
        raise RuntimeError(
            "RESEND_API_KEY is not set. "
            "Export it or add it to docker-compose.yml."
        )

    resend.api_key = RESEND_API_KEY
    redis_client = from_url(REDIS_URL, decode_responses=True)

    tasks.append(
        asyncio.create_task(
            consume_forever(ORDERS_CONFIRMED, "email-service", handle_order_confirmed)
        )
    )
    tasks.append(
        asyncio.create_task(
            consume_forever(ORDERS_REJECTED, "email-service", handle_order_rejected)
        )
    )

    yield  # application runs here

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    if redis_client:
        await redis_client.aclose()


app = FastAPI(
    title="Email Service",
    description=__doc__,
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health / stats — useful to watch during the load test
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "emails_sent": _emails_sent,
        "emails_failed": _emails_failed,
        "rate_limit_per_second": EMAIL_RATE_LIMIT_PER_SECOND,
        "simulated_api_calls_this_window": _resend_api_calls,  # add this
        "last_sent_at": _last_sent_at,                          # add this
    }

# ---------------------------------------------------------------------------
# Direct (non-Kafka) endpoint — PATH 2 for the throttling experiment
# ---------------------------------------------------------------------------

class DirectEmailRequest(BaseModel):
    to: EmailStr
    order_id: str
    product_id: str
    quantity: int
    confirmed: bool = True
    reason: str = ""


@app.post("/direct/send-email", status_code=200)
async def direct_send_email(req: DirectEmailRequest):
    """
    Sends an email DIRECTLY from the HTTP request — no Kafka buffer.
    Hammer this endpoint with concurrent requests to observe:
      - Response latencies climbing as the event loop is blocked
      - Resend returning 429 when rate-limit is exceeded
      - Lost emails (no retry / no DLQ)
    """
    try:
        result = await _send_via_resend(
            to=req.to,
            subject=f"Order {'Confirmed' if req.confirmed else 'Rejected'}: {req.order_id}",
            html=_render_html(req.order_id, req.product_id, req.quantity, req.confirmed, req.reason),
            product_id=req.product_id,
        )
        return {"email_id": result.id, "path": "direct"}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


# ---------------------------------------------------------------------------
# Kafka consumer handlers — PATH 1 for the throttling experiment
# ---------------------------------------------------------------------------

async def already_processed(event: Event) -> bool:
    assert redis_client is not None
    return not await redis_client.set(
        f"email:processed:{event.event_id}", "1", ex=86400, nx=True
    )


async def handle_order_confirmed(event: Event) -> None:
    if await already_processed(event):
        logger.info("Event %s already processed, skipping", event.event_id)
        return

    customer_email = event.payload.get("customer_email")
    if not customer_email:
        logger.warning("OrderConfirmed %s missing customer_email, skipping", event.event_id)
        return

    product_id = event.payload.get("product_id", "unknown")
    await _throttled_send(
        to=customer_email,
        subject=f"✅ Order Confirmed: {event.order_id}",
        html=_render_html(
            order_id=event.order_id,
            product_id=product_id,
            quantity=int(event.payload.get("quantity", 0)),
            confirmed=True,
        ),
        event_id=event.event_id,
        product_id=product_id,
    )


async def handle_order_rejected(event: Event) -> None:
    if await already_processed(event):
        logger.info("Event %s already processed, skipping", event.event_id)
        return

    customer_email = event.payload.get("customer_email")
    if not customer_email:
        logger.warning("OrderRejected %s missing customer_email, skipping", event.event_id)
        return

    product_id = event.payload.get("product_id", "unknown")
    await _throttled_send(
        to=customer_email,
        subject=f"❌ Order Rejected: {event.order_id}",
        html=_render_html(
            order_id=event.order_id,
            product_id=product_id,
            quantity=int(event.payload.get("quantity", 0)),
            confirmed=False,
            reason=event.payload.get("reason", "UNKNOWN"),
        ),
        event_id=event.event_id,
        product_id=product_id,
    )



# ---------------------------------------------------------------------------
# Core send helpers
# ---------------------------------------------------------------------------

async def _throttled_send(to: str, subject: str, html: str, event_id: str, product_id: str) -> None:
    """
    Sends via Resend but enforces EMAIL_RATE_LIMIT_PER_SECOND.
    This is the Kafka path — the consumer sleeps here, NOT the HTTP caller.
    The caller already got their 200 OK when the order was placed.
    """
    global _last_sent_at, _emails_sent, _emails_failed

    # Rate-limit: sleep until the minimum interval has elapsed
    elapsed = time.monotonic() - _last_sent_at
    if elapsed < _MIN_INTERVAL:
        await asyncio.sleep(_MIN_INTERVAL - elapsed)

    _last_sent_at = time.monotonic()

    try:
        result = await _send_via_resend(to=to, subject=subject, html=html, product_id=product_id)
        _emails_sent += 1
        logger.info(
            "📧 email sent email_id=%s to=%s event_id=%s",
            result.id, to, event_id,
        )
    except Exception as exc:
        _emails_failed += 1
        logger.error("📧 email FAILED to=%s event_id=%s error=%s", to, event_id, exc)
        raise   # let consume_forever handle retry / DLQ


async def _send_via_resend(to: str, subject: str, html: str, product_id: str):
    """
    Resend's SDK is synchronous — run it in a thread so we don't block
    the async event loop.
    """
    global _resend_api_calls, _resend_api_window_start

    # --- SIMULATE RESEND API RATE LIMITING (5 req / second) ---
    now = time.monotonic()
    if now - _resend_api_window_start >= 1.0:
        _resend_api_window_start = now
        _resend_api_calls = 0

    _resend_api_calls += 1
    if _resend_api_calls > 5:
        raise Exception("Too many requests. You can only make 5 requests per second. See rate limit response header.")

    # --- ONLY HIT ACTUAL RESEND API FOR SPECIAL TRIGGER ---
    if product_id == "SPECIAL-EMAIL-TRIGGER":
        params: resend.Emails.SendParams = {
            "from": EMAIL_FROM,
            "to": [to],
            "subject": subject,
            "html": html,
        }
        return await asyncio.to_thread(resend.Emails.send, params)
    else:
        # Fake a successful send for all the load testing spam
        class FakeResult:
            def __init__(self):
                self.id = f"fake_{uuid4()}"
        await asyncio.sleep(0.1)  # Simulate network latency
        return FakeResult()


def _render_html(
    order_id: str,
    product_id: str,
    quantity: int,
    confirmed: bool,
    reason: str = "",
) -> str:
    color = "#16a34a" if confirmed else "#dc2626"
    status = "Confirmed ✅" if confirmed else "Rejected ❌"
    extra = f"<p><strong>Reason:</strong> {reason}</p>" if reason else ""
    return f"""
    <div style="font-family:sans-serif;max-width:480px;margin:auto;padding:24px;border:1px solid #e5e7eb;border-radius:8px">
      <h2 style="color:{color}">Order {status}</h2>
      <p><strong>Order ID:</strong> {order_id}</p>
      <p><strong>Product:</strong> {product_id}</p>
      <p><strong>Quantity:</strong> {quantity}</p>
      {extra}
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0"/>
      <p style="color:#6b7280;font-size:12px">Kafka Learning Project — email-service</p>
    </div>
    """
