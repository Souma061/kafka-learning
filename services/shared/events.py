# this file defines the events that will be used in the application. Each event is represented as a class with a name and a payload. The payload is a dictionary that contains the data associated with the event. This structure allows us to easily serialize and deserialize events when sending them through Kafka.

from datetime import datetime,UTC
from typing import Any
from uuid import uuid4
from pydantic import BaseModel, Field


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4())) # Unique identifier for the event
    event_type : str # Type of the event (e.g., "order.created", "inventory.reserved")
    order_id: str # ID of the order associated with the event
    correlation_id: str # Correlation ID for tracing the flow of events across services
    payload: dict[str, Any] # The actual data associated with the event
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC)) # Timestamp when the event was created


def new_event(event_type:str, order_id:str, payload: dict[str,Any], correlation_id: str | None = None) -> Event:
    """
    Helper function to create a new event with the given type, order ID, payload, and optional correlation ID.
    """

    return Event(
        event_type=event_type,
        order_id=order_id,
        payload=payload,
        correlation_id=correlation_id or str(uuid4()) # Generate a new correlation ID if not provided
    )


