"""Event types, their topics and schemas, and the envelope every event travels in."""

import json
import uuid
from datetime import UTC, datetime
from functools import cache
from importlib import resources
from typing import Any

from jsonschema import Draft202012Validator

# event type -> (Kafka topic, schema file in schemas/events/)
EVENTS: dict[str, tuple[str, str]] = {
    "OrderPlaced": ("shop.orders.order-placed.v1", "order-placed.v1.schema.json"),
    "StockReserved": ("shop.inventory.stock-reserved.v1", "stock-reserved.v1.schema.json"),
    "StockRejected": ("shop.inventory.stock-rejected.v1", "stock-rejected.v1.schema.json"),
    "PaymentCaptured": ("shop.payments.payment-captured.v1", "payment-captured.v1.schema.json"),
    "PaymentFailed": ("shop.payments.payment-failed.v1", "payment-failed.v1.schema.json"),
    "OrderCancelled": ("shop.orders.order-cancelled.v1", "order-cancelled.v1.schema.json"),
    "PaymentRefunded": ("shop.payments.payment-refunded.v1", "payment-refunded.v1.schema.json"),
}


def topic_for(event_type: str) -> str:
    return EVENTS[event_type][0]


def load_schema(event_type: str) -> dict[str, Any]:
    schema_file = EVENTS[event_type][1]
    text = resources.files("shop_contracts").joinpath("schemas", "events", schema_file).read_text()
    schema: dict[str, Any] = json.loads(text)
    return schema


def new_envelope(event_type: str, data: dict[str, Any]) -> dict[str, Any]:
    if event_type not in EVENTS:
        raise KeyError(f"unknown event type: {event_type}")
    return {
        "event_id": str(uuid.uuid4()),
        "type": event_type,
        "version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "data": data,
    }


@cache
def _validator(event_type: str) -> Draft202012Validator:
    return Draft202012Validator(load_schema(event_type))


def validate_event(envelope: dict[str, Any]) -> None:
    """Raise jsonschema.ValidationError if the envelope breaks its event's schema."""
    _validator(envelope["type"]).validate(envelope)
