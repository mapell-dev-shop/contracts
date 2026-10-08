"""Publishing and consuming events over Kafka, and an in-memory publisher for tests."""

import json
import signal
from collections.abc import Callable, Mapping
from types import FrameType
from typing import Any, Protocol

from shop_contracts.events import topic_for, validate_event

Handler = Callable[[dict[str, Any]], None]


class EventNotDelivered(Exception):
    """Kafka did not confirm the event within the delivery timeout."""


class EventPublisher(Protocol):
    def publish(self, envelope: dict[str, Any], key: str) -> None: ...


class InMemoryEventPublisher:
    """Keeps published events in a list so tests can assert on them."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    def publish(self, envelope: dict[str, Any], key: str) -> None:
        validate_event(envelope)
        self.published.append(envelope)


# "localhost" also resolves to the IPv6 address ::1, where the Docker broker does not listen.
# Using IPv4 only skips a failed connection attempt and its log line on every connect.
IPV4_ONLY = {"broker.address.family": "v4"}


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str, delivery_timeout_ms: int = 5000) -> None:
        from confluent_kafka import Producer

        self._timeout_s = delivery_timeout_ms / 1000
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "message.timeout.ms": delivery_timeout_ms,
                **IPV4_ONLY,
            }
        )

    def publish(self, envelope: dict[str, Any], key: str) -> None:
        """Send the event and wait until Kafka confirms it. Raise EventNotDelivered otherwise."""
        validate_event(envelope)
        failures: list[str] = []

        def on_delivery(error: object, _message: object) -> None:
            if error is not None:
                failures.append(str(error))

        self._producer.produce(
            topic_for(envelope["type"]),
            key=key,
            value=json.dumps(envelope).encode(),
            on_delivery=on_delivery,
        )
        still_queued = self._producer.flush(self._timeout_s + 1)
        if failures or still_queued:
            reason = failures[0] if failures else "no confirmation from Kafka"
            raise EventNotDelivered(f"{envelope['type']} {envelope['event_id']}: {reason}")


def run_consumer(bootstrap_servers: str, group_id: str, handlers: Mapping[str, Handler]) -> None:
    """Consume the handlers' event types until the process stops.

    The offset is committed only after the handler returns, so delivery is at least once:
    handlers must be idempotent. A handler that raises stops the worker; the event is
    delivered again when the worker restarts. SIGTERM lets the current event finish, then
    closes the consumer so it leaves its group at once and a restarted worker gets its
    partitions without waiting for the group's session timeout.
    """
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            **IPV4_ONLY,
        }
    )
    stopping = False

    def request_stop(signum: int, frame: FrameType | None) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    consumer.subscribe([topic_for(event_type) for event_type in handlers])
    try:
        while not stopping:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            value = message.value()
            if value is None:
                raise ValueError(f"message without a value on {message.topic()}")
            envelope = json.loads(value)
            handlers[envelope["type"]](envelope)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
