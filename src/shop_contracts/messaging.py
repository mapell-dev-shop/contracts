"""Publishing and consuming events over Kafka, and an in-memory publisher for tests."""

import json
import signal
from collections.abc import Callable, Mapping
from types import FrameType
from typing import Any, Protocol

from shop_contracts.events import topic_for, validate_event

Handler = Callable[[dict[str, Any]], None]


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
    def __init__(self, bootstrap_servers: str) -> None:
        from confluent_kafka import Producer

        self._producer = Producer({"bootstrap.servers": bootstrap_servers, **IPV4_ONLY})

    def publish(self, envelope: dict[str, Any], key: str) -> None:
        validate_event(envelope)
        self._producer.produce(
            topic_for(envelope["type"]), key=key, value=json.dumps(envelope).encode()
        )
        self._producer.flush(10)


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
