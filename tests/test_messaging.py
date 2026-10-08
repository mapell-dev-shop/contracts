import pytest
from jsonschema import ValidationError

from shop_contracts.events import new_envelope
from shop_contracts.messaging import EventNotDelivered, InMemoryEventPublisher, KafkaEventPublisher


def test_in_memory_publisher_keeps_valid_events() -> None:
    publisher = InMemoryEventPublisher()
    envelope = new_envelope("PaymentFailed", {"order_id": "o-1", "reason": "card declined"})
    publisher.publish(envelope, key="o-1")
    assert publisher.published == [envelope]


def test_in_memory_publisher_refuses_an_event_that_breaks_its_schema() -> None:
    publisher = InMemoryEventPublisher()
    with pytest.raises(ValidationError):
        publisher.publish(new_envelope("PaymentFailed", {"order_id": "o-1"}), key="o-1")
    assert publisher.published == []


def test_kafka_publisher_raises_when_the_event_is_not_delivered() -> None:
    # Nothing listens on port 1, so the broker never confirms the event.
    publisher = KafkaEventPublisher("127.0.0.1:1", delivery_timeout_ms=500)
    envelope = new_envelope("PaymentFailed", {"order_id": "o-1", "reason": "card declined"})

    with pytest.raises(EventNotDelivered):
        publisher.publish(envelope, key="o-1")
