import pytest
from jsonschema import ValidationError

from shop_contracts.events import new_envelope
from shop_contracts.messaging import InMemoryEventPublisher


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
