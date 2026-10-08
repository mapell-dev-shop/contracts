import pytest
from jsonschema import Draft202012Validator, ValidationError

from shop_contracts.events import EVENTS, load_schema, new_envelope, topic_for, validate_event

ORDER_PLACED_DATA = {
    "order_id": "o-1",
    "lines": [{"sku": "BOOK-1", "quantity": 2, "unit_price_cents": 2500}],
    "total_cents": 5000,
}


@pytest.mark.parametrize("event_type", sorted(EVENTS))
def test_every_event_type_has_a_valid_schema_that_names_it(event_type: str) -> None:
    schema = load_schema(event_type)
    Draft202012Validator.check_schema(schema)
    assert schema["properties"]["type"]["const"] == event_type


def test_new_envelope_passes_its_schema() -> None:
    envelope = new_envelope("OrderPlaced", ORDER_PLACED_DATA)
    validate_event(envelope)
    assert envelope["type"] == "OrderPlaced"
    assert envelope["version"] == 1


def test_two_envelopes_never_share_an_event_id() -> None:
    first = new_envelope("OrderPlaced", ORDER_PLACED_DATA)
    second = new_envelope("OrderPlaced", ORDER_PLACED_DATA)
    assert first["event_id"] != second["event_id"]


def test_missing_required_field_is_rejected() -> None:
    envelope = new_envelope("OrderPlaced", {"order_id": "o-1", "lines": []})
    with pytest.raises(ValidationError):
        validate_event(envelope)


def test_unknown_fields_are_allowed_so_additive_changes_stay_compatible() -> None:
    envelope = new_envelope("OrderPlaced", {**ORDER_PLACED_DATA, "currency": "EUR"})
    validate_event(envelope)


def test_unknown_event_type_is_refused() -> None:
    with pytest.raises(KeyError):
        new_envelope("OrderShipped", {})


def test_topic_names_follow_the_convention() -> None:
    assert topic_for("OrderPlaced") == "shop.orders.order-placed.v1"
    assert all(topic.startswith("shop.") and topic.endswith(".v1") for topic, _ in EVENTS.values())
