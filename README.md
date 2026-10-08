# contracts

The shared contracts of the shop, as the Python package `shop_contracts`:

* `schemas/events/`: one JSON Schema per event type and version. These files are the contract.
* `events.py`: which topic and schema belong to each event type, the envelope every event travels in (`new_envelope`), and `validate_event`.
* `messaging.py`: publishing to Kafka, consuming from it, and an in-memory publisher for tests. Both publishers refuse an event that breaks its schema.

The services depend on this package by path (`../contracts`), so a change here reaches them at once. How to change an event without breaking anyone: see `VERSIONING.md`.

## Test it

    uv run pytest
    uv run ruff check . && uv run ruff format --check .
    uv run mypy src
