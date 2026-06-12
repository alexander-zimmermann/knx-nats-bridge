from __future__ import annotations

import asyncio
from typing import Any

import pytest
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js.errors import NoStreamResponseError

from knx_nats_bridge.config import Settings
from knx_nats_bridge.metrics import Metrics
from knx_nats_bridge.publisher import Publisher


class FakeJetStream:
    """Records publishes; raises the queued exceptions first, then succeeds."""

    def __init__(self, errors: list[Exception] | None = None) -> None:
        self.errors = list(errors or [])
        self.published: list[tuple[str, bytes]] = []

    async def publish(self, subject: str, body: bytes, **_kwargs: Any) -> None:
        if self.errors:
            raise self.errors.pop(0)
        self.published.append((subject, body))


def _publisher() -> tuple[Publisher, Metrics]:
    metrics = Metrics()
    publisher = Publisher(Settings(knx_gateway_host="192.0.2.10"), metrics)
    return publisher, metrics


def _event(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ga": "1/2/3",
        "name": "Licht Flur",
        "dpt": "1.001",
        "value": True,
        "ts": "2026-01-01T00:00:00.000000Z",
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_publish_event_retries_timeouts_then_succeeds() -> None:
    publisher, metrics = _publisher()
    js = FakeJetStream(errors=[NATSTimeoutError(), NATSTimeoutError()])
    publisher._js = js  # type: ignore[assignment]

    assert await publisher.publish_event("knx.1.2.3", _event()) is True
    assert len(js.published) == 1
    assert metrics.publish_errors.labels(reason="timeout")._value.get() == 2
    assert metrics.telegrams_published._value.get() == 1


@pytest.mark.asyncio
async def test_publish_event_gives_up_after_three_timeouts() -> None:
    publisher, metrics = _publisher()
    publisher._js = FakeJetStream(errors=[NATSTimeoutError()] * 3)  # type: ignore[assignment]

    assert await publisher.publish_event("knx.1.2.3", _event()) is False
    assert metrics.publish_errors.labels(reason="timeout")._value.get() == 3


@pytest.mark.asyncio
async def test_no_stream_fails_fast_without_retry() -> None:
    publisher, metrics = _publisher()
    js = FakeJetStream(errors=[NoStreamResponseError()])
    publisher._js = js  # type: ignore[assignment]

    assert await publisher.publish_event("knx.1.2.3", _event()) is False
    assert js.published == []
    assert metrics.publish_errors.labels(reason="no_stream")._value.get() == 1


@pytest.mark.asyncio
async def test_schema_violation_is_rejected_before_publish() -> None:
    publisher, metrics = _publisher()
    js = FakeJetStream()
    publisher._js = js  # type: ignore[assignment]

    # Object values are not allowed by the event schema.
    assert await publisher.publish_event("knx.1.2.3", _event(value={"nested": 1})) is False
    assert js.published == []
    assert metrics.publish_errors.labels(reason="schema")._value.get() == 1


@pytest.mark.asyncio
async def test_raw_array_value_passes_schema() -> None:
    # RAW unmapped policy emits list[int] values; the schema must accept them.
    publisher, _ = _publisher()
    js = FakeJetStream()
    publisher._js = js  # type: ignore[assignment]

    assert await publisher.publish_event("knx.7.7.7", _event(dpt="0.000", value=[12, 26])) is True
    assert len(js.published) == 1


@pytest.mark.asyncio
async def test_enqueue_reports_queue_full() -> None:
    publisher, metrics = _publisher()
    while not publisher._queue.full():
        publisher._queue.put_nowait(("knx.0.0.0", {}))

    assert publisher.enqueue("knx.1.2.3", _event()) is False
    assert metrics.publish_errors.labels(reason="queue_full")._value.get() == 1


@pytest.mark.asyncio
async def test_worker_drains_queue_in_order() -> None:
    publisher, _ = _publisher()
    js = FakeJetStream()
    publisher._js = js  # type: ignore[assignment]

    for sub in ("knx.1.1.1", "knx.1.1.2", "knx.1.1.3"):
        assert publisher.enqueue(sub, _event(ga="1/1/1")) is True

    worker = asyncio.create_task(publisher._drain_queue())
    await asyncio.wait_for(publisher._queue.join(), timeout=2.0)
    worker.cancel()

    assert [s for s, _ in js.published] == ["knx.1.1.1", "knx.1.1.2", "knx.1.1.3"]
