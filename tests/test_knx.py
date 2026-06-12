from __future__ import annotations

from typing import Any

import pytest
from xknx.core import XknxConnectionState
from xknx.dpt import DPT2ByteFloat, DPTArray, DPTBinary
from xknx.telegram import Telegram
from xknx.telegram.address import GroupAddress
from xknx.telegram.apci import GroupValueRead, GroupValueWrite

from knx_nats_bridge.config import Settings, UnmappedPolicy
from knx_nats_bridge.knx import KnxListener, _decode, _jsonable
from knx_nats_bridge.mapping import GAEntry, GroupAddressMapping
from knx_nats_bridge.metrics import Metrics


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def enqueue(self, subject: str, payload: dict[str, Any]) -> bool:
        self.events.append((subject, payload))
        return True


class FakeConnectionManager:
    def __init__(self, state: XknxConnectionState) -> None:
        self.state = state


class FakeXknx:
    def __init__(self, state: XknxConnectionState) -> None:
        self.connection_manager = FakeConnectionManager(state)


def _settings(**overrides: Any) -> Settings:
    overrides.setdefault("knx_gateway_host", "192.0.2.10")
    return Settings(**overrides)


def _listener(
    entries: dict[str, GAEntry] | None = None,
    policy: UnmappedPolicy = UnmappedPolicy.SKIP,
) -> tuple[KnxListener, FakePublisher, Metrics]:
    settings = _settings(knx_nats_unmapped_policy=policy)
    mapping = GroupAddressMapping(entries or {})
    publisher = FakePublisher()
    metrics = Metrics()
    listener = KnxListener(settings, mapping, publisher, metrics)  # type: ignore[arg-type]
    return listener, publisher, metrics


def _write_telegram(ga: str, value: DPTArray | DPTBinary) -> Telegram:
    return Telegram(destination_address=GroupAddress(ga), payload=GroupValueWrite(value))


def test_connected_is_false_for_disconnected_state() -> None:
    # Regression: "CONNECTED" is a substring of "DISCONNECTED"; a substring
    # check made the liveness probe (and the gauge) report a dead tunnel as up.
    listener, _, _ = _listener()
    listener._xknx = FakeXknx(XknxConnectionState.DISCONNECTED)  # type: ignore[assignment]
    assert listener.connected is False


def test_connected_is_true_for_connected_state() -> None:
    listener, _, _ = _listener()
    listener._xknx = FakeXknx(XknxConnectionState.CONNECTED)  # type: ignore[assignment]
    assert listener.connected is True


def test_on_state_gauge_tracks_enum_not_substring() -> None:
    listener, _, metrics = _listener()
    listener._on_state(XknxConnectionState.CONNECTED)
    assert metrics.tunnel_connected._value.get() == 1
    listener._on_state(XknxConnectionState.DISCONNECTED)
    assert metrics.tunnel_connected._value.get() == 0


def test_mapped_telegram_is_enqueued() -> None:
    listener, publisher, metrics = _listener({"1/2/3": GAEntry(name="Licht Flur", dpt="1.001")})
    listener._on_telegram(_write_telegram("1/2/3", DPTBinary(1)))

    [(subject, payload)] = publisher.events
    assert subject == "knx.1.2.3"
    assert payload["ga"] == "1/2/3"
    assert payload["name"] == "Licht Flur"
    assert payload["dpt"] == "1.001"
    assert payload["value"] is True
    assert payload["ts"].endswith("Z")
    assert metrics.telegrams_received._value.get() == 1


def test_unmapped_skip_policy_drops_event() -> None:
    listener, publisher, metrics = _listener()
    listener._on_telegram(_write_telegram("7/7/7", DPTBinary(1)))

    assert publisher.events == []
    assert metrics.telegrams_unmapped._value.get() == 1


def test_unmapped_raw_policy_publishes_synthetic_entry() -> None:
    listener, publisher, _ = _listener(policy=UnmappedPolicy.RAW)
    listener._on_telegram(_write_telegram("7/7/7", DPTArray((0x0C, 0x1A))))

    [(subject, payload)] = publisher.events
    assert subject == "knx.7.7.7"
    assert payload["name"] == "7/7/7"
    assert payload["dpt"] == "0.000"
    assert payload["value"] == [0x0C, 0x1A]


def test_group_value_read_is_ignored() -> None:
    listener, publisher, metrics = _listener({"1/2/3": GAEntry(name="Licht Flur", dpt="1.001")})
    telegram = Telegram(destination_address=GroupAddress("1/2/3"), payload=GroupValueRead())
    listener._on_telegram(telegram)

    assert publisher.events == []
    assert metrics.telegrams_received._value.get() == 0


def test_decode_known_dpt_returns_float() -> None:
    raw = DPT2ByteFloat.to_knx(21.5)
    assert _decode(raw, "9.001") == pytest.approx(21.5, abs=0.1)


def test_decode_unknown_dpt_returns_raw_representation() -> None:
    assert _decode(DPTBinary(1), "0.000") == 1
    assert _decode(DPTArray((1, 2)), "0.000") == [1, 2]


def test_jsonable_collapses_tuples_and_value_wrappers() -> None:
    class Wraps:
        value = 42

    assert _jsonable((1, 2)) == [1, 2]
    assert _jsonable(Wraps()) == 42
    assert _jsonable({"k": (True, None)}) == {"k": [True, None]}
