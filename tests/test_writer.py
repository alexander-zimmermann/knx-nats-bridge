from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
from xknx.dpt import DPT2ByteFloat, DPTBinary
from xknx.telegram import Telegram
from xknx.telegram.apci import GroupValueWrite

from knx_nats_bridge.metrics import Metrics
from knx_nats_bridge.writer import Writer, _encode_for_dpt
from knx_nats_bridge.writer_rules import WriterRule, WriterRules


class FakeTelegramQueue:
    def __init__(self) -> None:
        self.sent: list[Telegram] = []

    async def put(self, telegram: Telegram) -> None:
        self.sent.append(telegram)


@dataclass
class FakeXknx:
    telegrams: FakeTelegramQueue


@dataclass
class FakeMsg:
    subject: str
    data: bytes


def _writer(mappings: list[WriterRule]) -> tuple[Writer, FakeXknx, Metrics]:
    settings: Any = object()  # _on_message and _apply don't touch settings
    metrics = Metrics()
    xknx = FakeXknx(telegrams=FakeTelegramQueue())
    table = WriterRules(mappings)
    writer = Writer(settings, table, xknx, metrics)  # type: ignore[arg-type]
    return writer, xknx, metrics


@pytest.mark.asyncio
async def test_binary_mapping_writes_true() -> None:
    writer, xknx, _ = _writer(
        [WriterRule("ems-esp.boiler_data", "15/2/1", "1.001", "$.burnstart_active")]
    )
    msg = FakeMsg("ems-esp.boiler_data", json.dumps({"burnstart_active": True}).encode())
    await writer._on_message(msg)  # type: ignore[arg-type]

    assert len(xknx.telegrams.sent) == 1
    telegram = xknx.telegrams.sent[0]
    assert str(telegram.destination_address) == "15/2/1"
    assert isinstance(telegram.payload, GroupValueWrite)
    assert isinstance(telegram.payload.value, DPTBinary)
    assert telegram.payload.value.value == 1


@pytest.mark.asyncio
async def test_truthy_int_score_writes_true() -> None:
    writer, xknx, _ = _writer(
        [WriterRule("unifi.events.fassade.person", "14/3/1", "1.005", "$.score")]
    )
    msg = FakeMsg("unifi.events.fassade.person", json.dumps({"score": 84}).encode())
    await writer._on_message(msg)  # type: ignore[arg-type]

    [telegram] = xknx.telegrams.sent
    assert isinstance(telegram.payload, GroupValueWrite)
    assert isinstance(telegram.payload.value, DPTBinary)
    assert telegram.payload.value.value == 1


@pytest.mark.asyncio
async def test_zero_score_writes_false() -> None:
    writer, xknx, _ = _writer(
        [WriterRule("unifi.events.fassade.motion", "14/3/2", "1.005", "$.score")]
    )
    msg = FakeMsg("unifi.events.fassade.motion", json.dumps({"score": 0}).encode())
    await writer._on_message(msg)  # type: ignore[arg-type]

    [telegram] = xknx.telegrams.sent
    assert isinstance(telegram.payload, GroupValueWrite)
    assert isinstance(telegram.payload.value, DPTBinary)
    assert telegram.payload.value.value == 0


@pytest.mark.asyncio
async def test_fan_out_one_subject_two_gas() -> None:
    writer, xknx, _ = _writer(
        [
            WriterRule("ems-esp.boiler_data", "15/2/1", "1.001", "$.burnstart_active"),
            WriterRule("ems-esp.boiler_data", "15/2/2", "9.001", "$.curflowtemp"),
        ]
    )
    msg = FakeMsg(
        "ems-esp.boiler_data",
        json.dumps({"burnstart_active": True, "curflowtemp": 55.5}).encode(),
    )
    await writer._on_message(msg)  # type: ignore[arg-type]

    assert [str(t.destination_address) for t in xknx.telegrams.sent] == ["15/2/1", "15/2/2"]


@pytest.mark.asyncio
async def test_missing_field_increments_error_metric_and_skips_write() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("ems-esp.boiler_data", "15/2/1", "1.001", "$.missing_field")]
    )
    msg = FakeMsg("ems-esp.boiler_data", json.dumps({"other": 1}).encode())
    await writer._on_message(msg)  # type: ignore[arg-type]

    assert xknx.telegrams.sent == []
    assert metrics.knx_write_errors.labels(reason="payload_path")._value.get() == 1


@pytest.mark.asyncio
async def test_bad_json_increments_error_metric_and_skips_all() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("ems-esp.boiler_data", "15/2/1", "1.001", "$.burnstart_active")]
    )
    msg = FakeMsg("ems-esp.boiler_data", b"not-json")
    await writer._on_message(msg)  # type: ignore[arg-type]

    assert xknx.telegrams.sent == []
    assert metrics.knx_write_errors.labels(reason="bad_json")._value.get() == 1


@pytest.mark.asyncio
async def test_bus_put_failure_recorded_as_error() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("ems-esp.boiler_data", "15/2/1", "1.001", "$.burnstart_active")]
    )

    async def boom(_: Telegram) -> None:
        raise RuntimeError("bus is down")

    xknx.telegrams.put = boom  # type: ignore[method-assign]

    msg = FakeMsg("ems-esp.boiler_data", json.dumps({"burnstart_active": True}).encode())
    await writer._on_message(msg)  # type: ignore[arg-type]

    assert (
        metrics.knx_writes.labels(
            subject="ems-esp.boiler_data", ga="15/2/1", outcome="error"
        )._value.get()
        == 1
    )
    assert metrics.knx_write_errors.labels(reason="bus")._value.get() == 1


def test_encode_dpt_1_coerces_truthy() -> None:
    assert isinstance(_encode_for_dpt(True, "1.001"), DPTBinary)
    assert isinstance(_encode_for_dpt(0, "1.001"), DPTBinary)
    assert isinstance(_encode_for_dpt("nonempty", "1.001"), DPTBinary)


def test_encode_dpt_9_passes_float() -> None:
    payload = _encode_for_dpt(21.5, "9.001")
    # 9.001 is 2-byte float; ensure it round-trips through xknx.
    assert DPT2ByteFloat.from_knx(payload) == pytest.approx(21.5, abs=0.1)


def test_encode_unknown_dpt_raises() -> None:
    with pytest.raises(ValueError, match="unknown DPT"):
        _encode_for_dpt(1, "999.999")
