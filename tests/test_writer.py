from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js.errors import NotFoundError
from xknx.dpt import DPT2ByteFloat, DPTBinary
from xknx.telegram import Telegram
from xknx.telegram.address import GroupAddress
from xknx.telegram.apci import GroupValueRead, GroupValueResponse, GroupValueWrite

from knx_nats_bridge.metrics import Metrics
from knx_nats_bridge.writer import Writer, _encode_for_dpt
from knx_nats_bridge.writer_rules import WriterRule, WriterRules


class FakeTelegramQueue:
    def __init__(self) -> None:
        self.sent: list[Telegram] = []

    async def put(self, telegram: Telegram) -> None:
        self.sent.append(telegram)

    def put_nowait(self, telegram: Telegram) -> None:
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


def _suppressed(metrics: Metrics, subject: str, ga: str) -> float:
    return metrics.knx_writes.labels(subject=subject, ga=ga, outcome="suppressed")._value.get()


async def _feed(writer: Writer, subject: str, *values: Any) -> None:
    for v in values:
        # field name is irrelevant; the rules below all read "$.v"
        await writer._on_message(FakeMsg(subject, json.dumps({"v": v}).encode()))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_deadband_absolute_suppresses_small_passes_large() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("solaredge-1.powerflow", "15/4/0", "14.056", "$.v", min_delta=50)]
    )
    # 1000 writes (first), +30 suppressed (<50), 1090 writes (+90 vs last sent 1000)
    await _feed(writer, "solaredge-1.powerflow", 1000, 1030, 1090)
    assert len(xknx.telegrams.sent) == 2
    assert _suppressed(metrics, "solaredge-1.powerflow", "15/4/0") == 1


@pytest.mark.asyncio
async def test_min_delta_zero_suppresses_exact_duplicate() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("warp.evse.state", "15/6/0", "5.010", "$.v", min_delta=0)]
    )
    await _feed(writer, "warp.evse.state", 2, 2, 3)  # write, suppressed (equal), write
    assert len(xknx.telegrams.sent) == 2
    assert _suppressed(metrics, "warp.evse.state", "15/6/0") == 1


@pytest.mark.asyncio
async def test_deadband_relative_band_dominates_at_high_value() -> None:
    writer, xknx, metrics = _writer(
        [
            WriterRule(
                "solaredge-1.powerflow", "15/4/0", "14.056", "$.v", min_delta=25, min_delta_pct=2
            )
        ]
    )
    # band at last=8000 is max(25, 160)=160: +100 suppressed, +200 writes
    await _feed(writer, "solaredge-1.powerflow", 8000, 8100, 8200)
    assert len(xknx.telegrams.sent) == 2
    assert _suppressed(metrics, "solaredge-1.powerflow", "15/4/0") == 1


@pytest.mark.asyncio
async def test_deadband_absolute_floor_dominates_at_low_value() -> None:
    writer, xknx, metrics = _writer(
        [
            WriterRule(
                "solaredge-1.powerflow", "15/4/0", "14.056", "$.v", min_delta=25, min_delta_pct=2
            )
        ]
    )
    # band at last=100 is max(25, 2)=25: +10 suppressed, +30 writes
    await _feed(writer, "solaredge-1.powerflow", 100, 110, 130)
    assert len(xknx.telegrams.sent) == 2
    assert _suppressed(metrics, "solaredge-1.powerflow", "15/4/0") == 1


@pytest.mark.asyncio
async def test_no_threshold_always_writes() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("solaredge-1.powerflow", "15/4/0", "14.056", "$.v")]
    )
    await _feed(writer, "solaredge-1.powerflow", 1000, 1000, 1000)
    assert len(xknx.telegrams.sent) == 3
    assert _suppressed(metrics, "solaredge-1.powerflow", "15/4/0") == 0


@pytest.mark.asyncio
async def test_deadband_non_numeric_falls_back_to_equality() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("unifi.events.fassade.person", "14/3/1", "1.005", "$.v", min_delta=1)]
    )
    await _feed(writer, "unifi.events.fassade.person", "x", "x", "y")  # write, suppress, write
    assert len(xknx.telegrams.sent) == 2
    assert _suppressed(metrics, "unifi.events.fassade.person", "14/3/1") == 1


@pytest.mark.asyncio
async def test_failed_write_does_not_update_deadband_baseline() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("solaredge-1.powerflow", "15/4/0", "14.056", "$.v", min_delta=50)]
    )

    async def boom(_: Telegram) -> None:
        raise RuntimeError("bus is down")

    xknx.telegrams.put = boom  # type: ignore[method-assign]
    await _feed(writer, "solaredge-1.powerflow", 1000)  # attempted, fails -> no baseline

    sent: list[Telegram] = []

    async def ok(t: Telegram) -> None:
        sent.append(t)

    xknx.telegrams.put = ok  # type: ignore[method-assign]
    # 1000 is "first seen" again (baseline never recorded), so it must write.
    await _feed(writer, "solaredge-1.powerflow", 1000)
    assert len(sent) == 1


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


# --- read responder -------------------------------------------------------


def _read(ga: str) -> Telegram:
    return Telegram(destination_address=GroupAddress(ga), payload=GroupValueRead())


def _responses(metrics: Metrics, ga: str, outcome: str) -> float:
    return metrics.knx_read_responses.labels(ga=ga, outcome=outcome)._value.get()


def test_read_responder_answers_with_cached_value() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("ems-esp.boiler_data_dhw", "15/2/17", "9.001", "$.wwseltemp")]
    )
    # Simulate a prior write so the responder has a last-known value to serve.
    writer._last_written["15/2/17"] = 50.0

    writer._on_read_request(_read("15/2/17"))

    [telegram] = xknx.telegrams.sent
    assert str(telegram.destination_address) == "15/2/17"
    assert isinstance(telegram.payload, GroupValueResponse)
    assert DPT2ByteFloat.from_knx(telegram.payload.value) == pytest.approx(50.0, abs=0.1)
    assert _responses(metrics, "15/2/17", "ok") == 1


def test_read_responder_stays_silent_without_cached_value() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("ems-esp.boiler_data_dhw", "15/2/17", "9.001", "$.wwseltemp")]
    )
    # No prior write -> nothing cached (e.g. right after a restart).
    writer._on_read_request(_read("15/2/17"))

    assert xknx.telegrams.sent == []
    assert _responses(metrics, "15/2/17", "no_value") == 1


def test_read_responder_ignores_foreign_ga() -> None:
    writer, xknx, _ = _writer(
        [WriterRule("ems-esp.boiler_data_dhw", "15/2/17", "9.001", "$.wwseltemp")]
    )
    writer._last_written["15/2/17"] = 50.0

    # A read for a GA the writer does not own must not be answered.
    writer._on_read_request(_read("0/0/1"))

    assert xknx.telegrams.sent == []


def test_read_responder_ignores_non_read_telegram() -> None:
    writer, xknx, _ = _writer(
        [WriterRule("ems-esp.boiler_data_dhw", "15/2/17", "9.001", "$.wwseltemp")]
    )
    writer._last_written["15/2/17"] = 50.0

    # A GroupValueWrite on an owned GA is not a read request -> ignore it.
    write = Telegram(
        destination_address=GroupAddress("15/2/17"),
        payload=GroupValueWrite(_encode_for_dpt(42.0, "9.001")),
    )
    writer._on_read_request(write)

    assert xknx.telegrams.sent == []


# --- startup seed (JetStream -> responder cache) ---------------------------


@dataclass
class FakeJSMsg:
    data: bytes
    acked: bool = False

    async def ack(self) -> None:
        self.acked = True


class FakePullSub:
    def __init__(self, msgs: list[FakeJSMsg], raise_on_fetch: Exception | None = None) -> None:
        self._msgs = msgs
        self._raise = raise_on_fetch
        self.unsubscribed = False

    async def fetch(self, batch: int = 1, **_kwargs: Any) -> list[FakeJSMsg]:
        if self._raise is not None:
            raise self._raise
        return self._msgs[:batch]

    async def unsubscribe(self) -> None:
        self.unsubscribed = True


class FakeJetStream:
    """Minimal pull_subscribe stub: maps subject -> last message / failure mode."""

    def __init__(
        self,
        last_by_subject: dict[str, bytes] | None = None,
        no_stream_subjects: set[str] | None = None,
        empty_subjects: set[str] | None = None,
    ) -> None:
        self.last_by_subject = last_by_subject or {}
        self.no_stream = no_stream_subjects or set()
        self.empty = empty_subjects or set()
        self.subs: list[FakePullSub] = []

    async def pull_subscribe(self, subject: str, **_kwargs: Any) -> FakePullSub:
        if subject in self.no_stream:
            raise NotFoundError
        if subject in self.empty:
            sub = FakePullSub([], raise_on_fetch=NATSTimeoutError())
        else:
            data = self.last_by_subject.get(subject)
            sub = FakePullSub([FakeJSMsg(data)] if data is not None else [])
        self.subs.append(sub)
        return sub


def _seed(metrics: Metrics, subject: str, outcome: str) -> float:
    return metrics.knx_seed.labels(subject=subject, outcome=outcome)._value.get()


@pytest.mark.asyncio
async def test_seed_populates_cache_without_bus_write() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("warp.evse.state", "15/6/0", "5.010", "$.error_state", seed_on_start=True)]
    )
    writer._js = FakeJetStream(  # type: ignore[assignment]
        last_by_subject={"warp.evse.state": json.dumps({"error_state": 0}).encode()}
    )

    await writer._seed_last_written()

    # Cache primed, but nothing written to the bus (cache-only seeding).
    assert writer._last_written == {"15/6/0": 0}
    assert xknx.telegrams.sent == []
    assert _seed(metrics, "warp.evse.state", "ok") == 1
    # The responder now answers a read that would have been silent after a restart.
    writer._on_read_request(_read("15/6/0"))
    [telegram] = xknx.telegrams.sent
    assert isinstance(telegram.payload, GroupValueResponse)


@pytest.mark.asyncio
async def test_seed_skips_subject_without_stream() -> None:
    writer, xknx, metrics = _writer(
        [WriterRule("warp.evse.state", "15/6/0", "5.010", "$.error_state", seed_on_start=True)]
    )
    writer._js = FakeJetStream(no_stream_subjects={"warp.evse.state"})  # type: ignore[assignment]

    await writer._seed_last_written()

    assert writer._last_written == {}
    assert xknx.telegrams.sent == []
    assert _seed(metrics, "warp.evse.state", "no_stream") == 1


@pytest.mark.asyncio
async def test_seed_handles_empty_stream() -> None:
    writer, _, metrics = _writer(
        [WriterRule("warp.evse.state", "15/6/0", "5.010", "$.error_state", seed_on_start=True)]
    )
    writer._js = FakeJetStream(empty_subjects={"warp.evse.state"})  # type: ignore[assignment]

    await writer._seed_last_written()

    assert writer._last_written == {}
    assert _seed(metrics, "warp.evse.state", "no_message") == 1


@pytest.mark.asyncio
async def test_seed_only_touches_flagged_rules() -> None:
    writer, _, _ = _writer(
        [
            WriterRule("warp.evse.state", "15/6/0", "5.010", "$.error_state", seed_on_start=True),
            WriterRule("warp.evse.state", "15/6/2", "5.010", "$.charger_state"),
        ]
    )
    writer._js = FakeJetStream(  # type: ignore[assignment]
        last_by_subject={
            "warp.evse.state": json.dumps({"error_state": 0, "charger_state": 2}).encode()
        }
    )

    await writer._seed_last_written()

    # Only the seed_on_start rule is cached; the unflagged sibling GA is not.
    assert writer._last_written == {"15/6/0": 0}


@pytest.mark.asyncio
async def test_seed_noop_when_no_subjects_flagged() -> None:
    writer, _, _ = _writer([WriterRule("warp.evse.state", "15/6/0", "5.010", "$.error_state")])
    js = FakeJetStream(last_by_subject={"warp.evse.state": b"{}"})
    writer._js = js  # type: ignore[assignment]

    await writer._seed_last_written()

    assert writer._last_written == {}
    assert js.subs == []  # no JetStream call at all
