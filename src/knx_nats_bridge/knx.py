"""KNX tunnel listener: decode telegrams via DPT, build JSON event, hand off to publisher."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

from xknx import XKNX
from xknx.dpt import DPTArray, DPTBase, DPTBinary
from xknx.io import ConnectionConfig, ConnectionType
from xknx.telegram import Telegram
from xknx.telegram.address import GroupAddress
from xknx.telegram.apci import GroupValueResponse, GroupValueWrite

from .config import ConnectionType as CfgConnectionType
from .config import Settings, UnmappedPolicy
from .mapping import GAEntry, GroupAddressMapping
from .metrics import Metrics
from .publisher import Publisher

logger = logging.getLogger(__name__)


class KnxListener:
    def __init__(
        self,
        settings: Settings,
        mapping: GroupAddressMapping,
        publisher: Publisher,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._mapping = mapping
        self._publisher = publisher
        self._metrics = metrics
        self._xknx: XKNX | None = None
        # Hold strong references to in-flight publish tasks so they aren't
        # garbage-collected mid-await (asyncio docs §asyncio.create_task).
        self._publish_tasks: set[asyncio.Task[bool]] = set()

    @property
    def xknx(self) -> XKNX | None:
        return self._xknx

    @property
    def connected(self) -> bool:
        x = self._xknx
        if x is None:
            return False
        try:
            state = x.connection_manager.state
            name = getattr(state, "name", str(state)).upper()
        except Exception:
            return False
        return "CONNECTED" in name

    async def start(self) -> None:
        cfg = self._build_connection_config()
        self._xknx = XKNX(connection_config=cfg)
        self._xknx.telegram_queue.register_telegram_received_cb(self._on_telegram)

        # State hooks for the tunnel_connected gauge.
        self._xknx.connection_manager.register_connection_state_changed_cb(self._on_state)

        logger.info("starting xknx (connection_type=%s)", self._settings.knx_connection_type.value)
        await self._xknx.start()
        self._metrics.tunnel_connected.set(1)

    async def stop(self) -> None:
        if self._xknx is not None:
            try:
                await self._xknx.stop()
            finally:
                self._metrics.tunnel_connected.set(0)

    def _build_connection_config(self) -> ConnectionConfig:
        t = self._settings.knx_connection_type
        if t == CfgConnectionType.TUNNELING_TCP:
            return ConnectionConfig(
                connection_type=ConnectionType.TUNNELING_TCP,
                gateway_ip=self._settings.knx_gateway_host or "",
                gateway_port=self._settings.knx_gateway_port,
            )
        if t == CfgConnectionType.TUNNELING_UDP:
            return ConnectionConfig(
                connection_type=ConnectionType.TUNNELING,
                gateway_ip=self._settings.knx_gateway_host or "",
                gateway_port=self._settings.knx_gateway_port,
                local_ip=self._settings.knx_local_ip,
            )
        # routing
        return ConnectionConfig(
            connection_type=ConnectionType.ROUTING,
            local_ip=self._settings.knx_local_ip,
        )

    def _on_state(self, state: Any) -> None:
        # xknx invokes connection-state callbacks synchronously.
        name = getattr(state, "name", str(state)).upper()
        self._metrics.tunnel_connected.set(1 if "CONNECTED" in name else 0)
        logger.info("knx tunnel state: %s", name)

    def _on_telegram(self, telegram: Telegram) -> None:
        # Only GroupValue Write/Response carry an updated value worth publishing.
        # Read requests (GroupValueRead) and other APCI types are skipped silently.
        apci = telegram.payload
        if not isinstance(apci, (GroupValueWrite, GroupValueResponse)):
            return

        dest = telegram.destination_address
        if not isinstance(dest, GroupAddress):
            return

        ga_str = str(dest)  # "a/b/c" for 3-level GroupAddress style
        parts = ga_str.split("/")
        if len(parts) != 3:
            return

        entry = self._mapping.get(ga_str)
        if entry is None:
            policy = self._settings.knx_nats_unmapped_policy
            if policy == UnmappedPolicy.SKIP:
                return
            if policy == UnmappedPolicy.WARN:
                logger.warning("unmapped GA: %s (skipping)", ga_str)
                return
            # RAW: publish with a synthetic name/dpt so downstream can still see it.
            entry = GAEntry(name=ga_str, dpt="0.000")

        dpt_value: Any
        try:
            dpt_value = _decode(apci.value, entry.dpt)
        except Exception as exc:
            self._metrics.publish_errors.labels(reason="other").inc()
            logger.warning("DPT decode failed for %s dpt=%s: %s", ga_str, entry.dpt, exc)
            return

        self._metrics.telegrams_received.labels(dpt=entry.dpt).inc()
        self._metrics.last_telegram_ts.set(time.time())

        prefix = self._settings.nats_subject_prefix
        subject = f"{prefix}.{parts[0]}.{parts[1]}.{parts[2]}"

        payload = {
            "ga": ga_str,
            "name": entry.name,
            "dpt": entry.dpt,
            "value": dpt_value,
            "ts": _now_rfc3339_micros(),
        }
        # Sync callback context: schedule the async publish on the running loop.
        # Errors are logged inside publish_event; we don't block the bus thread.
        task = asyncio.create_task(self._publisher.publish_event(subject, payload))
        self._publish_tasks.add(task)
        task.add_done_callback(self._publish_tasks.discard)


def _decode(raw_value: Any, dpt: str) -> Any:
    """Decode a KNX payload value using xknx's DPT transcoders.

    raw_value is typically DPTBinary (1-bit up to 6-bit) or DPTArray (multi-byte).
    If the DPT is unknown (e.g. RAW policy for unmapped GAs), return a raw
    representation: int for DPTBinary, list[int] for DPTArray.
    """
    transcoder = DPTBase.parse_transcoder(dpt)
    if transcoder is None:
        if isinstance(raw_value, DPTBinary):
            return int(raw_value.value)
        if isinstance(raw_value, DPTArray):
            return list(raw_value.value)
        return None
    value = transcoder.from_knx(raw_value)
    return _jsonable(value)


def _jsonable(value: Any) -> Any:
    # xknx DPTs sometimes return tuples, dataclasses, enums. Collapse to JSON-native.
    if hasattr(value, "value"):
        return _jsonable(value.value)
    if isinstance(value, (bool, int, float, str)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


def _now_rfc3339_micros() -> str:
    # Python datetime gives microsecond precision; RFC3339 allows arbitrary
    # fractional digit count. Downstream can widen to nanoseconds if needed.
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
