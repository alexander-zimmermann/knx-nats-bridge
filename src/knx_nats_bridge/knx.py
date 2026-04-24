"""KNX tunnel listener: decode telegrams via DPT, build JSON event, hand off to publisher."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from xknx import XKNX
from xknx.dpt import DPTBase, DPTBinary
from xknx.io import ConnectionConfig, ConnectionType
from xknx.telegram import Telegram
from xknx.telegram.address import GroupAddress
from xknx.telegram.apci import GroupValueRead, GroupValueResponse, GroupValueWrite

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

    @property
    def xknx(self) -> XKNX | None:
        return self._xknx

    @property
    def connected(self) -> bool:
        x = self._xknx
        if x is None:
            return False
        try:
            return bool(x.knxip_interface is not None and getattr(x, "started", False))
        except Exception:
            return False

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

    async def _on_state(self, state: Any) -> None:
        # xknx passes an enum-like state; "CONNECTED" maps to 1.
        name = getattr(state, "name", str(state)).upper()
        self._metrics.tunnel_connected.set(1 if "CONNECTED" in name else 0)
        logger.info("knx tunnel state: %s", name)

    async def _on_telegram(self, telegram: Telegram) -> None:
        # We only care about GroupValue Write/Response on group addresses.
        apci = telegram.payload
        if not isinstance(apci, (GroupValueWrite, GroupValueResponse)):
            if isinstance(apci, GroupValueRead):
                return
            return

        dest = telegram.destination_address
        if not isinstance(dest, GroupAddress):
            return

        ga_str = dest.__str__()  # "a/b/c" for GroupAddress with 3-level style
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
            "ts": _now_rfc3339_nanos(),
        }
        await self._publisher.publish_event(subject, payload)


def _decode(raw_value: Any, dpt: str) -> Any:
    """Decode a KNX payload value using xknx's DPT transcoders.

    raw_value is typically DPTBinary (1-bit) or DPTArray (multi-byte).
    """
    # 1-bit DPTs arrive as DPTBinary; decode to bool for clean JSON semantics.
    if isinstance(raw_value, DPTBinary):
        if dpt.startswith("1."):
            return bool(raw_value.value)
        return int(raw_value.value)

    transcoder = DPTBase.parse_transcoder(dpt)
    if transcoder is None:
        raise ValueError(f"unknown DPT: {dpt}")
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


def _now_rfc3339_nanos() -> str:
    # datetime only gives microseconds; pad to 9 digits for nanosecond precision.
    now = datetime.now(UTC)
    base = now.strftime("%Y-%m-%dT%H:%M:%S")
    micros = now.microsecond
    return f"{base}.{micros:06d}000Z"
