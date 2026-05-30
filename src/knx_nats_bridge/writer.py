"""NATS-to-KNX writer: subscribe to mapped subjects, decode payloads, write to the bus."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from xknx import XKNX
from xknx.dpt import DPTArray, DPTBase, DPTBinary
from xknx.telegram import Telegram
from xknx.telegram.address import GroupAddress
from xknx.telegram.apci import GroupValueWrite

from .config import Settings
from .metrics import Metrics
from .write_mapping import WriteMapping, WriteMappingTable, extract_value

logger = logging.getLogger(__name__)


class Writer:
    def __init__(
        self,
        settings: Settings,
        mappings: WriteMappingTable,
        xknx: XKNX,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._mappings = mappings
        self._xknx = xknx
        self._metrics = metrics
        self._nc: NatsClient | None = None
        self._subs: list[Subscription] = []

    @property
    def is_connected(self) -> bool:
        return bool(self._nc and self._nc.is_connected)

    async def start(self) -> None:
        if not len(self._mappings):
            logger.info("writer enabled but mapping table is empty — idle")
            return

        self._nc = NatsClient()
        kwargs = _nats_auth_kwargs(self._settings)
        kwargs.update(
            servers=self._settings.nats_servers_list,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            connect_timeout=10,
        )
        await self._nc.connect(**kwargs)
        logger.info(
            "writer connected to NATS: %s (%d subjects, %d mappings)",
            self._settings.nats_servers_list,
            len(self._mappings.subjects()),
            len(self._mappings),
        )

        for subject in self._mappings.subjects():
            sub = await self._nc.subscribe(subject, cb=self._on_message)
            self._subs.append(sub)
            logger.info("writer subscribed: %s", subject)

    async def stop(self) -> None:
        for sub in self._subs:
            try:
                await sub.unsubscribe()
            except Exception:
                logger.exception("error unsubscribing %s", sub.subject)
        self._subs.clear()
        if self._nc and self._nc.is_connected:
            try:
                await self._nc.drain()
            except Exception:
                logger.exception("error draining writer NATS client")

    async def _on_message(self, msg: Msg) -> None:
        subject = msg.subject
        start = time.monotonic()
        try:
            payload = json.loads(msg.data) if msg.data else {}
        except json.JSONDecodeError:
            self._metrics.knx_write_errors.labels(reason="bad_json").inc()
            logger.warning("writer: non-JSON message on %s", subject)
            return

        # One subject may fan out to several GAs (e.g. boiler_data mirrors
        # both burner-status and DHW-state); process each mapping in order.
        for mapping in self._mappings.for_subject(subject):
            await self._apply(mapping, payload)

        self._metrics.knx_write_duration.observe(time.monotonic() - start)

    async def _apply(self, mapping: WriteMapping, payload: dict[str, Any]) -> None:
        try:
            raw_value = extract_value(payload, mapping.payload_path)
        except (KeyError, ValueError) as exc:
            self._metrics.knx_write_errors.labels(reason="payload_path").inc()
            logger.warning(
                "writer: cannot extract %s from %s payload: %s",
                mapping.payload_path,
                mapping.subject,
                exc,
            )
            return

        try:
            dpt_payload = _encode_for_dpt(raw_value, mapping.dpt)
        except Exception as exc:
            self._metrics.knx_write_errors.labels(reason="dpt_encode").inc()
            logger.warning(
                "writer: cannot encode value %r for DPT %s (subject=%s, ga=%s): %s",
                raw_value,
                mapping.dpt,
                mapping.subject,
                mapping.ga,
                exc,
            )
            return

        telegram = Telegram(
            destination_address=GroupAddress(mapping.ga),
            payload=GroupValueWrite(dpt_payload),
        )
        try:
            await self._xknx.telegrams.put(telegram)
        except Exception:
            self._metrics.knx_writes.labels(
                subject=mapping.subject, ga=mapping.ga, outcome="error"
            ).inc()
            self._metrics.knx_write_errors.labels(reason="bus").inc()
            logger.exception("writer: bus write failed for ga=%s", mapping.ga)
            return

        self._metrics.knx_writes.labels(subject=mapping.subject, ga=mapping.ga, outcome="ok").inc()
        logger.debug(
            "writer: %s -> ga=%s dpt=%s value=%r",
            mapping.subject,
            mapping.ga,
            mapping.dpt,
            raw_value,
        )


def _encode_for_dpt(value: Any, dpt: str) -> DPTArray | DPTBinary:
    """Encode a Python value to a DPTArray/DPTBinary payload for the given DPT."""
    transcoder = DPTBase.parse_transcoder(dpt)
    if transcoder is None:
        raise ValueError(f"unknown DPT {dpt!r}")
    # DPT main 1.xxx is 1-bit boolean; xknx accepts bool/int. Anything truthy
    # becomes 1 to keep mappings tolerant ($.score returning 0/100 both work).
    main = int(dpt.split(".", 1)[0])
    if main == 1:
        value = bool(value)
    return transcoder.to_knx(value)


def _nats_auth_kwargs(settings: Settings) -> dict[str, Any]:
    """Build the auth subset of NatsClient.connect kwargs from settings.

    Precedence matches publisher.py: creds file > nkey seed file > user/password.
    Each form is mutually exclusive in nats-py.
    """
    kwargs: dict[str, Any] = {}
    if settings.nats_creds_file and settings.nats_creds_file.exists():
        kwargs["user_credentials"] = str(settings.nats_creds_file)
    elif settings.nats_nkey_seed_file and settings.nats_nkey_seed_file.exists():
        kwargs["nkeys_seed"] = str(settings.nats_nkey_seed_file)
    elif settings.nats_user:
        password = settings.read_nats_password()
        if password is None:
            raise RuntimeError("NATS_USER is set but NATS_USER_PASSWORD_FILE is missing or empty")
        kwargs["user"] = settings.nats_user
        kwargs["password"] = password
    return kwargs
