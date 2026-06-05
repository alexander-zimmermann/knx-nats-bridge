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
from .writer_rules import WriterRule, WriterRules, extract_value

logger = logging.getLogger(__name__)


class Writer:
    def __init__(
        self,
        settings: Settings,
        rules: WriterRules,
        xknx: XKNX,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._rules = rules
        self._xknx = xknx
        self._metrics = metrics
        self._nc: NatsClient | None = None
        self._subs: list[Subscription] = []
        # Last value written per GA, for the deadband filter (_should_write).
        # Empty on start so every GA gets one fresh write after a restart.
        self._last_written: dict[str, Any] = {}

    @property
    def is_connected(self) -> bool:
        return bool(self._nc and self._nc.is_connected)

    async def start(self) -> None:
        if not len(self._rules):
            logger.info("writer enabled but rules table is empty — idle")
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
            "writer connected to NATS: %s (%d subjects, %d rules)",
            self._settings.nats_servers_list,
            len(self._rules.subjects()),
            len(self._rules),
        )

        for subject in self._rules.subjects():
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
        # both burner-status and DHW-state); process each rule in order.
        for rule in self._rules.for_subject(subject):
            await self._apply(rule, payload)

        self._metrics.knx_write_duration.observe(time.monotonic() - start)

    async def _apply(self, rule: WriterRule, payload: dict[str, Any]) -> None:
        try:
            raw_value = extract_value(payload, rule.payload_path)
        except (KeyError, ValueError) as exc:
            self._metrics.knx_write_errors.labels(reason="payload_path").inc()
            logger.warning(
                "writer: cannot extract %s from %s payload: %s",
                rule.payload_path,
                rule.subject,
                exc,
            )
            return

        # Deadband barrier: drop bus-spamming jitter before encoding/sending.
        if not self._should_write(rule, raw_value):
            self._metrics.knx_writes.labels(
                subject=rule.subject, ga=rule.ga, outcome="suppressed"
            ).inc()
            logger.debug(
                "writer: suppressed (deadband) %s -> ga=%s value=%r",
                rule.subject,
                rule.ga,
                raw_value,
            )
            return

        try:
            dpt_payload = _encode_for_dpt(raw_value, rule.dpt)
        except Exception as exc:
            self._metrics.knx_write_errors.labels(reason="dpt_encode").inc()
            logger.warning(
                "writer: cannot encode value %r for DPT %s (subject=%s, ga=%s): %s",
                raw_value,
                rule.dpt,
                rule.subject,
                rule.ga,
                exc,
            )
            return

        telegram = Telegram(
            destination_address=GroupAddress(rule.ga),
            payload=GroupValueWrite(dpt_payload),
        )
        try:
            await self._xknx.telegrams.put(telegram)
        except Exception:
            self._metrics.knx_writes.labels(subject=rule.subject, ga=rule.ga, outcome="error").inc()
            self._metrics.knx_write_errors.labels(reason="bus").inc()
            logger.exception("writer: bus write failed for ga=%s", rule.ga)
            return

        # Record only after a successful send so the deadband measures against
        # the last value actually on the bus (a failed write retries next time).
        self._last_written[rule.ga] = raw_value
        self._metrics.knx_writes.labels(subject=rule.subject, ga=rule.ga, outcome="ok").inc()
        logger.debug(
            "writer: %s -> ga=%s dpt=%s value=%r",
            rule.subject,
            rule.ga,
            rule.dpt,
            raw_value,
        )

    def _should_write(self, rule: WriterRule, new: Any) -> bool:
        """Deadband: decide whether `new` differs enough from the last write.

        - First value per GA always writes (fresh state after restart).
        - No thresholds configured -> always write (cyclic-refresh semantics).
        - Numeric value -> write only when it moved past
          max(min_delta, min_delta_pct/100 * |last|). Comparing against the
          last *sent* value means slow drift still eventually crosses the band.
        - bool / non-numeric -> plain change check.
        """
        if rule.ga not in self._last_written:
            return True
        if rule.min_delta is None and rule.min_delta_pct is None:
            return True
        last = self._last_written[rule.ga]
        if _is_number(new) and _is_number(last):
            band = max(rule.min_delta or 0.0, (rule.min_delta_pct or 0.0) / 100.0 * abs(last))
            return abs(new - last) > band
        return new != last


def _is_number(value: Any) -> bool:
    """True for int/float but not bool (bool is an int subclass in Python)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


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
