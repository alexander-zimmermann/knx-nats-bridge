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
from xknx.telegram.apci import GroupValueRead, GroupValueResponse, GroupValueWrite

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
        # GA -> rule index for the read responder: the first rule per GA supplies
        # the DPT used to encode the cached value into a GroupValueResponse.
        self._rule_by_ga: dict[str, WriterRule] = {}
        for r in rules:
            self._rule_by_ga.setdefault(r.ga, r)
        # Handle for the responder's telegram callback, so stop() can unregister it.
        self._read_cb: Any = None

    @property
    def is_connected(self) -> bool:
        return bool(self._nc and self._nc.is_connected)

    @property
    def has_rules(self) -> bool:
        return len(self._rules) > 0

    async def start(self) -> None:
        if not self.has_rules:
            logger.info("writer enabled but rules table is empty — idle")
            return

        self._nc = NatsClient()
        kwargs = self._settings.nats_auth_kwargs()
        kwargs.update(
            servers=self._settings.nats_servers_list,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            connect_timeout=10,
            disconnected_cb=self._on_disconnect,
            reconnected_cb=self._on_reconnect,
        )
        await self._nc.connect(**kwargs)
        self._metrics.writer_nats_connected.set(1)
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

        if self._settings.bridge_read_responder_enabled:
            # Answer GroupValueRead for the GAs we write. Filter to those GAs so
            # the callback never fires for foreign traffic. match_for_outgoing is
            # False: we react only to reads from the bus, never to our own response.
            self._read_cb = self._xknx.telegram_queue.register_telegram_received_cb(
                self._on_read_request,
                group_addresses=[GroupAddress(ga) for ga in self._rule_by_ga],
                match_for_outgoing=False,
            )
            logger.info("read responder enabled for %d group addresses", len(self._rule_by_ga))

    async def stop(self) -> None:
        if self._read_cb is not None:
            try:
                self._xknx.telegram_queue.unregister_telegram_received_cb(self._read_cb)
            except Exception:
                logger.exception("error unregistering read-responder callback")
            self._read_cb = None
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
        self._metrics.writer_nats_connected.set(0)

    async def _on_disconnect(self) -> None:
        self._metrics.writer_nats_connected.set(0)
        logger.warning("writer nats disconnected")

    async def _on_reconnect(self) -> None:
        self._metrics.writer_nats_connected.set(1)
        logger.info("writer nats reconnected")

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

    def _on_read_request(self, telegram: Telegram) -> None:
        """Answer a GroupValueRead for a written GA with the last value put on the bus.

        Sync callback (xknx invokes telegram_received_cbs synchronously). Sending
        uses put_nowait on the unbounded telegram queue, so no await is needed; the
        outgoing GroupValueResponse is still paced by the bus rate limit.
        """
        if not isinstance(telegram.payload, GroupValueRead):
            return
        dest = telegram.destination_address
        if not isinstance(dest, GroupAddress):
            return
        ga = str(dest)
        rule = self._rule_by_ga.get(ga)
        if rule is None:
            return  # not a GA we own; the group_addresses filter should exclude it
        if ga not in self._last_written:
            # No value cached yet (e.g. right after a restart, before the first
            # write for this GA). Stay silent rather than answer with a guess.
            self._metrics.knx_read_responses.labels(ga=ga, outcome="no_value").inc()
            logger.debug("read responder: no cached value for ga=%s yet", ga)
            return
        try:
            dpt_payload = _encode_for_dpt(self._last_written[ga], rule.dpt)
        except Exception:
            self._metrics.knx_read_responses.labels(ga=ga, outcome="error").inc()
            logger.exception("read responder: cannot encode cached value for ga=%s", ga)
            return
        response = Telegram(destination_address=dest, payload=GroupValueResponse(dpt_payload))
        try:
            self._xknx.telegrams.put_nowait(response)
        except Exception:
            self._metrics.knx_read_responses.labels(ga=ga, outcome="error").inc()
            logger.exception("read responder: failed to enqueue response for ga=%s", ga)
            return
        self._metrics.knx_read_responses.labels(ga=ga, outcome="ok").inc()
        logger.debug("read responder: answered ga=%s value=%r", ga, self._last_written[ga])

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
            return bool(abs(new - last) > band)
        return bool(new != last)


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
