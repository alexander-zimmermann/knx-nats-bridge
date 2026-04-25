"""NATS JetStream publisher: schema-validate, ack-publish with exponential-backoff retry."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import jsonschema
from nats.aio.client import Client as NatsClient
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js import JetStreamContext
from nats.js.errors import APIError, NoStreamResponseError

from .config import Settings
from .metrics import Metrics

logger = logging.getLogger(__name__)

_EVENT_SCHEMA_PATH = Path(__file__).resolve().parent / "_schemas" / "event.schema.json"


class Publisher:
    """NATS JetStream publisher with synchronous ack and retry."""

    def __init__(self, settings: Settings, metrics: Metrics) -> None:
        self._settings = settings
        self._metrics = metrics
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._schema: dict[str, Any] | None = None
        if _EVENT_SCHEMA_PATH.exists():
            self._schema = json.loads(_EVENT_SCHEMA_PATH.read_text(encoding="utf-8"))

    async def connect(self) -> None:
        if self._nc and self._nc.is_connected:
            return

        kwargs: dict[str, Any] = {
            "servers": self._settings.nats_servers_list,
            "max_reconnect_attempts": -1,
            "reconnect_time_wait": 2,
            "connect_timeout": 10,
            "disconnected_cb": self._on_disconnect,
            "reconnected_cb": self._on_reconnect,
            "closed_cb": self._on_closed,
            "error_cb": self._on_error,
        }

        # Auth precedence: creds file > nkey seed file > user/password.
        # Each form is mutually exclusive in nats-py; pick the first that's configured.
        if self._settings.nats_creds_file and self._settings.nats_creds_file.exists():
            kwargs["user_credentials"] = str(self._settings.nats_creds_file)
        elif self._settings.nats_nkey_seed_file and self._settings.nats_nkey_seed_file.exists():
            kwargs["nkeys_seed"] = str(self._settings.nats_nkey_seed_file)
        elif self._settings.nats_user:
            password = self._settings.read_nats_password()
            if password is None:
                raise RuntimeError(
                    "NATS_USER is set but NATS_USER_PASSWORD_FILE is missing or empty"
                )
            kwargs["user"] = self._settings.nats_user
            kwargs["password"] = password

        self._nc = NatsClient()
        await self._nc.connect(**kwargs)
        self._js = self._nc.jetstream()
        self._metrics.nats_connected.set(1)
        logger.info("connected to NATS: %s", self._settings.nats_servers_list)

        if self._settings.nats_stream_check:
            await self._verify_stream()

    async def _verify_stream(self) -> None:
        assert self._js is not None
        try:
            info = await self._js.stream_info(self._settings.nats_stream_name)
            logger.info(
                "jetstream stream ok: %s (subjects=%s, messages=%d)",
                info.config.name,
                info.config.subjects,
                info.state.messages,
            )
        except Exception as exc:
            logger.warning(
                "jetstream stream %r not reachable at startup: %s",
                self._settings.nats_stream_name,
                exc,
            )

    async def close(self) -> None:
        if self._nc and self._nc.is_connected:
            await self._nc.drain()
        self._metrics.nats_connected.set(0)

    @property
    def is_connected(self) -> bool:
        return bool(self._nc and self._nc.is_connected)

    async def publish_event(self, subject: str, payload: dict[str, Any]) -> bool:
        """Validate and publish one event, waiting for a JetStream ack.

        Returns True on success, False on a permanent failure after retries.
        """
        if self._schema is not None:
            try:
                jsonschema.validate(instance=payload, schema=self._schema)
            except jsonschema.ValidationError as exc:
                self._metrics.publish_errors.labels(reason="schema").inc()
                logger.error(
                    "payload failed schema validation: %s | payload=%s",
                    exc.message,
                    payload,
                )
                return False

        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        backoff = 0.1
        for attempt in range(1, 4):
            if not self._js:
                self._metrics.publish_errors.labels(reason="other").inc()
                return False
            try:
                await self._js.publish(subject, body, timeout=5.0)
                self._metrics.telegrams_published.inc()
                return True
            except NoStreamResponseError:
                self._metrics.publish_errors.labels(reason="no_stream").inc()
                logger.error("no stream matches subject %s (attempt %d)", subject, attempt)
                await asyncio.sleep(30)
                return False
            except NATSTimeoutError:
                self._metrics.publish_errors.labels(reason="timeout").inc()
                logger.warning("publish timeout for %s (attempt %d)", subject, attempt)
            except NoRespondersError:
                self._metrics.publish_errors.labels(reason="nak").inc()
                logger.warning("no responders for %s (attempt %d)", subject, attempt)
            except APIError as exc:
                self._metrics.publish_errors.labels(reason="nak").inc()
                logger.warning("jetstream api error for %s (attempt %d): %s", subject, attempt, exc)
            except Exception:
                self._metrics.publish_errors.labels(reason="other").inc()
                logger.exception("unexpected publish error for %s (attempt %d)", subject, attempt)

            if attempt < 3:
                await asyncio.sleep(backoff)
                backoff *= 2

        return False

    async def _on_disconnect(self) -> None:
        self._metrics.nats_connected.set(0)
        logger.warning("nats disconnected")

    async def _on_reconnect(self) -> None:
        self._metrics.nats_connected.set(1)
        logger.info("nats reconnected")

    async def _on_closed(self) -> None:
        self._metrics.nats_connected.set(0)
        logger.warning("nats client closed")

    async def _on_error(self, err: Exception) -> None:
        logger.warning("nats error callback: %s", err)
