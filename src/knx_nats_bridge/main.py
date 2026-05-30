"""Entry point: wire config, mapping, metrics, publisher, and KNX listener; handle signals."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
import time

from .config import Settings
from .knx import KnxListener
from .logging_setup import TrackedStreamHandler
from .logging_setup import configure as configure_logging
from .mapping import GroupAddressMapping
from .metrics import Metrics
from .metrics import serve as serve_metrics
from .publisher import Publisher
from .write_mapping import WriteMappingTable
from .writer import Writer

logger = logging.getLogger(__name__)

# Liveness fails after this many seconds of consecutive log-emit failures.
# Forgiving enough for a transient stdout glitch (kubelet log rotation etc.),
# tight enough that a real wedge causes a restart well within an hour.
LOG_EMIT_RECOVERY_WINDOW_SECONDS = 60.0


def logger_watchdog_ok(now: float) -> bool:
    """Return False if log emits have been failing for longer than the recovery window."""
    if TrackedStreamHandler.emit_errors_total <= 0:
        return True
    return (now - TrackedStreamHandler.last_emit_ok_ts) <= LOG_EMIT_RECOVERY_WINDOW_SECONDS


async def _amain() -> int:
    settings = Settings()
    configure_logging(settings.log_level, settings.log_format)
    logger.info("knx-nats-bridge starting")
    logger.info(
        "config: connection=%s gateway=%s:%s subject_prefix=%s catalog=%s",
        settings.knx_connection_type.value,
        settings.knx_gateway_host,
        settings.knx_gateway_port,
        settings.nats_subject_prefix,
        settings.knx_nats_catalog_path,
    )

    mapping = GroupAddressMapping.load(settings.knx_nats_catalog_path)
    logger.info("loaded %d GA entries", len(mapping))

    metrics = Metrics()
    publisher = Publisher(settings, metrics)
    listener = KnxListener(settings, mapping, publisher, metrics)

    write_mappings: WriteMappingTable | None = None
    if settings.bridge_write_enabled:
        write_mappings = WriteMappingTable.load(
            settings.bridge_write_mapping_path,
            reader_subject_prefix=settings.nats_subject_prefix,
        )
        logger.info(
            "writer enabled: %d mappings across %d subjects",
            len(write_mappings),
            len(write_mappings.subjects()),
        )

    writer: Writer | None = None

    def is_healthy() -> bool:
        if not (publisher.is_connected and listener.connected):
            return False
        if writer is not None and len(writer._mappings) and not writer.is_connected:
            return False
        return logger_watchdog_ok(time.monotonic())

    http_server = await serve_metrics(metrics, settings.metrics_port, is_healthy)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await publisher.connect()
        await listener.start()
        if write_mappings is not None:
            xknx_instance = listener.xknx
            if xknx_instance is None:
                raise RuntimeError("listener.xknx is None after start() — cannot start writer")
            writer = Writer(settings, write_mappings, xknx_instance, metrics)
            await writer.start()
        logger.info("bridge is up")
        await stop_event.wait()
    except Exception:
        logger.exception("fatal error in bridge startup/run")
        return 1
    finally:
        logger.info("shutting down")
        if writer is not None:
            try:
                await writer.stop()
            except Exception:
                logger.exception("error stopping writer")
        try:
            await listener.stop()
        except Exception:
            logger.exception("error stopping KNX listener")
        try:
            await publisher.close()
        except Exception:
            logger.exception("error closing NATS publisher")
        http_server.close()
        with contextlib.suppress(Exception):
            await http_server.wait_closed()

    return 0


def run() -> None:
    sys.exit(asyncio.run(_amain()))


if __name__ == "__main__":
    run()
