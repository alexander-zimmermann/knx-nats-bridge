"""Entry point: wire config, mapping, metrics, publisher, and KNX listener; handle signals."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys

from .config import Settings
from .knx import KnxListener
from .logging_setup import configure as configure_logging
from .mapping import GroupAddressMapping
from .metrics import Metrics
from .metrics import serve as serve_metrics
from .publisher import Publisher

logger = logging.getLogger(__name__)


async def _amain() -> int:
    settings = Settings()
    configure_logging(settings.log_level, settings.log_format)
    logger.info("knx-nats-bridge starting")
    logger.info(
        "config: connection=%s gateway=%s:%s subject_prefix=%s mapping=%s",
        settings.knx_connection_type.value,
        settings.knx_gateway_host,
        settings.knx_gateway_port,
        settings.nats_subject_prefix,
        settings.knx_nats_mapping_path,
    )

    mapping = GroupAddressMapping.load(settings.knx_nats_mapping_path)
    logger.info("loaded %d GA entries", len(mapping))

    metrics = Metrics()
    publisher = Publisher(settings, metrics)
    listener = KnxListener(settings, mapping, publisher, metrics)

    def is_healthy() -> bool:
        return publisher.is_connected and listener.connected

    http_server = await serve_metrics(metrics, settings.metrics_port, is_healthy)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await publisher.connect()
        await listener.start()
        logger.info("bridge is up")
        await stop_event.wait()
    except Exception:
        logger.exception("fatal error in bridge startup/run")
        return 1
    finally:
        logger.info("shutting down")
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
