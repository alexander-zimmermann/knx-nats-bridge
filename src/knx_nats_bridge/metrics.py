"""Prometheus metrics registry and a tiny HTTP server exposing /metrics and /healthz."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

logger = logging.getLogger(__name__)


class HealthCheck(Protocol):
    def __call__(self) -> bool: ...


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        self.telegrams_received = Counter(
            "knx_telegrams_received_total",
            "KNX telegrams received from the bus, by DPT",
            ["dpt"],
            registry=self.registry,
        )
        self.telegrams_published = Counter(
            "knx_telegrams_published_total",
            "KNX telegrams successfully published to NATS",
            registry=self.registry,
        )
        self.publish_errors = Counter(
            "knx_publish_errors_total",
            "Publish errors by reason",
            ["reason"],
            registry=self.registry,
        )
        self.tunnel_connected = Gauge(
            "knx_tunnel_connected",
            "1 if KNX tunnel is currently connected, 0 otherwise",
            registry=self.registry,
        )
        self.nats_connected = Gauge(
            "nats_connected",
            "1 if NATS client is currently connected, 0 otherwise",
            registry=self.registry,
        )
        self.last_telegram_ts = Gauge(
            "knx_last_telegram_received_timestamp",
            "Unix timestamp of the last received KNX telegram (seconds)",
            registry=self.registry,
        )


async def serve(
    metrics: Metrics,
    port: int,
    is_healthy: Callable[[], Awaitable[bool]] | Callable[[], bool],
) -> asyncio.AbstractServer:
    """Start a tiny HTTP server exposing /metrics and /healthz."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                return
            # Drain the rest of the request headers.
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break

            parts = request_line.decode("ascii", errors="replace").split()
            path = parts[1] if len(parts) >= 2 else "/"

            if path.startswith("/metrics"):
                body = generate_latest(metrics.registry)
                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    + f"Content-Type: {CONTENT_TYPE_LATEST}\r\n".encode("ascii")
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            elif path.startswith("/healthz"):
                result = is_healthy()
                if asyncio.iscoroutine(result):
                    ok = await result
                else:
                    ok = bool(result)
                status = b"200 OK" if ok else b"503 Service Unavailable"
                body = b"ok\n" if ok else b"unhealthy\n"
                writer.write(
                    b"HTTP/1.1 " + status + b"\r\n"
                    b"Content-Type: text/plain\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            else:
                body = b"not found\n"
                writer.write(
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Content-Type: text/plain\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            await writer.drain()
        except Exception:
            logger.exception("metrics http handler failed")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handle, host="0.0.0.0", port=port)
    logger.info("metrics server listening on :%d", port)
    return server
