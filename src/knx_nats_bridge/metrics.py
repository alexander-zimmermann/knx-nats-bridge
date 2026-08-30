"""Prometheus metrics registry and a tiny HTTP server exposing /metrics and /healthz."""

from __future__ import annotations

import logging

from nats_bridge_core import TrackedStreamHandler
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
)

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        self.telegrams_received = Counter(
            "knx_telegrams_received_total",
            "KNX GroupValue Write/Response telegrams seen on the bus",
            registry=self.registry,
        )
        self.telegrams_decoded = Counter(
            "knx_telegrams_decoded_total",
            "Telegrams successfully decoded by DPT (subset of received)",
            ["dpt"],
            registry=self.registry,
        )
        self.telegrams_unmapped = Counter(
            "knx_telegrams_unmapped_total",
            "Telegrams whose group address has no mapping entry",
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
        self.writer_nats_connected = Gauge(
            "knx_writer_nats_connected",
            "1 if the writer's NATS client is currently connected, 0 otherwise",
            registry=self.registry,
        )
        self.last_telegram_ts = Gauge(
            "knx_last_telegram_received_timestamp",
            "Unix timestamp of the last received KNX telegram (seconds)",
            registry=self.registry,
        )
        # Surface logger-health state so a stuck stdout is visible in Prometheus,
        # not just via liveness. Source of truth is TrackedStreamHandler.
        self.log_emit_errors = Gauge(
            "knx_bridge_log_emit_errors",
            "Cumulative count of logging handler emit() failures since pod start",
            registry=self.registry,
        )
        self.log_emit_errors.set_function(lambda: float(TrackedStreamHandler.emit_errors_total))
        self.log_last_emit_ok_timestamp = Gauge(
            "knx_bridge_log_last_emit_ok_timestamp",
            "Monotonic-seconds timestamp of the last successful log emit",
            registry=self.registry,
        )
        self.log_last_emit_ok_timestamp.set_function(
            lambda: float(TrackedStreamHandler.last_emit_ok_ts)
        )

        # Writer path (NATS -> KNX). Always registered so /metrics has a stable
        # surface whether the writer is enabled or not.
        self.knx_writes = Counter(
            "knx_writes_total",
            "KNX GroupValueWrite operations triggered by NATS events "
            "(outcome: ok | error | suppressed). suppressed = dropped by the deadband.",
            ["subject", "ga", "outcome"],
            registry=self.registry,
        )
        self.knx_write_errors = Counter(
            "knx_write_errors_total",
            "Writer-side errors by reason (bad_json, payload_path, dpt_encode, bus)",
            ["reason"],
            registry=self.registry,
        )
        self.knx_write_duration = Histogram(
            "knx_write_duration_seconds",
            "End-to-end latency from NATS message receipt to bus put()",
            registry=self.registry,
        )

        # Read-responder path (KNX -> KNX). GroupValueRead requests answered with
        # the writer's last-written value. outcome: ok | no_value (nothing cached
        # yet, e.g. right after restart) | error (DPT encode/bus failure).
        self.knx_read_responses = Counter(
            "knx_read_responses_total",
            "GroupValueRead requests handled by the read responder "
            "(outcome: ok | no_value | error)",
            ["ga", "outcome"],
            registry=self.registry,
        )

        # Startup seed (JetStream -> writer cache). Primes the read responder
        # for event-driven subjects whose value wouldn't otherwise arrive again
        # until the next change. outcome: ok | no_message (empty stream) |
        # no_stream (subject not JetStream-backed) | error (decode/encode).
        self.knx_seed = Counter(
            "knx_seed_total",
            "Read-responder cache entries seeded from JetStream at startup "
            "(outcome: ok | no_message | no_stream | error)",
            ["subject", "outcome"],
            registry=self.registry,
        )

    # --- nats_bridge_core.PublisherMetrics -------------------------------
    # This bridge has one undifferentiated publish counter, so ctx is unused.

    def set_connected(self, connected: bool) -> None:
        self.nats_connected.set(1 if connected else 0)

    def count_published(self, _ctx: object) -> None:
        self.telegrams_published.inc()

    def count_error(self, _ctx: object, reason: str) -> None:
        self.publish_errors.labels(reason=reason).inc()
