"""Settings from env vars and config files (pydantic-settings); secrets are read from files."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from nats_bridge_core import NatsSettings
from pydantic import field_validator, model_validator


class ConnectionType(StrEnum):
    TUNNELING_TCP = "tunneling_tcp"
    TUNNELING_UDP = "tunneling_udp"
    ROUTING = "routing"


class UnmappedPolicy(StrEnum):
    SKIP = "skip"
    WARN = "warn"
    RAW = "raw"


class Settings(NatsSettings):
    # KNX
    knx_connection_type: ConnectionType = ConnectionType.TUNNELING_TCP
    knx_gateway_host: str | None = None
    knx_gateway_port: int = 3671
    knx_local_ip: str | None = None
    bridge_ga_catalog_path: Path = Path("/etc/knx-nats-bridge/ga-catalog.yaml")
    knx_nats_unmapped_policy: UnmappedPolicy = UnmappedPolicy.SKIP
    # Max outgoing bus telegrams per second (xknx paces sends by 1/N seconds).
    # Caps the writer so NATS bursts don't overload the shared TP1 bus. 0 = off.
    knx_rate_limit: int = 10

    # NATS
    nats_subject_prefix: str = "knx"
    nats_stream_name: str = "KNX"

    # Bridge writer (NATS -> KNX). Off by default so the image releases without
    # any cluster-side effect until the mapping is provisioned and the NATS
    # user has been granted the necessary subscribe permissions.
    bridge_writer_enabled: bool = False
    bridge_writer_rules_path: Path = Path("/etc/knx-nats-bridge/writer-rules.yaml")

    # Read responder (KNX -> KNX). When enabled, the bridge answers GroupValueRead
    # requests for the GAs it writes (writer rules) with the last value it put on
    # the bus, so visualisations that poll on startup get a value for slow-changing
    # datapoints instead of a default 0. Requires the writer to be enabled — the
    # responder sources its values from the writer's last-written cache.
    bridge_read_responder_enabled: bool = False

    @field_validator("knx_rate_limit")
    @classmethod
    def _rate_limit_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("KNX_RATE_LIMIT must be >= 0 (0 disables rate limiting)")
        return v

    @model_validator(mode="after")
    def _require_gateway_or_routing(self) -> Settings:
        tunneling = (ConnectionType.TUNNELING_TCP, ConnectionType.TUNNELING_UDP)
        if self.knx_connection_type in tunneling and not self.knx_gateway_host:
            raise ValueError("KNX_GATEWAY_HOST is required for tunneling modes")
        return self

    @model_validator(mode="after")
    def _require_rules_file_when_writer_enabled(self) -> Settings:
        if self.bridge_writer_enabled and not self.bridge_writer_rules_path.exists():
            raise ValueError(
                f"BRIDGE_WRITER_ENABLED is true but rules file "
                f"{self.bridge_writer_rules_path} does not exist"
            )
        return self

    @model_validator(mode="after")
    def _read_responder_requires_writer(self) -> Settings:
        if self.bridge_read_responder_enabled and not self.bridge_writer_enabled:
            raise ValueError(
                "BRIDGE_READ_RESPONDER_ENABLED is true but BRIDGE_WRITER_ENABLED is "
                "false; the responder answers reads for the writer's group addresses"
            )
        return self
