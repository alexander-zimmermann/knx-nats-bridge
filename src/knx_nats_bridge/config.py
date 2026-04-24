"""Settings from env vars and config files (pydantic-settings); secrets are read from files."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConnectionType(StrEnum):
    TUNNELING_TCP = "tunneling_tcp"
    TUNNELING_UDP = "tunneling_udp"
    ROUTING = "routing"


class UnmappedPolicy(StrEnum):
    SKIP = "skip"
    WARN = "warn"
    RAW = "raw"


class LogFormat(StrEnum):
    JSON = "json"
    TEXT = "text"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # KNX
    knx_connection_type: ConnectionType = ConnectionType.TUNNELING_TCP
    knx_gateway_host: str | None = None
    knx_gateway_port: int = 3671
    knx_local_ip: str | None = None
    knx_individual_address: str | None = None
    knx_secure_keyring_file: Path | None = None
    knx_secure_keyring_password: str | None = None
    knx_nats_mapping_path: Path = Path("/etc/knx-nats-bridge/ga-mapping.yaml")
    knx_nats_unmapped_policy: UnmappedPolicy = UnmappedPolicy.SKIP

    # NATS
    nats_servers: str = "nats://localhost:4222"
    nats_subject_prefix: str = "knx"
    nats_creds_file: Path | None = None
    nats_user: str | None = None
    nats_user_password_file: Path | None = None
    nats_stream_check: bool = True
    nats_stream_name: str = "KNX"

    # Observability
    metrics_port: int = 9090
    log_level: str = "INFO"
    log_format: LogFormat = LogFormat.JSON

    @property
    def nats_servers_list(self) -> list[str]:
        return [s.strip() for s in self.nats_servers.split(",") if s.strip()]

    @field_validator("nats_subject_prefix")
    @classmethod
    def _prefix_no_dot(cls, v: str) -> str:
        if "." in v or "/" in v or " " in v or not v:
            raise ValueError(
                "nats_subject_prefix must be a non-empty single-token (no dots, slashes, spaces)"
            )
        return v

    @model_validator(mode="after")
    def _require_gateway_or_routing(self) -> Settings:
        tunneling = (ConnectionType.TUNNELING_TCP, ConnectionType.TUNNELING_UDP)
        if self.knx_connection_type in tunneling and not self.knx_gateway_host:
            raise ValueError("KNX_GATEWAY_HOST is required for tunneling modes")
        return self

    def read_nats_password(self) -> str | None:
        if self.nats_user_password_file and self.nats_user_password_file.exists():
            return self.nats_user_password_file.read_text().strip()
        return None
