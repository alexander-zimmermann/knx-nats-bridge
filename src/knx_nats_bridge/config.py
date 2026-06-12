"""Settings from env vars and config files (pydantic-settings); secrets are read from files."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

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
    bridge_ga_catalog_path: Path = Path("/etc/knx-nats-bridge/ga-catalog.yaml")
    knx_nats_unmapped_policy: UnmappedPolicy = UnmappedPolicy.SKIP
    # Max outgoing bus telegrams per second (xknx paces sends by 1/N seconds).
    # Caps the writer so NATS bursts don't overload the shared TP1 bus. 0 = off.
    knx_rate_limit: int = 10

    # NATS
    nats_servers: str = "nats://localhost:4222"
    nats_subject_prefix: str = "knx"
    nats_creds_file: Path | None = None
    nats_nkey_seed_file: Path | None = None
    nats_user: str | None = None
    nats_user_password_file: Path | None = None
    nats_stream_check: bool = True
    nats_stream_name: str = "KNX"

    # Bridge writer (NATS -> KNX). Off by default so the image releases without
    # any cluster-side effect until the mapping is provisioned and the NATS
    # user has been granted the necessary subscribe permissions.
    bridge_writer_enabled: bool = False
    bridge_writer_rules_path: Path = Path("/etc/knx-nats-bridge/writer-rules.yaml")

    # Observability
    metrics_port: int = 9090
    log_level: str = "INFO"
    log_format: LogFormat = LogFormat.JSON

    @property
    def nats_servers_list(self) -> list[str]:
        return [s.strip() for s in self.nats_servers.split(",") if s.strip()]

    @field_validator("knx_rate_limit")
    @classmethod
    def _rate_limit_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("KNX_RATE_LIMIT must be >= 0 (0 disables rate limiting)")
        return v

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

    @model_validator(mode="after")
    def _require_rules_file_when_writer_enabled(self) -> Settings:
        if self.bridge_writer_enabled and not self.bridge_writer_rules_path.exists():
            raise ValueError(
                f"BRIDGE_WRITER_ENABLED is true but rules file "
                f"{self.bridge_writer_rules_path} does not exist"
            )
        return self

    def read_nats_password(self) -> str | None:
        if self.nats_user_password_file and self.nats_user_password_file.exists():
            return self.nats_user_password_file.read_text().strip()
        return None

    def nats_auth_kwargs(self) -> dict[str, Any]:
        """Build the auth subset of NatsClient.connect kwargs.

        Auth precedence: creds file > nkey seed file > user/password.
        Each form is mutually exclusive in nats-py; pick the first that's configured.
        """
        kwargs: dict[str, Any] = {}
        if self.nats_creds_file and self.nats_creds_file.exists():
            kwargs["user_credentials"] = str(self.nats_creds_file)
        elif self.nats_nkey_seed_file and self.nats_nkey_seed_file.exists():
            kwargs["nkeys_seed"] = str(self.nats_nkey_seed_file)
        elif self.nats_user:
            password = self.read_nats_password()
            if password is None:
                raise RuntimeError(
                    "NATS_USER is set but NATS_USER_PASSWORD_FILE is missing or empty"
                )
            kwargs["user"] = self.nats_user
            kwargs["password"] = password
        return kwargs
