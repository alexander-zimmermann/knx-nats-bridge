from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from knx_nats_bridge.config import ConnectionType, Settings


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in list(os.environ):
        if k.startswith(("KNX_", "NATS_", "LOG_", "METRICS_")):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("KNX_GATEWAY_HOST", "192.0.2.10")
    s = Settings()
    assert s.knx_connection_type is ConnectionType.TUNNELING_TCP
    assert s.knx_gateway_port == 3671
    assert s.nats_subject_prefix == "knx"
    assert s.nats_servers_list == ["nats://localhost:4222"]


def test_subject_prefix_rejects_dot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KNX_GATEWAY_HOST", "192.0.2.10")
    monkeypatch.setenv("NATS_SUBJECT_PREFIX", "knx.raw")
    with pytest.raises(ValidationError):
        Settings()


def test_tunneling_requires_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KNX_GATEWAY_HOST", raising=False)
    monkeypatch.setenv("KNX_CONNECTION_TYPE", "tunneling_tcp")
    with pytest.raises(ValidationError):
        Settings()


def test_routing_does_not_require_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KNX_GATEWAY_HOST", raising=False)
    monkeypatch.setenv("KNX_CONNECTION_TYPE", "routing")
    s = Settings()
    assert s.knx_connection_type is ConnectionType.ROUTING


def test_servers_list_splits_commas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KNX_GATEWAY_HOST", "192.0.2.10")
    monkeypatch.setenv("NATS_SERVERS", "nats://a:4222, nats://b:4222")
    s = Settings()
    assert s.nats_servers_list == ["nats://a:4222", "nats://b:4222"]


def test_nkey_seed_file_accepted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("KNX_GATEWAY_HOST", "192.0.2.10")
    seed_file = tmp_path / "nkey-seed"
    seed_file.write_text("SUADFK6A2CUJXTARGJWGSSJNG7OINWPY4TAYAAMZOEEJNYQLKVH6BYVYTU\n")
    monkeypatch.setenv("NATS_NKEY_SEED_FILE", str(seed_file))
    s = Settings()
    assert s.nats_nkey_seed_file == seed_file
    assert s.nats_nkey_seed_file is not None
    assert s.nats_nkey_seed_file.exists()
