"""Unit tests for the ga-catalog importer CLI.

The full `run()` path needs a live Postgres + the `ga_catalog` schema
and lives in the cluster smoke test after deploy. Here we cover the
pure-Python helpers (YAML parsing, env-driven DSN construction).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from knx_nats_bridge.cli.import_catalog import _dsn_from_env, _read_catalog, _to_rows


def _write_catalog(tmp: Path, contents: str) -> Path:
    p = tmp / "ga-catalog.yaml"
    p.write_text(contents, encoding="utf-8")
    return p


def test_to_rows_extracts_optional_metadata() -> None:
    catalog = {
        "0/1/40": {
            "name": "Lighting.1F.Bedroom.Ceiling",
            "dpt": "1.001",
            "room": "Bedroom",
            "function": "Lighting",
            "description": "Ceiling light",
        },
        "0/2/0": {"name": "General.Scenes", "dpt": "17.001"},
    }
    rows = _to_rows(catalog)
    assert rows == [
        ("0/1/40", "Lighting.1F.Bedroom.Ceiling", "Bedroom", "Lighting", "Ceiling light", "1.001"),
        ("0/2/0", "General.Scenes", None, None, None, "17.001"),
    ]


def test_to_rows_rejects_entry_without_name_or_dpt() -> None:
    with pytest.raises(ValueError, match="requires string 'name' and 'dpt'"):
        _to_rows({"1/0/0": {"name": "X"}})  # missing dpt
    with pytest.raises(ValueError, match="requires string 'name' and 'dpt'"):
        _to_rows({"1/0/0": {"dpt": "1.001"}})  # missing name
    with pytest.raises(ValueError, match="must be a mapping"):
        _to_rows({"1/0/0": "not a mapping"})


def test_read_catalog_rejects_top_level_list(tmp_path: Path) -> None:
    path = _write_catalog(tmp_path, "- not: a-mapping\n")
    with pytest.raises(ValueError, match="expected mapping at top level"):
        _read_catalog(path)


def test_read_catalog_empty_returns_empty_dict(tmp_path: Path) -> None:
    path = _write_catalog(tmp_path, "")
    assert _read_catalog(path) == {}


def test_dsn_from_env_url_encodes_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Random passwords routinely contain `/`, `@`, `:`, `+` that break
    psycopg's URI parser if passed verbatim."""
    monkeypatch.setenv("MCP_DB_HOST", "tsdb.example")
    monkeypatch.setenv("MCP_DB_PORT", "5433")
    monkeypatch.setenv("MCP_DB_NAME", "homelab")
    monkeypatch.setenv("MCP_DB_USERNAME", "user/with/slash")
    monkeypatch.setenv("MCP_DB_PASSWORD", "p@ss:wo+rd")
    dsn = _dsn_from_env()
    assert dsn == ("postgresql://user%2Fwith%2Fslash:p%40ss%3Awo%2Brd@tsdb.example:5433/homelab")


def test_dsn_from_env_default_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_DB_HOST", "x")
    monkeypatch.setenv("MCP_DB_NAME", "x")
    monkeypatch.setenv("MCP_DB_USERNAME", "x")
    monkeypatch.setenv("MCP_DB_PASSWORD", "x")
    monkeypatch.delenv("MCP_DB_PORT", raising=False)
    assert "5432" in _dsn_from_env()


def test_dsn_from_env_missing_required_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("MCP_DB_HOST", "MCP_DB_NAME", "MCP_DB_USERNAME", "MCP_DB_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit, match="missing required env vars"):
        _dsn_from_env()
