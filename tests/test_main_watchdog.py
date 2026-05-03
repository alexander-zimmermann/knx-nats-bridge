from __future__ import annotations

import pytest

from knx_nats_bridge.logging_setup import TrackedStreamHandler
from knx_nats_bridge.main import LOG_EMIT_RECOVERY_WINDOW_SECONDS, logger_watchdog_ok


@pytest.fixture(autouse=True)
def _reset_handler_state() -> None:
    TrackedStreamHandler.emit_errors_total = 0
    TrackedStreamHandler.last_emit_ok_ts = 0.0


def test_watchdog_ok_when_no_errors_seen() -> None:
    assert logger_watchdog_ok(now=12345.0) is True


def test_watchdog_ok_when_recent_emit_after_error() -> None:
    TrackedStreamHandler.emit_errors_total = 1
    TrackedStreamHandler.last_emit_ok_ts = 1000.0
    # 30s elapsed since last successful emit — within the 60s window.
    assert logger_watchdog_ok(now=1030.0) is True


def test_watchdog_fails_when_no_recent_emit_after_error() -> None:
    TrackedStreamHandler.emit_errors_total = 1
    TrackedStreamHandler.last_emit_ok_ts = 1000.0
    # 70s elapsed since last successful emit — exceeds the recovery window.
    assert logger_watchdog_ok(now=1070.0) is False


def test_watchdog_window_constant_matches_plan() -> None:
    assert LOG_EMIT_RECOVERY_WINDOW_SECONDS == 60.0
