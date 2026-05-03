from __future__ import annotations

import io
import logging
from collections.abc import Iterator

import pytest

from knx_nats_bridge.config import LogFormat
from knx_nats_bridge.logging_setup import TrackedStreamHandler, configure


@pytest.fixture(autouse=True)
def _reset_handler_state() -> None:
    TrackedStreamHandler.emit_errors_total = 0
    TrackedStreamHandler.last_emit_ok_ts = 0.0


class _BrokenStream(io.StringIO):
    def write(self, _: str) -> int:
        raise OSError("stdout pipe broken")


def test_configure_attaches_tracked_handler() -> None:
    configure("INFO", LogFormat.JSON)
    root = logging.getLogger()
    assert any(isinstance(h, TrackedStreamHandler) for h in root.handlers)
    assert TrackedStreamHandler.last_emit_ok_ts > 0.0


def test_emit_success_advances_last_emit_ok_ts() -> None:
    handler = TrackedStreamHandler(io.StringIO())
    handler.setFormatter(logging.Formatter("%(message)s"))
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "ok", None, None)
    handler.emit(record)
    assert TrackedStreamHandler.last_emit_ok_ts > 0.0
    assert TrackedStreamHandler.emit_errors_total == 0


@pytest.fixture
def _silent_log_errors() -> Iterator[None]:
    """Suppress the stderr noise from Handler.handleError() during tests."""
    saved = logging.raiseExceptions
    logging.raiseExceptions = False
    try:
        yield
    finally:
        logging.raiseExceptions = saved


def test_emit_failure_increments_counter_and_does_not_raise(_silent_log_errors: None) -> None:
    handler = TrackedStreamHandler(_BrokenStream())
    handler.setFormatter(logging.Formatter("%(message)s"))
    record = logging.LogRecord("t", logging.WARNING, __file__, 1, "boom", None, None)
    handler.emit(record)  # must not raise
    assert TrackedStreamHandler.emit_errors_total == 1


def test_emit_recovery_updates_last_emit_ok_ts(_silent_log_errors: None) -> None:
    handler = TrackedStreamHandler(_BrokenStream())
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.emit(logging.LogRecord("t", logging.WARNING, __file__, 1, "boom", None, None))
    assert TrackedStreamHandler.emit_errors_total == 1
    pre_recovery_ts = TrackedStreamHandler.last_emit_ok_ts

    # Swap to a working stream and emit again — the timestamp should advance.
    handler.setStream(io.StringIO())
    handler.emit(logging.LogRecord("t", logging.INFO, __file__, 1, "ok", None, None))
    assert TrackedStreamHandler.last_emit_ok_ts > pre_recovery_ts
    # Counter is cumulative; recovery does not reset it.
    assert TrackedStreamHandler.emit_errors_total == 1
