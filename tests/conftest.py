"""One recording tracer provider for the whole session: the global can only be set once."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    """Finished spans of the current test."""
    _exporter.clear()
    yield _exporter
    _exporter.clear()


@pytest.fixture
def footprint(tmp_path: Path) -> Callable[..., Path]:
    """Write a footprint file from ``<address> <direction>`` lines; returns its path."""

    def write(*lines: str) -> Path:
        path = tmp_path / "footprint.txt"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    return write
