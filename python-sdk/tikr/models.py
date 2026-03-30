"""Data models for Tikr SDK."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Tick:
    """A single data point to ingest into Tikr."""

    timestamp_ns: int
    dimensions: dict[str, str] = field(default_factory=dict)
    fields: dict[str, int] = field(default_factory=dict)
    sequence: int = 0


@dataclass
class Bar:
    """A rolled-up bar returned from a Tikr query."""

    series: str
    bucket_ts: int
    dimensions: dict[str, str]
    metrics: dict[str, int]
    first_timestamp: int
    last_timestamp: int
    tick_count: int


@dataclass
class SeriesInfo:
    """Metadata about a configured series."""

    name: str
    dimensions: list[str]
    metrics: list[str]
    rollup: str
