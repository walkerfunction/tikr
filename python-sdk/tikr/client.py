"""Tikr gRPC client."""

from __future__ import annotations

from typing import Iterator, Sequence

import grpc

from tikr._stubs import get_pb2, get_pb2_grpc
from tikr.models import Bar, SeriesInfo, Tick


class TikrClient:
    """High-level client for the Tikr gRPC service.

    Usage::

        from tikr import TikrClient, Tick

        client = TikrClient("localhost:9876")
        ticks = [Tick(timestamp_ns=..., dimensions={...}, fields={...})]
        count = client.ingest("market_ticks", ticks)
        bars = client.query_bars("market_ticks", dimensions={"symbol": "AAPL"})
    """

    def __init__(self, address: str = "localhost:9876", **channel_kwargs):
        self._address = address
        self._channel = grpc.insecure_channel(address, **channel_kwargs)
        self._stub = get_pb2_grpc().TikrStub(self._channel)

    def close(self):
        """Close the gRPC channel."""
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ── Ingest ──────────────────────────────────────────────────────

    def ingest(
        self,
        series: str,
        ticks: Sequence[Tick],
        batch_size: int = 5000,
    ) -> int:
        """Ingest ticks into a series. Returns total ticks accepted."""
        pb2 = get_pb2()

        def _stream():
            for i in range(0, len(ticks), batch_size):
                batch = ticks[i : i + batch_size]
                proto_ticks = [
                    pb2.TickData(
                        timestamp_ns=t.timestamp_ns,
                        dimensions=t.dimensions,
                        fields=t.fields,
                        sequence=t.sequence,
                    )
                    for t in batch
                ]
                yield pb2.IngestRequest(series=series, ticks=proto_ticks)

        resp = self._stub.IngestTicks(_stream())
        return resp.ticks_received

    def ingest_stream(
        self,
        requests: Iterator,
    ) -> int:
        """Ingest from a raw iterator of IngestRequest messages (for advanced use)."""
        resp = self._stub.IngestTicks(requests)
        return resp.ticks_received

    # ── Query ───────────────────────────────────────────────────────

    def query_bars(
        self,
        series: str,
        dimensions: dict[str, str] | None = None,
        start_ns: int = 0,
        end_ns: int = 0,
    ) -> list[Bar]:
        """Query rolled-up bars for a series."""
        pb2 = get_pb2()
        req = pb2.BarQuery(
            series=series,
            dimensions=dimensions or {},
            start_ns=start_ns,
            end_ns=end_ns,
        )
        bars = []
        for b in self._stub.QueryBars(req):
            bars.append(Bar(
                series=b.series,
                bucket_ts=b.bucket_ts,
                dimensions=dict(b.dimensions),
                metrics=dict(b.metrics),
                first_timestamp=b.first_timestamp,
                last_timestamp=b.last_timestamp,
                tick_count=b.tick_count,
            ))
        return bars

    def query_ticks(
        self,
        series: str,
        dimensions: dict[str, str] | None = None,
        start_ns: int = 0,
        end_ns: int = 0,
        limit: int = 0,
    ) -> list[Tick]:
        """Query raw ticks for a series."""
        pb2 = get_pb2()
        req = pb2.TickQuery(
            series=series,
            dimensions=dimensions or {},
            start_ns=start_ns,
            end_ns=end_ns,
            limit=limit,
        )
        ticks = []
        for t in self._stub.QueryTicks(req):
            ticks.append(Tick(
                timestamp_ns=t.timestamp_ns,
                dimensions=dict(t.dimensions),
                fields=dict(t.fields),
                sequence=t.sequence,
            ))
        return ticks

    # ── Admin ───────────────────────────────────────────────────────

    def list_series(self) -> list[SeriesInfo]:
        """List all configured series on the server."""
        pb2 = get_pb2()
        resp = self._stub.ListSeries(pb2.Empty())
        return [
            SeriesInfo(
                name=s.name,
                dimensions=list(s.dimensions),
                metrics=list(s.metrics),
                rollup=s.rollup,
            )
            for s in resp.series
        ]

    def get_info(self) -> dict:
        """Get server info (version, uptime, series)."""
        pb2 = get_pb2()
        info = self._stub.GetInfo(pb2.Empty())
        return {
            "version": info.version,
            "uptime_seconds": info.uptime_seconds,
            "series": [
                SeriesInfo(
                    name=s.name,
                    dimensions=list(s.dimensions),
                    metrics=list(s.metrics),
                    rollup=s.rollup,
                )
                for s in info.series
            ],
        }
