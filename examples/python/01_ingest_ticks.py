#!/usr/bin/env python3
"""
Tikr Example: Ingest market ticks and query rolled-up bars.

Run Tikr first: docker compose -f docker/docker-compose.yml up -d

Usage:
    pip install -e python-sdk/
    python examples/python/01_ingest_ticks.py
"""

import os
import random
import time

from tikr import TikrClient, Tick

TIKR_ADDR = os.environ.get("TIKR_ADDR", "localhost:9876")


def generate_ticks(
    symbol: str, num_seconds: int = 5, ticks_per_sec: int = 100
) -> list[Tick]:
    """Generate realistic market tick data for a symbol."""
    base_price = 17500  # $175.00 in fixed-point (* 100)
    base_time = int(time.time()) * 1_000_000_000

    ticks = []
    for sec in range(num_seconds):
        for i in range(ticks_per_sec):
            ts = base_time + sec * 1_000_000_000 + i * (1_000_000_000 // ticks_per_sec)
            ticks.append(
                Tick(
                    timestamp_ns=ts,
                    dimensions={"symbol": symbol},
                    fields={
                        "price": base_price + random.randint(-50, 50),
                        "quantity": random.randint(1, 200),
                    },
                    sequence=i,
                )
            )
    return ticks


with TikrClient(TIKR_ADDR) as client:
    # 1. Server info
    print("=== Tikr Server Info ===")
    info = client.get_info()
    print(f"  version: {info['version']}")
    for s in info["series"]:
        print(f"  series: {s.name} (rollup: {s.rollup}, metrics: {s.metrics})")
    print()

    # 2. Ingest market ticks
    print("=== Ingesting Market Ticks ===")
    print("  symbol: AAPL, 5 seconds, 100 ticks/sec")
    ticks = generate_ticks("AAPL", num_seconds=5, ticks_per_sec=100)
    count = client.ingest("market_ticks", ticks)
    print(f"  total ingested: {count} ticks")
    print()

    time.sleep(2)  # wait for rollup flush

    # 3. Query rolled-up bars
    print("=== Query 1-Second Bars ===")
    now_ns = int(time.time()) * 1_000_000_000
    bars = client.query_bars(
        "market_ticks",
        dimensions={"symbol": "AAPL"},
        start_ns=now_ns - 60 * 1_000_000_000,
        end_ns=now_ns + 60 * 1_000_000_000,
    )
    print(f"  got {len(bars)} bars")
    for bar in bars:
        m = bar.metrics
        print(
            f"    bucket={bar.bucket_ts} "
            f"open={m.get('open', '?')} high={m.get('high', '?')} "
            f"low={m.get('low', '?')} close={m.get('close', '?')} "
            f"volume={m.get('volume', '?')} ticks={bar.tick_count}"
        )
    print()

    # 4. Query raw ticks
    print("=== Query Raw Ticks (first 10) ===")
    ticks = client.query_ticks(
        "market_ticks",
        dimensions={"symbol": "AAPL"},
        start_ns=now_ns - 60 * 1_000_000_000,
        end_ns=now_ns + 60 * 1_000_000_000,
        limit=10,
    )
    print(f"  got {len(ticks)} ticks")
    for t in ticks[:5]:
        print(
            f"    ts={t.timestamp_ns} price={t.fields.get('price', '?')} qty={t.fields.get('quantity', '?')}"
        )
    if len(ticks) > 5:
        print(f"    ... and {len(ticks) - 5} more")

    print()
    print("Done! Tikr ingested ticks, rolled them into 1s bars, and served queries.")
    print(
        "In production, those bars would also be pushed to Kafka for your cloud TSDB."
    )
