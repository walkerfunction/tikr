#!/usr/bin/env python3
"""
Tikr Example: Ingest market ticks and query rolled-up bars.

This shows how an app builder uses Tikr as their time series edge engine.
Run Tikr first: docker compose -f docker/docker-compose.yml up -d

Usage:
    pip install grpcio grpcio-tools protobuf
    python examples/python/01_ingest_ticks.py
"""

import time
import random
import grpc
import sys
import os

# Generate Python proto stubs if not already present
PROTO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "proto")
STUB_DIR = os.path.join(os.path.dirname(__file__), "_generated")

def generate_stubs():
    """Generate Python gRPC stubs from tikr.proto."""
    os.makedirs(STUB_DIR, exist_ok=True)
    from grpc_tools import protoc
    protoc.main([
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={STUB_DIR}",
        f"--grpc_python_out={STUB_DIR}",
        os.path.join(PROTO_DIR, "tikr.proto"),
    ])
    # Create __init__.py
    open(os.path.join(STUB_DIR, "__init__.py"), "w").close()
    print("Generated Python stubs in", STUB_DIR)

# Auto-generate stubs
if not os.path.exists(os.path.join(STUB_DIR, "tikr_pb2.py")):
    generate_stubs()

sys.path.insert(0, STUB_DIR)
import tikr_pb2
import tikr_pb2_grpc


# ─── Connect to Tikr ───────────────────────────────────────────────

TIKR_ADDR = os.environ.get("TIKR_ADDR", "localhost:9876")
channel = grpc.insecure_channel(TIKR_ADDR)
stub = tikr_pb2_grpc.TikrStub(channel)


# ─── 1. Check server info ──────────────────────────────────────────

print("=== Tikr Server Info ===")
info = stub.GetInfo(tikr_pb2.Empty())
print(f"  version: {info.version}")
for s in info.series:
    print(f"  series: {s.name} (rollup: {s.rollup}, metrics: {s.metrics})")
print()


# ─── 2. Ingest synthetic market ticks ──────────────────────────────

def generate_ticks(symbol: str, num_seconds: int = 5, ticks_per_sec: int = 100):
    """
    Generate realistic market tick data for a symbol.
    Yields IngestRequest messages for gRPC streaming.
    """
    base_price = 17500  # $175.00 in fixed-point (* 100)
    base_time = int(time.time()) * 1_000_000_000  # current time in nanoseconds

    for sec in range(num_seconds):
        ticks = []
        for i in range(ticks_per_sec):
            ts = base_time + sec * 1_000_000_000 + i * (1_000_000_000 // ticks_per_sec)
            price = base_price + random.randint(-50, 50)  # ±$0.50 noise
            qty = random.randint(1, 200)

            ticks.append(tikr_pb2.TickData(
                timestamp_ns=ts,
                dimensions={"symbol": symbol},
                fields={"price": price, "quantity": qty},
                sequence=i,
            ))

        yield tikr_pb2.IngestRequest(series="market_ticks", ticks=ticks)
        print(f"  sent {ticks_per_sec} ticks for second {sec + 1}/{num_seconds}")


print("=== Ingesting Market Ticks ===")
print(f"  symbol: AAPL, 5 seconds, 100 ticks/sec")

response = stub.IngestTicks(generate_ticks("AAPL", num_seconds=5, ticks_per_sec=100))
print(f"  total ingested: {response.ticks_received} ticks")
print()

# Brief wait for rollup flush
time.sleep(2)


# ─── 3. Query rolled-up bars ───────────────────────────────────────

print("=== Query 1-Second Bars ===")
now_ns = int(time.time()) * 1_000_000_000
bar_req = tikr_pb2.BarQuery(
    series="market_ticks",
    dimensions={"symbol": "AAPL"},
    start_ns=now_ns - 60 * 1_000_000_000,  # last 60 seconds
    end_ns=now_ns + 60 * 1_000_000_000,
)

bars = list(stub.QueryBars(bar_req))
print(f"  got {len(bars)} bars")
for bar in bars:
    print(f"    bucket={bar.bucket_ts} "
          f"open={bar.metrics.get('open', '?')} "
          f"high={bar.metrics.get('high', '?')} "
          f"low={bar.metrics.get('low', '?')} "
          f"close={bar.metrics.get('close', '?')} "
          f"volume={bar.metrics.get('volume', '?')} "
          f"ticks={bar.tick_count}")
print()


# ─── 4. Query raw ticks ────────────────────────────────────────────

print("=== Query Raw Ticks (first 10) ===")
tick_req = tikr_pb2.TickQuery(
    series="market_ticks",
    dimensions={"symbol": "AAPL"},
    start_ns=now_ns - 60 * 1_000_000_000,
    end_ns=now_ns + 60 * 1_000_000_000,
    limit=10,
)

ticks = list(stub.QueryTicks(tick_req))
print(f"  got {len(ticks)} ticks")
for t in ticks[:5]:
    print(f"    ts={t.timestamp_ns} price={t.fields.get('price', '?')} qty={t.fields.get('quantity', '?')}")
if len(ticks) > 5:
    print(f"    ... and {len(ticks) - 5} more")

print()
print("Done! Tikr ingested ticks, rolled them into 1s bars, and served queries.")
print("In production, those bars would also be pushed to Kafka for your cloud TSDB.")
