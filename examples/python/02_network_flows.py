#!/usr/bin/env python3
"""
Tikr Example: Ingest network flow data — same engine, different spec.

This demonstrates that Tikr is schema-agnostic. The same engine that
handles HFT market data also handles network telemetry, IoT, etc.

Usage:
    python examples/python/02_network_flows.py
"""

import time
import random
import grpc
import sys
import os

STUB_DIR = os.path.join(os.path.dirname(__file__), "_generated")
sys.path.insert(0, STUB_DIR)

# Auto-generate stubs if needed
if not os.path.exists(os.path.join(STUB_DIR, "tikr_pb2.py")):
    print("Run 01_ingest_ticks.py first to generate stubs")
    sys.exit(1)

import tikr_pb2
import tikr_pb2_grpc

TIKR_ADDR = os.environ.get("TIKR_ADDR", "localhost:9876")
channel = grpc.insecure_channel(TIKR_ADDR)
stub = tikr_pb2_grpc.TikrStub(channel)

# ─── Ingest network flows ──────────────────────────────────────────

PROTOCOLS = ["TCP", "UDP", "ICMP"]
IPS = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "192.168.1.1", "192.168.1.2"]


def generate_flows(num_seconds=3, flows_per_sec=50):
    base_time = int(time.time()) * 1_000_000_000

    for sec in range(num_seconds):
        ticks = []
        for i in range(flows_per_sec):
            ts = base_time + sec * 1_000_000_000 + i * (1_000_000_000 // flows_per_sec)
            ticks.append(tikr_pb2.TickData(
                timestamp_ns=ts,
                dimensions={
                    "src_ip": random.choice(IPS),
                    "dst_ip": random.choice(IPS),
                    "protocol": random.choice(PROTOCOLS),
                },
                fields={
                    "bytes": random.randint(64, 65535),
                    "packets": random.randint(1, 100),
                    "latency_us": random.randint(50, 5000),
                },
                sequence=i,
            ))

        yield tikr_pb2.IngestRequest(series="network_flows", ticks=ticks)
        print(f"  sent {flows_per_sec} flows for second {sec + 1}/{num_seconds}")


print("=== Ingesting Network Flows ===")
response = stub.IngestTicks(generate_flows())
print(f"  total ingested: {response.ticks_received} flows")
print()

time.sleep(2)

# ─── Query network rollup bars ─────────────────────────────────────

print("=== Query Network Flow Bars ===")
now_ns = int(time.time()) * 1_000_000_000
bars = list(stub.QueryBars(tikr_pb2.BarQuery(
    series="network_flows",
    dimensions={"src_ip": "10.0.0.1", "dst_ip": "10.0.0.2", "protocol": "TCP"},
    start_ns=now_ns - 60 * 1_000_000_000,
    end_ns=now_ns + 60 * 1_000_000_000,
)))

print(f"  got {len(bars)} bars for 10.0.0.1 → 10.0.0.2 TCP")
for bar in bars:
    print(f"    bucket={bar.bucket_ts} "
          f"bytes={bar.metrics.get('bytes_total', 0)} "
          f"packets={bar.metrics.get('packets_total', 0)} "
          f"max_latency={bar.metrics.get('max_latency_us', 0)}μs "
          f"flows={bar.tick_count}")

print()
print("Same Tikr engine, completely different data. No code changes needed —")
print("just a different YAML spec file in config/specs/.")
