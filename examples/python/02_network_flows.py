#!/usr/bin/env python3
"""
Tikr Example: Ingest network flow data — same engine, different spec.

Demonstrates that Tikr is schema-agnostic. The same engine that handles
HFT market data also handles network telemetry, IoT, etc.

Usage:
    pip install -e python-sdk/
    python examples/python/02_network_flows.py
"""

import os
import random
import time

from tikr import TikrClient, Tick

TIKR_ADDR = os.environ.get("TIKR_ADDR", "localhost:9876")
PROTOCOLS = ["TCP", "UDP", "ICMP"]
IPS = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "192.168.1.1", "192.168.1.2"]


def generate_flows(num_seconds=3, flows_per_sec=50) -> list[Tick]:
    base_time = int(time.time()) * 1_000_000_000
    ticks = []
    for sec in range(num_seconds):
        for i in range(flows_per_sec):
            ts = base_time + sec * 1_000_000_000 + i * (1_000_000_000 // flows_per_sec)
            ticks.append(Tick(
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
    return ticks


with TikrClient(TIKR_ADDR) as client:
    # Ingest network flows
    print("=== Ingesting Network Flows ===")
    ticks = generate_flows()
    count = client.ingest("network_flows", ticks)
    print(f"  total ingested: {count} flows")
    print()

    time.sleep(2)

    # Query network rollup bars
    print("=== Query Network Flow Bars ===")
    now_ns = int(time.time()) * 1_000_000_000
    bars = client.query_bars(
        "network_flows",
        dimensions={"src_ip": "10.0.0.1", "dst_ip": "10.0.0.2", "protocol": "TCP"},
        start_ns=now_ns - 60 * 1_000_000_000,
        end_ns=now_ns + 60 * 1_000_000_000,
    )

    print(f"  got {len(bars)} bars for 10.0.0.1 -> 10.0.0.2 TCP")
    for bar in bars:
        m = bar.metrics
        print(f"    bucket={bar.bucket_ts} "
              f"bytes={m.get('bytes_total', 0)} "
              f"packets={m.get('packets_total', 0)} "
              f"max_latency={m.get('max_latency_us', 0)}us "
              f"flows={bar.tick_count}")

    print()
    print("Same Tikr engine, completely different data. No code changes needed -")
    print("just a different YAML spec file in config/specs/.")
