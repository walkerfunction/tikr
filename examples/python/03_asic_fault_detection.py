#!/usr/bin/env python3
"""
Tikr Example: ASIC Hardware Metrics → Fault Detection

Real-world use case: Network switches and routers contain ASICs that emit
telemetry every few milliseconds — temperature, voltage, error counters,
packet drops. An OpenTelemetry collector scrapes these metrics and pushes
them to Tikr for 1-second rollup.

The rolled-up bars enable real-time fault detection:
  - Temperature spike (temp_max > threshold in any 1s window)
  - Rising error rate (crc_errors_total trending up across windows)
  - Voltage droop (voltage_min < safe threshold)
  - Memory pressure (memory_util_max near saturation)

Architecture:
  [ASIC] → [OTel Collector] → [Tikr gRPC] → 1s bars → [Kafka] → [Cloud TSDB]
                                                 ↓
                                          [Local fault detector]

This script simulates the OTel Collector → Tikr leg, then queries bars
to demonstrate fault detection logic.

Usage:
    python examples/python/03_asic_fault_detection.py
"""

import time
import random
import grpc
import sys
import os

STUB_DIR = os.path.join(os.path.dirname(__file__), "_generated")
sys.path.insert(0, STUB_DIR)

if not os.path.exists(os.path.join(STUB_DIR, "tikr_pb2.py")):
    print("Run 01_ingest_ticks.py first to generate stubs")
    sys.exit(1)

import tikr_pb2
import tikr_pb2_grpc

TIKR_ADDR = os.environ.get("TIKR_ADDR", "localhost:9876")
channel = grpc.insecure_channel(TIKR_ADDR)
stub = tikr_pb2_grpc.TikrStub(channel)


# ─── Simulate ASIC telemetry from a data center switch ─────────────

DEVICES = [
    {"device_id": "spine-01.dc1", "asic_id": "memory-0", "port_id": "Ethernet1/1"},
    {"device_id": "spine-01.dc1", "asic_id": "memory-0", "port_id": "Ethernet1/2"},
    {"device_id": "spine-01.dc1", "asic_id": "memory-1", "port_id": "Ethernet2/1"},
    {"device_id": "leaf-03.dc1",  "asic_id": "memory-0", "port_id": "Ethernet1/1"},
]

# Normal operating ranges for a Memory ASIC
NORMAL_TEMP_C = 65       # 65°C baseline (stored as centidegrees: 6500)
NORMAL_VOLTAGE_MV = 850  # 850mV core voltage
NORMAL_MEM_UTIL = 4500   # 45.00% in basis points


def generate_asic_telemetry(num_seconds=5, samples_per_sec=200, inject_fault=True):
    """
    Simulate OTel-scraped ASIC metrics.

    Each 'tick' represents one OTel metric scrape from a switch ASIC.
    In production, the OTel Collector would convert its internal metric
    format to Tikr's gRPC IngestRequest — a thin adapter.

    If inject_fault=True, device spine-01/port Ethernet1/1 develops a
    thermal fault at second 3 (temperature spikes, CRC errors appear).
    """
    base_time = int(time.time()) * 1_000_000_000

    for sec in range(num_seconds):
        ticks = []

        for device in DEVICES:
            for i in range(samples_per_sec):
                ts = base_time + sec * 1_000_000_000 + i * (1_000_000_000 // samples_per_sec)

                # Normal operating conditions
                temp_cdeg = NORMAL_TEMP_C * 100 + random.randint(-200, 200)  # ±2°C noise
                voltage_mv = NORMAL_VOLTAGE_MV + random.randint(-10, 10)
                crc_errors = 0
                pkt_drops = random.randint(0, 2)  # occasional normal drops
                mem_util = NORMAL_MEM_UTIL + random.randint(-300, 300)

                # Inject thermal fault on spine-01 Ethernet1/1 at second 3+
                if (inject_fault
                        and sec >= 3
                        and device["device_id"] == "spine-01.dc1"
                        and device["port_id"] == "Ethernet1/1"):
                    temp_cdeg = 9500 + random.randint(0, 500)   # 95-100°C — overheating!
                    crc_errors = random.randint(5, 50)           # CRC errors from signal degradation
                    pkt_drops = random.randint(10, 200)          # packet drops spike
                    voltage_mv = 820 + random.randint(-15, 5)    # voltage drooping under thermal stress

                ticks.append(tikr_pb2.TickData(
                    timestamp_ns=ts,
                    dimensions=device,
                    fields={
                        "temperature": temp_cdeg,
                        "voltage_mv": voltage_mv,
                        "crc_errors_delta": crc_errors,
                        "packet_drops": pkt_drops,
                        "memory_util_bps": mem_util,
                    },
                    sequence=i,
                ))

        yield tikr_pb2.IngestRequest(series="asic_metrics", ticks=ticks)
        status = "FAULT INJECTED" if inject_fault and sec >= 3 else "normal"
        print(f"  second {sec + 1}/{num_seconds}: {len(ticks)} samples [{status}]")


# ─── Ingest ────────────────────────────────────────────────────────

print("=" * 60)
print("ASIC Hardware Metrics → Tikr Edge Rollup")
print("=" * 60)
print()
print("Scenario: 4 ASIC ports on 2 switches, 200 samples/sec each")
print("          Thermal fault injected on spine-01 Eth1/1 at t+3s")
print()

print("--- Ingesting ASIC telemetry ---")
response = stub.IngestTicks(generate_asic_telemetry(num_seconds=5, samples_per_sec=200))
print(f"  total: {response.ticks_received} metric samples ingested")
print()

time.sleep(2)  # wait for rollup flush


# ─── Query bars & run fault detection ──────────────────────────────

print("--- Querying 1s Rolled-Up Bars ---")
print()

now_ns = int(time.time()) * 1_000_000_000

# Query each device/port combination
FAULT_TEMP_THRESHOLD = 8500    # 85°C — alert threshold
FAULT_CRC_THRESHOLD = 10       # >10 CRC errors/sec = fault
FAULT_VOLTAGE_MIN = 830        # <830mV = voltage droop alert

faults_detected = []

for device in DEVICES:
    bars = list(stub.QueryBars(tikr_pb2.BarQuery(
        series="asic_metrics",
        dimensions=device,
        start_ns=now_ns - 120 * 1_000_000_000,
        end_ns=now_ns + 120 * 1_000_000_000,
    )))

    label = f"{device['device_id']} / {device['port_id']}"
    print(f"  {label}: {len(bars)} bars")

    for bar in bars:
        m = bar.metrics
        temp_max = m.get("temp_max", 0)
        temp_min = m.get("temp_min", 0)
        voltage_min = m.get("voltage_min", 0)
        crc_total = m.get("crc_errors_total", 0)
        drops_total = m.get("packet_drops_total", 0)
        mem_max = m.get("memory_util_max", 0)
        samples = bar.tick_count

        # Fault detection logic
        alerts = []
        if temp_max > FAULT_TEMP_THRESHOLD:
            alerts.append(f"THERMAL({temp_max / 100:.1f}°C)")
        if crc_total > FAULT_CRC_THRESHOLD:
            alerts.append(f"CRC_ERRORS({crc_total})")
        if voltage_min < FAULT_VOLTAGE_MIN:
            alerts.append(f"VOLTAGE_DROOP({voltage_min}mV)")

        status = " ".join(alerts) if alerts else "OK"
        print(f"    t={bar.bucket_ts} temp=[{temp_min / 100:.1f}-{temp_max / 100:.1f}°C] "
              f"voltage=[{voltage_min}-{m.get('voltage_max', 0)}mV] "
              f"crc={crc_total} drops={drops_total} "
              f"mem={mem_max / 100:.1f}% samples={samples} → {status}")

        if alerts:
            faults_detected.append({
                "device": label,
                "bucket": bar.bucket_ts,
                "alerts": alerts,
            })

    print()


# ─── Fault Summary ────────────────────────────────────────────────

print("=" * 60)
print("FAULT DETECTION SUMMARY")
print("=" * 60)

if faults_detected:
    print(f"\n  {len(faults_detected)} fault(s) detected:\n")
    for f in faults_detected:
        print(f"  ⚠ {f['device']} @ bucket {f['bucket']}")
        for alert in f["alerts"]:
            print(f"      → {alert}")
    print()
    print("  Action: These bars would be pushed to Kafka (tikr-bars-asic-1s)")
    print("          and flagged by the ML anomaly hook for investigation.")
else:
    print("\n  No faults detected — all metrics within normal operating range.\n")

print()
print("--- What just happened ---")
print("1. Simulated OTel Collector pushing ASIC metrics → Tikr (gRPC streaming)")
print("2. Tikr rolled up 200 samples/sec into 1-second bars per device/port")
print("3. Queried bars and ran threshold-based fault detection")
print("4. In production: bars also flow to Kafka → Cloud TSDB for long-term trending")
print()
print("The same Tikr engine handles HFT market data, network flows, and hardware")
print("telemetry — just a different YAML spec file. No code changes needed.")
