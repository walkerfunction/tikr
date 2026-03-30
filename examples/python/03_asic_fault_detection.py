#!/usr/bin/env python3
"""
Tikr Example: ASIC Hardware Metrics -> Fault Detection

Real-world use case: Network switches contain ASICs that emit telemetry
every few milliseconds. An OTel collector scrapes these metrics and pushes
them to Tikr for 1-second rollup. Rolled-up bars enable fault detection:
  - Temperature spike (temp_max > threshold)
  - Rising error rate (crc_errors_total trending up)
  - Voltage droop (voltage_min < safe threshold)

Architecture:
  [ASIC] -> [OTel Collector] -> [Tikr gRPC] -> 1s bars -> [Kafka] -> [Cloud]
                                                    |
                                             [Local fault detector]

Usage:
    pip install -e python-sdk/
    python examples/python/03_asic_fault_detection.py
"""

import os
import random
import time

from tikr import TikrClient, Tick

TIKR_ADDR = os.environ.get("TIKR_ADDR", "localhost:9876")

DEVICES = [
    {"device_id": "spine-01.dc1", "asic_id": "memory-0", "port_id": "Ethernet1/1"},
    {"device_id": "spine-01.dc1", "asic_id": "memory-0", "port_id": "Ethernet1/2"},
    {"device_id": "spine-01.dc1", "asic_id": "memory-1", "port_id": "Ethernet2/1"},
    {"device_id": "leaf-03.dc1", "asic_id": "memory-0", "port_id": "Ethernet1/1"},
]

NORMAL_TEMP_C = 65
NORMAL_VOLTAGE_MV = 850
NORMAL_MEM_UTIL = 4500

FAULT_TEMP_THRESHOLD = 8500
FAULT_CRC_THRESHOLD = 10
FAULT_VOLTAGE_MIN = 830


def generate_asic_telemetry(
    num_seconds=5, samples_per_sec=200, inject_fault=True
) -> list[Tick]:
    """Simulate OTel-scraped ASIC metrics with optional thermal fault injection."""
    base_time = int(time.time()) * 1_000_000_000
    ticks = []

    for sec in range(num_seconds):
        for device in DEVICES:
            for i in range(samples_per_sec):
                ts = (
                    base_time
                    + sec * 1_000_000_000
                    + i * (1_000_000_000 // samples_per_sec)
                )

                temp_cdeg = NORMAL_TEMP_C * 100 + random.randint(-200, 200)
                voltage_mv = NORMAL_VOLTAGE_MV + random.randint(-10, 10)
                crc_errors = 0
                pkt_drops = random.randint(0, 2)
                mem_util = NORMAL_MEM_UTIL + random.randint(-300, 300)

                # Thermal fault on spine-01 Ethernet1/1 at second 3+
                if (
                    inject_fault
                    and sec >= 3
                    and device["device_id"] == "spine-01.dc1"
                    and device["port_id"] == "Ethernet1/1"
                ):
                    temp_cdeg = 9500 + random.randint(0, 500)
                    crc_errors = random.randint(5, 50)
                    pkt_drops = random.randint(10, 200)
                    voltage_mv = 820 + random.randint(-15, 5)

                ticks.append(
                    Tick(
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
                    )
                )

        status = "FAULT INJECTED" if inject_fault and sec >= 3 else "normal"
        print(
            f"  second {sec + 1}/{num_seconds}: {len(DEVICES) * samples_per_sec} samples [{status}]"
        )

    return ticks


with TikrClient(TIKR_ADDR) as client:
    print("=" * 60)
    print("ASIC Hardware Metrics -> Tikr Edge Rollup")
    print("=" * 60)
    print()
    print("Scenario: 4 ASIC ports on 2 switches, 200 samples/sec each")
    print("          Thermal fault injected on spine-01 Eth1/1 at t+3s")
    print()

    # Ingest
    print("--- Ingesting ASIC telemetry ---")
    ticks = generate_asic_telemetry(num_seconds=5, samples_per_sec=200)
    count = client.ingest("asic_metrics", ticks, batch_size=5000)
    print(f"  total: {count} metric samples ingested")
    print()

    time.sleep(2)

    # Query and detect faults
    print("--- Querying 1s Rolled-Up Bars ---")
    print()

    now_ns = int(time.time()) * 1_000_000_000
    faults_detected = []

    for device in DEVICES:
        bars = client.query_bars(
            "asic_metrics",
            dimensions=device,
            start_ns=now_ns - 120 * 1_000_000_000,
            end_ns=now_ns + 120 * 1_000_000_000,
        )

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

            alerts = []
            if temp_max > FAULT_TEMP_THRESHOLD:
                alerts.append(f"THERMAL({temp_max / 100:.1f}C)")
            if crc_total > FAULT_CRC_THRESHOLD:
                alerts.append(f"CRC_ERRORS({crc_total})")
            if voltage_min < FAULT_VOLTAGE_MIN:
                alerts.append(f"VOLTAGE_DROOP({voltage_min}mV)")

            status = " ".join(alerts) if alerts else "OK"
            print(
                f"    t={bar.bucket_ts} temp=[{temp_min / 100:.1f}-{temp_max / 100:.1f}C] "
                f"voltage=[{voltage_min}-{m.get('voltage_max', 0)}mV] "
                f"crc={crc_total} drops={drops_total} "
                f"mem={mem_max / 100:.1f}% samples={bar.tick_count} -> {status}"
            )

            if alerts:
                faults_detected.append(
                    {"device": label, "bucket": bar.bucket_ts, "alerts": alerts}
                )

        print()

    # Summary
    print("=" * 60)
    print("FAULT DETECTION SUMMARY")
    print("=" * 60)

    if faults_detected:
        print(f"\n  {len(faults_detected)} fault(s) detected:\n")
        for f in faults_detected:
            print(f"  ! {f['device']} @ bucket {f['bucket']}")
            for alert in f["alerts"]:
                print(f"      -> {alert}")
        print()
        print("  Action: These bars would be pushed to Kafka (tikr-bars-asic-1s)")
        print("          and flagged by the ML anomaly hook for investigation.")
    else:
        print("\n  No faults detected -- all metrics within normal operating range.\n")

    print()
    print("The same Tikr engine handles HFT market data, network flows, and hardware")
    print("telemetry -- just a different YAML spec file. No code changes needed.")
