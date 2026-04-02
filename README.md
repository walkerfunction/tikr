# Tikr

[![CI](https://github.com/walkerfunction/tikr/actions/workflows/ci.yml/badge.svg)](https://github.com/walkerfunction/tikr/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/walkerfunction/tikr.svg)](https://pkg.go.dev/github.com/walkerfunction/tikr)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**On-prem edge rollup engine for high-frequency time series.** Tikr ingests millions of data points per second via gRPC, rolls them into 1-second bars on the edge, and forwards compressed summaries to Kafka for cloud storage.

```
[Data Source] ──gRPC──▶ [Tikr on-prem] ──OTLP bars──▶ [Kafka] ──▶ [Any OTel backend]
   100k pts/s             Pebble store                 standard      Grafana, Datadog,
                          rollup engine                format        ClickHouse, etc.
```

## Quick Start

```bash
# Start Tikr + Kafka
docker compose -f docker/docker-compose.yml up -d

# Install the Python SDK and run the example
pip install -e python-sdk/
python examples/python/01_ingest_ticks.py
```

## Features

- **Schema-agnostic YAML specs** -- define any time series with a YAML file, no code changes
- **OTLP-native output** -- bars are encoded as standard OTLP protobuf on Kafka, consumable by any OTel-compatible backend
- **gRPC streaming ingest** -- client-streaming RPC handles bursty, high-throughput writes
- **Pebble storage** -- embedded LSM engine (pure Go, no CGO dependency for builds)
- **Sub-second rollups** -- first, last, min, max, sum, count aggregations into 1s bars
- **OTel instrumentation** -- ticks in, bars out, query latency, Kafka drops all tracked via OpenTelemetry
- **Multi-dimensional** -- roll up per any combination of dimension keys
- **Pre-wired ML hook** -- query bars locally for real-time anomaly detection at the edge
- **Pure Go** -- single static binary, minimal operational overhead

## Use Cases

| Use Case | Spec File | Dimensions |
|---|---|---|
| HFT market data (OHLCV bars) | `config/specs/market_ticks.yaml` | symbol |
| ASIC hardware fault detection | `config/specs/asic_metrics.yaml` | device_id, asic_id, port_id |
| Network flow telemetry | `config/specs/network_flows.yaml` | src_ip, dst_ip, protocol |

## Data Retention

Tikr is an edge node -- all local data is ephemeral with configurable TTL (1h--24h).

**Two-layer TTL enforcement (Pebble and backends without native TTL):**

1. **Lazy read filter** -- on every query, the Reader checks the timestamp embedded in each key and skips expired entries before decoding the value. Zero write-path cost, immediate correctness.

2. **Reaper** -- a background goroutine (every 10min) discovers `(series_id, dim_hash)` groups by hopping through the data keyspace via `SeekGE`, then issues incremental `DeleteRange` tombstones per group. A per-prefix watermark tracks the last-reaped cutoff, so each cycle only tombstones the newly-expired slice -- no overlapping tombstones, no write amplification. Compaction discards tombstoned keys during SSTable merges.

The reaper operates on the `Blob` interface and works with any storage backend. Backends that implement native `TTLSupport` (e.g., RocksDB with TTL compaction) handle expiry themselves -- the reaper is not started.

```yaml
# config/default.yaml
storage:
  ticks:
    ttl: 6h         # min: 1h, max: 24h
    max_size_gb: 50
  rollup:
    ttl: 12h        # min: 1h, max: 24h
    max_size_gb: 5
```

## Documentation

- **[Detailed README](docs/README-detailed.md)** -- architecture, configuration, API reference
- **[Agent Context](docs/README-agents.md)** -- for AI coding agents working on this repo

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 9876 | gRPC | Ingest + Query API |
| 9877 | HTTP | Health / debug |
| 9878 | HTTP | OTel metrics |

## License

Apache 2.0 -- see [LICENSE](LICENSE).
