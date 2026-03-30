# Tikr

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**On-prem edge rollup engine for high-frequency time series.** Tikr ingests millions of data points per second via gRPC, rolls them into 1-second bars on the edge, and forwards compressed summaries to Kafka for cloud storage.

```
[Data Source] ──gRPC──▶ [Tikr on-prem] ──1s bars──▶ [Kafka] ──▶ [Cloud TSDB]
   100k pts/s             Pebble store               10x less      long-term
                          rollup engine               bandwidth     trending
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
- **gRPC streaming ingest** -- client-streaming RPC handles bursty, high-throughput writes
- **Pebble storage** -- embedded LSM engine (pure Go, no CGO dependency for builds)
- **Sub-second rollups** -- first, last, min, max, sum, count aggregations into 1s bars
- **Multi-dimensional** -- roll up per any combination of dimension keys
- **Pre-wired ML hook** -- query bars locally for real-time anomaly detection at the edge
- **Pure Go** -- single static binary, minimal operational overhead
- **Docker-first** -- all builds, tests, and deployments run in containers

## Use Cases

| Use Case | Spec File | Dimensions |
|---|---|---|
| HFT market data (OHLCV bars) | `config/specs/market_ticks.yaml` | symbol |
| ASIC hardware fault detection | `config/specs/asic_metrics.yaml` | device_id, asic_id, port_id |
| Network flow telemetry | `config/specs/network_flows.yaml` | src_ip, dst_ip, protocol |

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
