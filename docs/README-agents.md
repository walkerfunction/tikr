# Tikr -- Agent Context

Context file for AI coding agents (Claude, Copilot, Cursor, etc.) working on this repo.

## Project

- **What**: On-prem edge rollup engine for high-frequency time series
- **Repo**: https://github.com/walkerfunction/tikr
- **Module**: `github.com/walkerfunction/tikr`
- **Language**: Go 1.24, pure Go (no CGO required for builds)
- **License**: Apache 2.0

## Package Layout

```
cmd/tikr/           -- main entry point
pkg/
  core/             -- domain types: Tick, Bar, SeriesSpec, MetricDef
  storage/          -- Pebble engine, reader, writer
  ingest/           -- gRPC ingest server, Batcher, Pipeline
  agg/              -- RollupEngine (aggregation logic)
  query/            -- gRPC query server (ticks + bars)
  pb/               -- generated protobuf/gRPC code (from proto/tikr.proto)
  config/           -- YAML config and spec loader
  output/           -- Kafka bar output (planned)
  telemetry/        -- OpenTelemetry + Prometheus metrics
proto/              -- tikr.proto source
config/
  default.yaml      -- server config
  specs/            -- series spec YAML files (market_ticks, asic_metrics, network_flows)
docker/             -- Dockerfile, Dockerfile.dev, docker-compose.yml
examples/python/    -- Python gRPC examples (ingest, query, fault detection)
benchmarks/         -- Go benchmarks
```

## Key Types

| Type | Package | Purpose |
|------|---------|---------|
| `Tick` | `pkg/core` | Single raw data point (timestamp, dimensions, fields) |
| `Bar` | `pkg/core` | Rolled-up time bucket (metrics map, tick count, timestamps) |
| `SeriesSpec` | `pkg/core` | Parsed YAML spec defining dimensions, metrics, aggregations |
| `RollupEngine` | `pkg/agg` | Maintains accumulators, flushes completed bars on window boundary |
| `Pipeline` | `pkg/ingest` | Wires gRPC ingest to Batcher to storage + rollup |
| `Batcher` | `pkg/ingest` | Groups incoming ticks, flushes to storage and RollupEngine |

## Data Flow

```
gRPC IngestTicks (client-streaming)
    |
    v
Batcher (groups by series, flushes batches)
    |
    ├──> Pebble write (prefix 0x01 = raw ticks)
    |
    v
RollupEngine (in-memory accumulators per dimension combo)
    |
    ├──> Pebble write (prefix 0x02 = rolled-up bars)
    └──> BarChan --> Kafka output (planned)
```

## Storage

- **Engine**: Pebble (CockroachDB's LSM store, pure Go)
- **Key encoding**: big-endian, lexicographically ordered
- **Prefix bytes**:
  - `0x01` -- raw ticks: `[0x01][series_id][dim_hash][timestamp_ns]`
  - `0x02` -- rollup bars: `[0x02][series_id][dim_hash][bucket_ts]`
  - `0x03` -- metadata: `[0x03][key_name]`
- **TTL**: ticks 6h, bars 12h (configurable in default.yaml)

## Proto

- Source: `proto/tikr.proto`
- Generated output: `pkg/pb/`
- Regenerate: `make proto`
- Key RPCs: `IngestTicks` (client-stream), `QueryTicks` (server-stream), `QueryBars` (server-stream), `ListSeries`, `GetInfo`

## Aggregation Functions

`first`, `last`, `min`, `max`, `sum`, `count` -- defined per metric in YAML spec files.

## Build and Test

All builds and tests run inside Docker:

```
make test       # go test -race -v ./pkg/...
make build      # produces bin/tikr
make lint       # golangci-lint
make bench      # benchmarks
make proto      # regenerate protobuf stubs
make docker-up  # start tikr + kafka via docker compose
```

No local Go toolchain needed. The dev image (`docker/Dockerfile.dev`) has everything.

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `cockroachdb/pebble v1.1.5` | Embedded LSM storage |
| `google.golang.org/grpc v1.70.0` | gRPC server and client |
| `google.golang.org/protobuf v1.36.11` | Protocol buffer runtime |
| `gopkg.in/yaml.v3 v3.0.1` | YAML config and spec parsing |

## Current State

- **Phase 2 complete**: ingest + rollup + query all working end-to-end
- **Kafka output**: not yet wired (output package exists but is a stub)
- **Telemetry**: OpenTelemetry + Prometheus metrics scaffolded
- **Python examples**: 3 examples (market ticks, network flows, ASIC fault detection)

## Conventions

- All numeric values use fixed-point integers (e.g., price in cents, temperature in centidegrees, utilization in basis points)
- Timestamps are always nanoseconds since Unix epoch (`uint64`)
- Dimensions are string-typed key-value pairs
- Fields/metrics are `int64` or `uint64`

## Series Specs

Located in `config/specs/`. Each YAML file defines one series:

| Spec | Series Name | Dimensions | Rollup |
|------|-------------|------------|--------|
| `market_ticks.yaml` | `market_ticks` | symbol | 1s |
| `asic_metrics.yaml` | `asic_metrics` | device_id, asic_id, port_id | 1s |
| `network_flows.yaml` | `network_flows` | src_ip, dst_ip, protocol | 1s |
