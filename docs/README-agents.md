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
  storage/          -- Pebble engine, reader, writer, TTL (lazy filter + reaper)
  ingest/           -- gRPC ingest server, Batcher, Pipeline
  agg/              -- RollupEngine (aggregation logic), BarHook interface
  query/            -- gRPC query server (ticks + bars)
  pb/               -- generated protobuf/gRPC code (from proto/tikr.proto)
  config/           -- YAML config and spec loader
  output/           -- Kafka producer, OTLP bar encoding
  telemetry/        -- OpenTelemetry metrics (OTel SDK)
proto/              -- tikr.proto source
config/
  default.yaml      -- server config
  specs/            -- series spec YAML files (market_ticks, asic_metrics, network_flows)
docker/             -- Dockerfile, docker-compose.yml (Tikr + Kafka KRaft)
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
| `KafkaProducer` | `pkg/output` | Async Kafka writer, encodes bars as OTLP protobuf |
| `Metrics` | `pkg/telemetry` | OTel instruments (counters, histograms) for pipeline observability |
| `TTLConfig` | `pkg/storage` | Lazy read-time TTL filter config |
| `Reaper` | `pkg/storage` | Background tombstone writer for disk reclamation |

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
    └──> BarHook --> KafkaProducer --> OTLP protobuf on Kafka topic
```

## Storage

- **Engine**: Pebble (CockroachDB's LSM store, pure Go)
- **Key encoding**: big-endian, lexicographically ordered
- **Prefix bytes**:
  - `0x01` -- raw ticks: `[0x01][series_id:2][dim_hash:8][timestamp_ns:8][seq:4]`
  - `0x02` -- rollup bars: `[0x02][series_id:2][dim_hash:8][bucket_ts:8]`
  - `0x03` -- metadata: `[0x03][key_name]`
- **Reaper watermarks**: `reap:hwm:<prefix>` keys in meta store the last-reaped cutoff per data prefix

## TTL Enforcement

Two-layer design:

1. **Lazy read filter** -- `NewReaderWithTTL()` checks the timestamp in each key and skips expired entries before decoding values. Zero write-path cost.
2. **Reaper** (every 10min) -- discovers `(series_id, dim_hash)` groups by hopping through the data keyspace via `SeekGE` (O(groups) seeks). For each group, issues `DeleteRange([prefix][sid][dh][last_cutoff], [prefix][sid][dh][new_cutoff+1])` — incremental, non-overlapping tombstones. A per-prefix watermark in meta tracks progress across cycles. Pebble's `DeleteRange` is `[start, end)` exclusive, so `cutoff+1` makes it inclusive of keys at exactly the cutoff. Pebble's compaction discards tombstoned keys during SSTable merges.

Config: `storage.ticks.ttl` and `storage.rollup.ttl` in `config/default.yaml` (min 1h, max 24h).

## Kafka Output

- **Encoding**: OTLP protobuf (standard OpenTelemetry format)
- **Delivery**: async fire-and-forget. If Kafka is down, bars are dropped + counter incremented
- **Topic**: configured per series in spec YAML (`output.kafka.topic`)
- **Key**: dimension values for Kafka partitioning
- **Encoder**: `pkg/output/otlp.go` -- `BarToOTLP()` converts bars to OTLP `ResourceMetrics`

## Observability

OTel SDK instruments wired through the full pipeline:

| Metric | Type | Where |
|--------|------|-------|
| `tikr.ingest.ticks_total` | Counter | Pipeline.Ingest() |
| `tikr.ingest.batch_size` | Histogram | Server.IngestTicks() |
| `tikr.agg.bars_flushed_total` | Counter | Pipeline.consumeBars() |
| `tikr.output.kafka_writes_total` | Counter | KafkaProducer.OnBarFlushed() |
| `tikr.output.kafka_drops_total` | Counter | KafkaProducer async Completion callback |
| `tikr.query.requests_total` | Counter | QueryTicks/QueryBars (with "type" attr) |
| `tikr.query.latency_ms` | Histogram | QueryTicks/QueryBars |

Pre-allocated attribute sets used on hot paths to avoid per-call heap allocations.

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
make docker-clean  # stop + remove containers, volumes, images
```

No local Go toolchain needed. The dev image (`docker/Dockerfile.dev`) has everything.

## Dependencies

| Dependency | Purpose |
|------------|---------|
| `cockroachdb/pebble v1.1.5` | Embedded LSM storage |
| `google.golang.org/grpc v1.70.0` | gRPC server and client |
| `google.golang.org/protobuf v1.36.11` | Protocol buffer runtime |
| `gopkg.in/yaml.v3 v3.0.1` | YAML config and spec parsing |
| `segmentio/kafka-go v0.4.50` | Kafka producer |
| `go.opentelemetry.io/otel v1.32.0` | OTel metrics SDK |
| `go.opentelemetry.io/proto/otlp v1.0.0` | OTLP protobuf types for bar encoding |

## Current State

- **Phase 5 complete**: ingest, rollup, query, Kafka OTLP output, OTel instrumentation, TTL enforcement
- **Docker**: multi-stage build (distroless), compose with Kafka KRaft
- **CI**: GitHub Actions (lint, test, build, docker)
- **Release**: GHCR Docker push on version tags

## Conventions

- All numeric values use fixed-point integers (e.g., price in cents, temperature in centidegrees, utilization in basis points)
- Timestamps are always nanoseconds since Unix epoch (`uint64`)
- Dimensions are string-typed key-value pairs
- Fields/metrics are `int64` or `uint64`
- Branch workflow: never commit to master, always feature branch + PR

## Series Specs

Located in `config/specs/`. Each YAML file defines one series:

| Spec | Series Name | Dimensions | Rollup |
|------|-------------|------------|--------|
| `market_ticks.yaml` | `market_ticks` | symbol | 1s |
| `asic_metrics.yaml` | `asic_metrics` | device_id, asic_id, port_id | 1s |
| `network_flows.yaml` | `network_flows` | src_ip, dst_ip, protocol | 1s |
