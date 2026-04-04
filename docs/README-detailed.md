# Tikr -- Detailed Documentation

## Architecture

Tikr is an edge rollup engine that sits between high-frequency data sources and cloud infrastructure. It reduces bandwidth and storage costs by aggregating raw data points into time-bucketed bars before they leave the edge.

```
                          Tikr Edge Node
                 ┌──────────────────────────────┐
                 │                               │
  Data Source    │  gRPC Server (:9876)           │
  (market feed,  │      │                         │
   OTel, sFlow) │      ▼                         │
       ─────────┼─▶ Batcher                      │
                 │      │                         │
                 │      ├──▶ Pebble (raw ticks)   │    Kafka
                 │      │     prefix 0x01         │  ┌────────┐
                 │      ▼                         │  │        │
                 │  RollupEngine                  ├─▶│ topic   │──▶ Cloud TSDB
                 │      │                         │  │        │
                 │      ├──▶ Pebble (bars)        │  └────────┘
                 │      │     prefix 0x02         │
                 │      ▼                         │
                 │  Local Query API               │
                 │  (fault detection / ML hook)   │
                 │                               │
                 └──────────────────────────────┘
```

### Data Flow

1. **Ingest**: Client opens a gRPC streaming connection (`IngestTicks`), sending batches of `IngestRequest` messages. Each request contains a series name and a list of `TickData` points.
2. **Batcher**: Groups incoming ticks by series and flushes them to storage and the rollup engine.
3. **Storage**: Raw ticks are written to Pebble under the `0x01` prefix. Keys are big-endian encoded: `[prefix][series_id][dimension_hash][timestamp_ns]`.
4. **Rollup**: The `RollupEngine` maintains in-memory accumulators per dimension combination. When a 1-second boundary is crossed, it flushes completed bars to Pebble (prefix `0x02`) and pushes them to Kafka as **standard OTLP protobuf** (`ResourceMetrics`). Each bar metric becomes a Gauge data point with dimensions as attributes.
5. **Query**: Clients call `QueryTicks` or `QueryBars` to read raw data or rolled-up bars for a time range.

### OTLP Kafka Output

Bars on Kafka are encoded as OTLP `ResourceMetrics` protobuf. Any OTel-compatible backend (Grafana, Datadog, ClickHouse, etc.) can consume them directly without custom deserializers.

For a market ticks bar, the Kafka message contains:
- `market_ticks.open`, `market_ticks.high`, `market_ticks.low`, `market_ticks.close`, `market_ticks.volume`, `market_ticks.tick_count` as Gauge data points
- Dimension attributes (e.g., `symbol=AAPL`)
- `BucketTs` as the data point timestamp

## YAML Series Specs

Every time series is defined by a YAML spec file in `config/specs/`. No code changes are needed to add a new series -- just drop a YAML file and restart.

### Example: Market Ticks

```yaml
series: market_ticks
timestamp:
  field: timestamp_ns
  precision: nanosecond

dimensions:
  - name: symbol
    type: string

metrics:
  - name: open
    field: price
    type: int64
    aggregation: first
  - name: high
    field: price
    type: int64
    aggregation: max
  - name: low
    field: price
    type: int64
    aggregation: min
  - name: close
    field: price
    type: int64
    aggregation: last
  - name: volume
    field: quantity
    type: uint64
    aggregation: sum
  - name: tick_count
    type: uint64
    aggregation: count

granularity:
  rollup: 1s

output:
  kafka:
    topic: tikr-bars-market-1s
    encoding: otlp
    on_failure: drop
```

### Spec Fields

| Field | Description |
|---|---|
| `series` | Unique name for this time series |
| `timestamp.field` | Which field in `TickData.fields` holds the timestamp |
| `timestamp.precision` | Time precision: `nanosecond`, `microsecond`, `millisecond` |
| `dimensions[]` | Key fields used to partition rollup buckets (e.g., symbol, device_id) |
| `metrics[]` | Aggregated output fields. Each references a source `field` and an `aggregation` function |
| `granularity.rollup` | Bar duration (e.g., `1s`, `5s`, `1m`) |
| `output.kafka` | Kafka output configuration for completed bars |

## Aggregation Functions

| Function | Description | Example Use |
|----------|-------------|-------------|
| `first` | First value in the window (by timestamp) | Opening price |
| `last` | Last value in the window | Closing price |
| `min` | Minimum value | Low price, voltage floor |
| `max` | Maximum value | High price, peak temperature |
| `sum` | Sum of all values | Total volume, cumulative errors |
| `count` | Number of data points | Tick count, sample count |

## Storage Model

Tikr supports pluggable storage backends via the `Blob` interface. The backend is selected via `storage.backend` in config or the `TIKR_STORAGE_BACKEND` env var.

### Backend Comparison

| | Pebble (default) | RocksDB (`-tags rocksdb`) |
|---|---|---|
| **Language** | Pure Go | C++ via cgo (`grocksdb`) |
| **Build** | `go build` | Requires `librocksdb-dev` + C toolchain |
| **Column families** | Emulated via key prefix bytes (`0x01`, `0x02`, `0x03`) | Native column families (`NamespaceSupport`) |
| **TTL** | Application-level: lazy read filter + reaper (watermark-based `DeleteRange` tombstones) | `TTLSupport` interface implemented (currently no-op hook; reaper handles TTL). Future: native `OpenDbWithTTL` |
| **Compression** | LZ4 (default), ZSTD bottommost | Snappy L0-L1, ZSTD bottommost |
| **Bloom filters** | Built-in (point lookups only, not seeks) | Configurable bits per key (default 10) |
| **Block cache** | Pebble managed | Explicit LRU cache (default 256MB) |
| **Concurrency** | Go-native, no cgo overhead | cgo boundary on every call |
| **Best for** | Default deployments, simple builds, CI | Production with large datasets, future native TTL/compaction tuning |

### Benchmark Results (1M ticks, i9-9880H, Docker)

WriteTicks (1M ticks, 100s span):

| Backend | dims | batch | ticks/sec | ms/op | disk\_MB | compression | rollup\_bars |
|---------|------|-------|-----------|-------|---------|-------------|-------------|
| pebble  | 10   | 1000  | 426K      | 2349  | 50.7    | 1.94x       | 1,000       |
| pebble  | 10   | 5000  | 540K      | 1850  | 51.7    | 1.90x       | 1,000       |
| pebble  | 100  | 1000  | 357K      | 2803  | 51.3    | 1.91x       | 10,000      |
| pebble  | 100  | 5000  | 590K      | 1696  | 52.2    | 1.88x       | 10,000      |
| rocksdb | 10   | 1000  | 210K      | 4769  | 43.5    | 2.26x       | 1,000       |
| rocksdb | 10   | 5000  | 249K      | 4018  | 43.3    | 2.27x       | 1,000       |
| rocksdb | 100  | 1000  | 183K      | 5459  | 44.2    | 2.22x       | 10,000      |
| rocksdb | 100  | 5000  | 243K      | 4112  | 44.0    | 2.23x       | 10,000      |

ReadTicks (single dimension scan on 1M pre-populated ticks):

| Backend | dims | scope | ms/op |
|---------|------|-------|-------|
| pebble  | 10   | all   | 214   |
| pebble  | 10   | 1K    | 3.2   |
| pebble  | 10   | 10K   | 22    |
| rocksdb | 10   | all   | 320   |
| rocksdb | 10   | 1K    | 3.3   |
| rocksdb | 10   | 10K   | 34    |

ReadBars (10K pre-populated bars):

| Backend | dims | ms/op |
|---------|------|-------|
| pebble  | 10   | 14.6  |
| pebble  | 100  | 1.7   |
| rocksdb | 10   | 5.7   |
| rocksdb | 100  | 1.0   |

Reaper (1M expired ticks):

| Backend | dims | ms/op |
|---------|------|-------|
| pebble  | 10   | 1.9   |
| pebble  | 100  | 2.4   |
| rocksdb | 10   | 3.0   |
| rocksdb | 100  | 3.2   |

### Which Backend to Use

**Pebble** (default) -- best for most deployments:

- 2x write throughput (no cgo boundary overhead)
- Pure Go: simple `go build`, no `librocksdb-dev`, easier CI/CD
- Lower latency reads on full scans
- Smaller binary, no C++ dependency chain

Use when: standard HFT edge rollup, CI/CD pipelines, teams without C++ toolchain, latency-sensitive ingest paths.

**RocksDB** -- best for storage-constrained or multi-tenant:

- 15% better compression (43 MB vs 51 MB per 1M ticks -- adds up at scale)
- Native column families for true namespace isolation (ticks/bars/meta)
- 3x faster bar reads (native CF seek vs prefix scan)
- Future native TTL via `OpenDbWithTTL` per CF (no reaper needed)
- Mature tuning knobs: block cache, bloom bits, compaction styles, rate limiters

Use when: disk is expensive/limited (embedded edge boxes), multi-tenant with per-namespace TTLs, read-heavy bar queries (dashboards, signal feeds), production at scale with fine-grained tuning needs.

**Decision tree:**

```
Need simplest build + fastest writes?  → Pebble
Disk-constrained or multi-tenant?      → RocksDB
Read-heavy on rolled bars?             → RocksDB
Don't know / getting started?          → Pebble (switch later — same Blob interface)
```

Switching backends is a config change (`storage.backend: rocksdb`), not a rewrite.

### TTL Flow by Backend

| Step | Pebble | RocksDB (current) |
|------|--------|-------------------|
| **1. Read filter** | Lazy: skip keys older than `now - TTL` before decoding value | Same |
| **2. Reaper** | Every 10min: discover groups via `SeekGE` hopping | Same (reaper runs because `SetTTL` is a no-op) |
| **3. Tombstoning** | `DeleteRange(last_watermark, new_cutoff+1)` per group | Same |
| **4. Watermark** | `reap:hwm:<prefix>` in meta prevents overlapping tombstones | Same |
| **5. Disk reclaim** | Pebble compaction merges SSTs, discards tombstoned keys | RocksDB compaction (same mechanism) |
| **6. Future** | -- | Native `OpenDbWithTTL` per column family; reaper skipped |

### Key Encoding

All keys use big-endian encoding for correct lexicographic ordering:

| Prefix | Byte | Contents |
|--------|------|----------|
| Ticks | `0x01` | `[0x01][series_id][dim_hash][timestamp_ns]` |
| Rollup bars | `0x02` | `[0x02][series_id][dim_hash][bucket_ts]` |
| Metadata | `0x03` | `[0x03][key_name]` |

### TTL and Size Limits

Configured in `config/default.yaml`:

```yaml
storage:
  data_dir: /data
  ticks:
    ttl: 6h          # Raw ticks kept for 6 hours (min: 1h, max: 24h)
    max_size_gb: 50
  rollup:
    ttl: 12h         # Bars kept for 12 hours (min: 1h, max: 24h)
    max_size_gb: 5
```

**How TTL works (Pebble backend):**

Pebble does not have native TTL support, so Tikr enforces TTL at the application level using two complementary mechanisms:

1. **Lazy read filter** -- Every query checks the timestamp embedded in each data key against the TTL cutoff (`now - TTL`). Keys older than the cutoff are skipped before the value is decoded. This gives immediate read correctness with zero write-path overhead.

2. **Reaper (background tombstoning)** -- A background goroutine runs every 10 minutes to reclaim disk space from expired data. It discovers all `(series_id, dim_hash)` groups by hopping through the data keyspace with `SeekGE` (no registry needed). For each group, it issues a `DeleteRange` tombstone covering the range from the last watermark to the new cutoff. A per-prefix watermark (`reap:hwm:<prefix>`) stored in the meta prefix tracks the last-reaped cutoff, ensuring each cycle only tombstones the newly-expired slice. Pebble's compaction reclaims the underlying disk during SSTable merges.

**Backends with native TTL** (e.g., RocksDB with `TTLSupport`): The reaper is not started. Expiry is handled by the backend's own compaction.

**Size limits** (`max_size_gb`) are advisory only -- neither Pebble nor the reaper enforces size-based compaction.

## Use Cases

### 1. HFT Market Data

Spec: `config/specs/market_ticks.yaml`

Ingest raw trade ticks (price, quantity) at nanosecond resolution. Tikr produces OHLCV bars (open, high, low, close, volume) every second. Bars are forwarded to Kafka for cloud analytics while the edge retains raw ticks for local replay.

**Dimensions**: `symbol`
**Example**: `python examples/python/01_ingest_ticks.py`

### 2. ASIC Hardware Fault Detection

Spec: `config/specs/asic_metrics.yaml`

Network switches emit ASIC telemetry (temperature, voltage, CRC errors, packet drops) via OpenTelemetry. Tikr rolls up hundreds of samples per second into 1s bars. Local fault detection queries bars in real time to catch thermal spikes, voltage droops, and rising error rates before they reach the cloud.

**Dimensions**: `device_id`, `asic_id`, `port_id`
**Example**: `python examples/python/03_asic_fault_detection.py`

### 3. Network Flow Telemetry

Spec: `config/specs/network_flows.yaml`

sFlow or NetFlow data arrives at high volume from routers. Tikr aggregates byte/packet counts per source-destination pair into 1s summaries, enabling edge-level DDoS detection and traffic profiling.

**Dimensions**: `src_ip`, `dst_ip`, `protocol`
**Example**: `python examples/python/02_network_flows.py`

## Configuration Reference

The main configuration file is `config/default.yaml`:

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `server` | `grpc_port` | `9876` | gRPC ingest and query port |
| `server` | `http_port` | `9877` | HTTP health/debug port |
| `storage` | `data_dir` | `/data` | Pebble data directory |
| `storage.ticks` | `ttl` | `6h` | Raw tick retention |
| `storage.ticks` | `max_size_gb` | `50` | Max raw tick storage |
| `storage.rollup` | `ttl` | `12h` | Rolled-up bar retention |
| `storage.rollup` | `max_size_gb` | `5` | Max bar storage |
| `kafka` | `brokers` | `["kafka:9092"]` | Kafka broker addresses |
| `telemetry` | `enabled` | `true` | Enable OpenTelemetry |
| `telemetry` | `service_name` | `tikr` | OTel service name |
| `telemetry.prometheus` | `port` | `9878` | Prometheus metrics port |
| | `specs_dir` | `/etc/tikr/specs` | Directory for YAML spec files |

## Docker Deployment

### Development

```bash
# Start Tikr + Kafka
docker compose -f docker/docker-compose.yml up -d

# View logs
docker compose -f docker/docker-compose.yml logs -f

# Stop
docker compose -f docker/docker-compose.yml down
```

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make build` | Build the Tikr binary (inside Docker) |
| `make test` | Run all tests with race detector (inside Docker) |
| `make lint` | Run golangci-lint (inside Docker) |
| `make bench` | Run benchmarks (inside Docker) |
| `make proto` | Regenerate protobuf Go stubs |
| `make docker` | Build production Docker image |
| `make docker-up` | Start full stack (Tikr + Kafka) |
| `make docker-down` | Stop full stack |

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 9876 | gRPC | Ingest and Query API |
| 9877 | HTTP | Health and debug endpoints |
| 9878 | HTTP | OTel metrics |
| 9092 | TCP | Kafka (OTLP bar output) |

## gRPC API Reference

Proto definition: `proto/tikr.proto`

### Service: `Tikr`

| RPC | Type | Description |
|-----|------|-------------|
| `IngestTicks` | client-streaming | Stream `IngestRequest` messages; returns `IngestResponse` with count |
| `QueryTicks` | server-streaming | Query raw ticks by series, dimensions, and time range |
| `QueryBars` | server-streaming | Query rolled-up bars by series, dimensions, and time range |
| `ListSeries` | unary | List all configured series and their specs |
| `GetInfo` | unary | Get server version, uptime, and series info |

### Key Message Types

- **`IngestRequest`**: `{series, ticks[]}` -- batch of ticks for a named series
- **`TickData`**: `{timestamp_ns, dimensions{}, fields{}, sequence}` -- single data point
- **`BarData`**: `{series, bucket_ts, dimensions{}, metrics{}, first_timestamp, last_timestamp, tick_count}` -- rolled-up bar (gRPC query response; Kafka output uses OTLP encoding)
- **`TickQuery`**: `{series, dimensions{}, start_ns, end_ns, limit}`
- **`BarQuery`**: `{series, dimensions{}, start_ns, end_ns}`

## Contributing

1. Fork the repo and create a feature branch
2. All builds and tests run inside Docker -- no local Go installation required
3. Run `make test` to verify your changes pass
4. Run `make lint` to check code style
5. Submit a pull request

All contributions are welcome under the Apache 2.0 license.
