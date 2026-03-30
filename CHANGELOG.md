# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [v0.2.0] - 2026-03-29

### Added

- OTLP-native Kafka output -- bars encoded as standard OpenTelemetry protobuf
- OpenTelemetry instrumentation across the full pipeline (ticks in, bars flushed, Kafka writes/drops, query latency)
- Pre-allocated OTel attribute sets for zero-allocation hot paths
- Package-level doc comments for pkg.go.dev

### Changed

- Kafka bar encoding switched from custom protobuf to standard OTLP format
- Kafka producer accepts `*telemetry.Metrics` for async drop tracking
- Query server records request counts and latency via OTel

### Fixed

- Double-counting of ticks_total (removed duplicate counter in ingest server)
- Kafka async drop metrics not firing (restructured producer initialization)
- Batcher race condition on shutdown

## [v0.1.0] - 2026-03-29

### Added

- gRPC client-streaming ingest for high-throughput time series data
- Schema-agnostic YAML spec system for defining arbitrary time series
- Pebble-backed embedded storage engine (pure Go, no CGO dependency)
- Sub-second rollup aggregation engine (first, last, min, max, sum, count)
- Multi-dimensional rollups per any combination of dimension keys
- gRPC query service for retrieving rolled-up bars
- Kafka output for forwarding compressed summaries to cloud
- Prometheus telemetry integration
- Docker and Docker Compose setup (Tikr + Kafka)
- Python SDK and example scripts for ingest and query
- Example specs: market ticks (OHLCV), ASIC metrics, network flows
- Unit tests, integration tests, and benchmarks
- Makefile with Docker-based build, test, lint, and bench targets
