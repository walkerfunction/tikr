# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
