# Tikr -- Agent Instructions

For full project context, architecture, types, and data flow, see [docs/README-agents.md](docs/README-agents.md).

## Rules

- **Never commit to master.** Always use a feature branch + PR.
- **Go 1.24**, pure Go (no CGO). Module: `github.com/walkerfunction/tikr`
- Run `go test -race ./pkg/...` before committing.
- Use `make docker-up` to test the full stack (Tikr + Kafka).
- Git email: `walkerfunction@gmail.com`

## Key Architecture Decisions

- **Schema-agnostic**: the DB doesn't know OHLCV. YAML specs define dimensions, metrics, and aggregation functions. The engine mechanically applies them.
- **OTLP-native Kafka output**: bars are encoded as standard OpenTelemetry protobuf, consumable by any OTel-compatible backend.
- **Two-layer TTL**: lazy read filter (immediate correctness) + registry-free background reaper with incremental `DeleteRange` tombstones (disk reclamation). Reaper discovers groups via `SeekGE` hopping — O(groups) seeks, no writer-side bookkeeping. Per-prefix watermark prevents overlapping tombstones.
- **Fire-and-forget Kafka**: if Kafka is down, bars are dropped, not queued. Counter tracks drops.
- **Pre-allocated OTel attribute sets** on hot paths to avoid per-call allocations.

## Ports

| Port | Purpose |
|------|---------|
| 9876 | gRPC (ingest + query) |
| 9877 | HTTP (health/debug) |
| 9878 | OTel metrics |
