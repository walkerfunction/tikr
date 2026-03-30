# Contributing to Tikr

Thanks for your interest in contributing to Tikr! This guide covers everything you need to get started.

## Prerequisites

- **Docker** -- all builds, tests, and linting run inside Docker containers. No local Go toolchain required.

## Running Tests

```bash
# Unit tests (runs inside Docker)
make test

# Integration tests (requires Docker Compose stack)
make integration-test

# Linting
make lint

# Benchmarks
make bench
```

## Code Style

- Standard Go conventions. Run `gofmt` on all code.
- Lint checks run via `golangci-lint` inside Docker (`make lint`).

## Pull Request Process

1. **Fork** the repository and create a feature branch from `master`.
2. **Write tests** for any new functionality.
3. **Run `make test`** and ensure all tests pass.
4. **Open a PR** against `master` with a clear description of the change.
5. A maintainer will review and merge once CI is green.

## Project Structure

```
cmd/tikr/       -- main entry point
pkg/            -- core library packages
  agg/          -- rollup aggregation engine
  config/       -- YAML spec loading
  core/         -- shared types
  ingest/       -- gRPC streaming ingest
  output/       -- Kafka output
  query/        -- gRPC query service
  storage/      -- Pebble storage layer
  telemetry/    -- OpenTelemetry metrics
tests/          -- integration tests
proto/          -- protobuf definitions
docker/         -- Dockerfiles and Compose
config/         -- runtime configuration and specs
```

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
