package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics holds all OTel instruments for the Tikr service.
// The underlying MeterProvider can be configured with any OTel exporter
// (Prometheus, OTLP/gRPC, OTLP/HTTP, stdout, Kafka, etc.).
type Metrics struct {
	provider *sdkmetric.MeterProvider

	// Ingest metrics
	TicksTotal Counter                // tikr.ingest.ticks_total
	BatchSize  metric.Float64Histogram // tikr.ingest.batch_size

	// Aggregation metrics
	BarsFlushedTotal Counter // tikr.agg.bars_flushed_total

	// Output metrics
	KafkaWritesTotal Counter // tikr.output.kafka_writes_total
	KafkaDropsTotal  Counter // tikr.output.kafka_drops_total

	// Query metrics
	QueryRequestsTotal Counter                // tikr.query.requests_total (use "type" attr)
	QueryLatencyMs     metric.Float64Histogram // tikr.query.latency_ms
}

// Counter is a convenience alias for the OTel Int64Counter instrument.
type Counter = metric.Int64Counter

// Option configures the Metrics setup.
type Option func(*options)

type options struct {
	readers []sdkmetric.Reader
}

// WithReader adds an OTel metric reader (e.g., Prometheus exporter, OTLP exporter, periodic reader).
func WithReader(r sdkmetric.Reader) Option {
	return func(o *options) {
		o.readers = append(o.readers, r)
	}
}

// NewMetrics creates a Metrics instance with the given OTel readers/exporters.
// If no readers are provided, metrics are collected but not exported (useful for tests).
func NewMetrics(serviceName string, opts ...Option) (*Metrics, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	providerOpts := make([]sdkmetric.Option, 0, len(o.readers))
	for _, r := range o.readers {
		providerOpts = append(providerOpts, sdkmetric.WithReader(r))
	}

	provider := sdkmetric.NewMeterProvider(providerOpts...)
	return registerInstruments(provider, serviceName)
}

// registerInstruments creates all OTel instruments on the given provider.
func registerInstruments(provider *sdkmetric.MeterProvider, serviceName string) (*Metrics, error) {
	meter := provider.Meter(serviceName)

	ticksTotal, err := meter.Int64Counter("tikr.ingest.ticks_total",
		metric.WithDescription("Total ticks ingested"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating tikr.ingest.ticks_total: %w", err)
	}

	batchSize, err := meter.Float64Histogram("tikr.ingest.batch_size",
		metric.WithDescription("Batch sizes"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating tikr.ingest.batch_size: %w", err)
	}

	barsFlushedTotal, err := meter.Int64Counter("tikr.agg.bars_flushed_total",
		metric.WithDescription("Total bars flushed"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating tikr.agg.bars_flushed_total: %w", err)
	}

	kafkaWritesTotal, err := meter.Int64Counter("tikr.output.kafka_writes_total",
		metric.WithDescription("Bars sent to Kafka"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating tikr.output.kafka_writes_total: %w", err)
	}

	kafkaDropsTotal, err := meter.Int64Counter("tikr.output.kafka_drops_total",
		metric.WithDescription("Bars dropped due to Kafka failure"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating tikr.output.kafka_drops_total: %w", err)
	}

	queryRequestsTotal, err := meter.Int64Counter("tikr.query.requests_total",
		metric.WithDescription("Query requests total"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating tikr.query.requests_total: %w", err)
	}

	queryLatencyMs, err := meter.Float64Histogram("tikr.query.latency_ms",
		metric.WithDescription("Query latency in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating tikr.query.latency_ms: %w", err)
	}

	return &Metrics{
		provider:           provider,
		TicksTotal:         ticksTotal,
		BatchSize:          batchSize,
		BarsFlushedTotal:   barsFlushedTotal,
		KafkaWritesTotal:   kafkaWritesTotal,
		KafkaDropsTotal:    kafkaDropsTotal,
		QueryRequestsTotal: queryRequestsTotal,
		QueryLatencyMs:     queryLatencyMs,
	}, nil
}

// Shutdown flushes pending metrics and releases resources.
// It is safe to call with a nil provider, but NOT idempotent: the underlying
// OTel MeterProvider.Shutdown returns an error on the second call. Callers
// should ensure Shutdown is called exactly once.
func (m *Metrics) Shutdown(ctx context.Context) error {
	if m.provider != nil {
		return m.provider.Shutdown(ctx)
	}
	return nil
}
