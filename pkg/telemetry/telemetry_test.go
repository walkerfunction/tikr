package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func TestNewMetrics(t *testing.T) {
	m, err := NewMetrics("tikr-test")
	if err != nil {
		t.Fatalf("NewMetrics failed: %v", err)
	}
	defer func() {
		if err := m.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
	}()

	if m.TicksTotal == nil {
		t.Fatal("TicksTotal is nil")
	}
	if m.BatchSize == nil {
		t.Fatal("BatchSize is nil")
	}
	if m.BarsFlushedTotal == nil {
		t.Fatal("BarsFlushedTotal is nil")
	}
	if m.KafkaWritesTotal == nil {
		t.Fatal("KafkaWritesTotal is nil")
	}
	if m.KafkaDropsTotal == nil {
		t.Fatal("KafkaDropsTotal is nil")
	}
	if m.QueryRequestsTotal == nil {
		t.Fatal("QueryRequestsTotal is nil")
	}
	if m.QueryLatencyMs == nil {
		t.Fatal("QueryLatencyMs is nil")
	}
}

func TestMetricsIncrement(t *testing.T) {
	m, err := NewMetrics("tikr-test")
	if err != nil {
		t.Fatalf("NewMetrics failed: %v", err)
	}
	defer func() { _ = m.Shutdown(context.Background()) }()

	ctx := context.Background()

	// These should not panic.
	m.TicksTotal.Add(ctx, 100)
	m.BatchSize.Record(ctx, 500.0)
	m.BarsFlushedTotal.Add(ctx, 10)
	m.KafkaWritesTotal.Add(ctx, 5)
	m.KafkaDropsTotal.Add(ctx, 1)
	m.QueryRequestsTotal.Add(ctx, 3, metric.WithAttributes(attribute.String("type", "ticks")))
	m.QueryRequestsTotal.Add(ctx, 7, metric.WithAttributes(attribute.String("type", "bars")))
	m.QueryLatencyMs.Record(ctx, 12.5)
}
