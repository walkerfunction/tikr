package output

import (
	"context"
	"testing"
	"time"

	"github.com/tikr-dev/tikr/pkg/core"
)

func TestKafkaProducer_DropSemantics(t *testing.T) {
	specs := []*core.SeriesSpec{
		{
			Series: "test_series",
			Output: core.OutputSpec{
				Kafka: core.KafkaOutputSpec{
					Topic:     "test-topic",
					Encoding:  "proto",
					OnFailure: "drop",
				},
			},
		},
	}

	// Use an unreachable broker to verify fire-and-forget behaviour.
	kp, err := NewKafkaProducer([]string{"localhost:19092"}, specs)
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}
	defer kp.Close()

	bar := &core.Bar{
		Series:         "test_series",
		BucketTs:       1000000000,
		Dimensions:     map[string]string{"exchange": "NYSE", "symbol": "AAPL"},
		Metrics:        map[string]int64{"volume": 42, "high": 150},
		FirstTimestamp: 1000000001,
		LastTimestamp:   1000000099,
		TickCount:      10,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Must not block or return an error (drop semantics).
	if err := kp.OnBarFlushed(ctx, bar); err != nil {
		t.Fatalf("OnBarFlushed should not return error, got: %v", err)
	}
}

func TestKafkaProducer_NoWriter(t *testing.T) {
	// Series with no Kafka topic should be silently skipped.
	specs := []*core.SeriesSpec{
		{
			Series: "no_kafka",
			Output: core.OutputSpec{},
		},
	}

	kp, err := NewKafkaProducer([]string{"localhost:19092"}, specs)
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}
	defer kp.Close()

	bar := &core.Bar{
		Series:   "no_kafka",
		BucketTs: 1000000000,
	}

	ctx := context.Background()
	if err := kp.OnBarFlushed(ctx, bar); err != nil {
		t.Fatalf("OnBarFlushed should not return error for missing writer, got: %v", err)
	}
}

func TestDimensionKey(t *testing.T) {
	tests := []struct {
		name string
		dims map[string]string
		want string
	}{
		{"empty", nil, ""},
		{"single", map[string]string{"sym": "AAPL"}, "AAPL"},
		{
			"sorted",
			map[string]string{"exchange": "NYSE", "symbol": "AAPL"},
			"NYSE|AAPL", // exchange < symbol
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dimensionKey(tt.dims)
			if got != tt.want {
				t.Errorf("dimensionKey(%v) = %q, want %q", tt.dims, got, tt.want)
			}
		})
	}
}
