package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/walkerfunction/tikr/pkg/agg"
	"github.com/walkerfunction/tikr/pkg/core"
	"github.com/walkerfunction/tikr/pkg/storage"
	"github.com/walkerfunction/tikr/pkg/telemetry"
)

// TestOTel_TicksIngestedAndBarsFlushed verifies that OTel counters are
// correctly incremented through the full pipeline: ticks in → rollup → bars out.
func TestOTel_TicksIngestedAndBarsFlushed(t *testing.T) {
	// Set up OTel with a ManualReader so we can collect metrics on demand
	reader := sdkmetric.NewManualReader()
	metrics, err := telemetry.NewMetrics("tikr-test", telemetry.WithReader(reader))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metrics.Shutdown(context.Background()) }()

	spec := &core.SeriesSpec{
		Series:     "otel_test",
		Timestamp:  core.TimestampSpec{Field: "ts", Precision: "nanosecond"},
		Dimensions: []core.DimensionSpec{{Name: "k", Type: "string"}},
		Metrics: []core.MetricSpec{
			{Name: "total", Field: "v", Type: core.FieldInt64, Aggregation: core.AggSum},
			{Name: "count", Type: core.FieldUint64, Aggregation: core.AggCount},
		},
		Granularity: core.GranularitySpec{Rollup: "1s"},
	}

	dir := t.TempDir()
	engine, err := storage.NewEngine(storage.EngineConfig{
		DataDir:       dir,
		TicksTTL:      time.Hour,
		TicksMaxSize:  1 << 30,
		RollupTTL:     time.Hour,
		RollupMaxSize: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	writer := storage.NewWriter(engine)

	var barsMu sync.Mutex
	var bars []*core.Bar
	pipeline, err := NewPipeline(PipelineConfig{
		Specs:      []*core.SeriesSpec{spec},
		Writer:     writer,
		Hook:       agg.NoopHook{},
		BatcherCfg: BatcherConfig{MaxSize: 100, MaxLatency: time.Millisecond},
		Metrics:    metrics,
		OnBarFlushed: func(bar *core.Bar) {
			barsMu.Lock()
			bars = append(bars, bar)
			barsMu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	dims := map[string]string{"k": "x"}
	baseNs := uint64(1_000_000_000)

	// Ingest 5 ticks in bucket 1
	for i := 0; i < 5; i++ {
		if err := pipeline.Ingest(core.Tick{
			TimestampNs: baseNs + uint64(i*100),
			Series:      "otel_test",
			Dimensions:  dims,
			Fields:      map[string]int64{"v": 10},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for batcher to flush
	time.Sleep(50 * time.Millisecond)

	// Ingest 1 tick in bucket 2 to trigger rollup flush of bucket 1
	if err := pipeline.Ingest(core.Tick{
		TimestampNs: 2*baseNs + 100,
		Series:      "otel_test",
		Dimensions:  dims,
		Fields:      map[string]int64{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}

	// Wait for flush propagation
	time.Sleep(100 * time.Millisecond)

	// Close pipeline to flush everything
	pipeline.Close()

	// Verify we got at least 1 bar
	barsMu.Lock()
	nBars := len(bars)
	barsMu.Unlock()
	if nBars == 0 {
		t.Fatal("expected at least 1 bar to be flushed")
	}

	// Collect OTel metrics
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	// Print the full OTel message
	t.Logf("=== RAW OTEL MESSAGE ===")
	for _, sm := range rm.ScopeMetrics {
		t.Logf("Scope: %s", sm.Scope.Name)
		for _, m := range rm.ScopeMetrics[0].Metrics {
			t.Logf("  Metric: %s (%s)", m.Name, m.Description)
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					t.Logf("    Value: %d  Attrs: %v  Time: %s → %s",
						dp.Value, dp.Attributes.ToSlice(), dp.StartTime.Format("15:04:05"), dp.Time.Format("15:04:05"))
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					t.Logf("    Count: %d  Sum: %.1f  Bounds: %v  Counts: %v",
						dp.Count, dp.Sum, dp.Bounds, dp.BucketCounts)
				}
			}
		}
		_ = sm
	}
	t.Logf("========================")

	// Extract counter values by name
	counters := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					counters[m.Name] += dp.Value
				}
			}
		}
	}

	// Verify ticks ingested: 5 in bucket 1 + 1 in bucket 2 = 6
	ticksTotal := counters["tikr.ingest.ticks_total"]
	if ticksTotal != 6 {
		t.Errorf("tikr.ingest.ticks_total: got %d, want 6", ticksTotal)
	} else {
		t.Logf("tikr.ingest.ticks_total: %d (message in)", ticksTotal)
	}

	// Verify bars flushed out: bucket 1 flushed on boundary, bucket 2 flushed on Close
	barsFlushed := counters["tikr.agg.bars_flushed_total"]
	if barsFlushed < 1 {
		t.Errorf("tikr.agg.bars_flushed_total: got %d, want >= 1", barsFlushed)
	} else {
		t.Logf("tikr.agg.bars_flushed_total: %d (message out)", barsFlushed)
	}

	// Log all collected counters
	for name, val := range counters {
		t.Logf("  OTel %s = %d", name, val)
	}
}
