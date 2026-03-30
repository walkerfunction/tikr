package ingest

import (
	"sync"
	"testing"
	"time"

	"github.com/walkerfunction/tikr/pkg/agg"
	"github.com/walkerfunction/tikr/pkg/core"
	"github.com/walkerfunction/tikr/pkg/storage"
)

func TestBatcher_FlushOnSize(t *testing.T) {
	var mu sync.Mutex
	var flushed [][]core.Tick
	b := NewBatcher(BatcherConfig{MaxSize: 3, MaxLatency: time.Second}, func(batch []core.Tick) {
		mu.Lock()
		flushed = append(flushed, batch)
		mu.Unlock()
	})

	for i := 0; i < 5; i++ {
		b.Add(core.Tick{TimestampNs: uint64(i), Fields: map[string]int64{"v": int64(i)}})
	}

	mu.Lock()
	nFlushed := len(flushed)
	mu.Unlock()
	if nFlushed != 1 {
		t.Fatalf("expected 1 flush (at size 3), got %d", nFlushed)
	}
	mu.Lock()
	batchSize := len(flushed[0])
	mu.Unlock()
	if batchSize != 3 {
		t.Fatalf("expected batch size 3, got %d", batchSize)
	}

	// Close flushes remaining
	b.Close()
	mu.Lock()
	nFlushed = len(flushed)
	remainSize := len(flushed[1])
	mu.Unlock()
	if nFlushed != 2 {
		t.Fatalf("expected 2 flushes after close, got %d", nFlushed)
	}
	if remainSize != 2 {
		t.Fatalf("expected remaining batch size 2, got %d", remainSize)
	}
}

func TestBatcher_FlushOnTimer(t *testing.T) {
	var mu sync.Mutex
	var flushed [][]core.Tick
	b := NewBatcher(BatcherConfig{MaxSize: 1000, MaxLatency: 50 * time.Millisecond}, func(batch []core.Tick) {
		mu.Lock()
		flushed = append(flushed, batch)
		mu.Unlock()
	})

	b.Add(core.Tick{TimestampNs: 1})
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	nFlushed := len(flushed)
	mu.Unlock()
	if nFlushed != 1 {
		t.Fatalf("expected timer flush, got %d flushes", nFlushed)
	}
	b.Close()
}

func TestPipeline_IngestAndRollup(t *testing.T) {
	spec := &core.SeriesSpec{
		Series:     "test",
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
	reader := storage.NewReader(engine)

	var barsMu sync.Mutex
	var bars []*core.Bar
	pipeline, err := NewPipeline(PipelineConfig{
		Specs:      []*core.SeriesSpec{spec},
		Writer:     writer,
		Hook:       agg.NoopHook{},
		BatcherCfg: BatcherConfig{MaxSize: 100, MaxLatency: time.Millisecond},
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

	// Send 5 ticks in bucket 1
	for i := 0; i < 5; i++ {
		if err := pipeline.Ingest(core.Tick{
			TimestampNs: baseNs + uint64(i*100),
			Series:      "test",
			Dimensions:  dims,
			Fields:      map[string]int64{"v": 10},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for batcher timer to flush
	time.Sleep(50 * time.Millisecond)

	// Send tick in bucket 2 to trigger rollup flush
	if err := pipeline.Ingest(core.Tick{
		TimestampNs: 2*baseNs + 100,
		Series:      "test",
		Dimensions:  dims,
		Fields:      map[string]int64{"v": 1},
	}); err != nil {
		t.Fatal(err)
	}

	// Wait for flush propagation
	time.Sleep(100 * time.Millisecond)

	// Verify ticks were written to storage
	dimHash := core.DimensionHash(dims)
	ticks, err := reader.ReadTicks(1, dimHash, 0, 3*baseNs)
	if err != nil {
		t.Fatal(err)
	}
	if len(ticks) < 5 {
		t.Fatalf("expected at least 5 ticks in storage, got %d", len(ticks))
	}

	// Close pipeline first (flushes batchers + rollup engines, waits for consumeBars to finish)
	pipeline.Close()

	// Verify bar was flushed
	barsMu.Lock()
	nBars := len(bars)
	var bar *core.Bar
	if nBars > 0 {
		bar = bars[0]
	}
	barsMu.Unlock()

	if nBars == 0 {
		t.Fatal("expected at least 1 bar to be flushed")
	}
	if bar.Metrics["total"] != 50 {
		t.Errorf("total: got %d, want 50", bar.Metrics["total"])
	}
	if bar.Metrics["count"] != 5 {
		t.Errorf("count: got %d, want 5", bar.Metrics["count"])
	}
}
