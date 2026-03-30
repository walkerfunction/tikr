package storage

import (
	"testing"
	"time"

	"github.com/walkerfunction/tikr/pkg/core"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	dir := t.TempDir()
	engine, err := NewEngine(EngineConfig{
		DataDir:       dir,
		TicksTTL:      6 * time.Hour,
		TicksMaxSize:  1 * 1024 * 1024 * 1024, // 1GB
		RollupTTL:     12 * time.Hour,
		RollupMaxSize: 100 * 1024 * 1024, // 100MB
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func TestEngine_OpenClose(t *testing.T) {
	engine := newTestEngine(t)
	_ = engine // just verify open/close works
}

func TestWriter_WriteTicks_ReadBack(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	reader := NewReader(engine)

	var seriesID uint16 = 1
	dims := map[string]string{"symbol": "AAPL"}
	dimHash := core.DimensionHash(dims)

	ticks := []core.Tick{
		{TimestampNs: 1000, Dimensions: dims, Fields: map[string]int64{"price": 17520, "quantity": 50}, Sequence: 1},
		{TimestampNs: 2000, Dimensions: dims, Fields: map[string]int64{"price": 17525, "quantity": 30}, Sequence: 2},
		{TimestampNs: 3000, Dimensions: dims, Fields: map[string]int64{"price": 17510, "quantity": 80}, Sequence: 3},
	}

	if err := writer.WriteTicks(seriesID, ticks); err != nil {
		t.Fatalf("write ticks: %v", err)
	}

	// Read back
	got, err := reader.ReadTicks(seriesID, dimHash, 0, 10000)
	if err != nil {
		t.Fatalf("read ticks: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 ticks, got %d", len(got))
	}

	// Verify ordering
	for i := 1; i < len(got); i++ {
		if got[i].TimestampNs <= got[i-1].TimestampNs {
			t.Fatalf("ticks not in chronological order at index %d", i)
		}
	}

	// Verify fields
	if got[0].Fields["price"] != 17520 {
		t.Fatalf("expected price 17520, got %d", got[0].Fields["price"])
	}
}

func TestWriter_WriteBar_ReadBack(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	reader := NewReader(engine)

	var seriesID uint16 = 1
	dims := map[string]string{"symbol": "AAPL"}
	dimHash := core.DimensionHash(dims)

	bar := &core.Bar{
		Series:         "market_ticks",
		BucketTs:       1000000000, // 1 second in ns
		Dimensions:     dims,
		Metrics:        map[string]int64{"open": 17520, "high": 17530, "low": 17510, "close": 17525, "volume": 160},
		FirstTimestamp: 1000,
		LastTimestamp:   3000,
		TickCount:      3,
	}

	if err := writer.WriteBar(seriesID, bar); err != nil {
		t.Fatalf("write bar: %v", err)
	}

	bars, err := reader.ReadBars(seriesID, dimHash, 0, 2000000000)
	if err != nil {
		t.Fatalf("read bars: %v", err)
	}

	if len(bars) != 1 {
		t.Fatalf("expected 1 bar, got %d", len(bars))
	}

	got := bars[0]
	if got.Metrics["open"] != 17520 {
		t.Fatalf("expected open 17520, got %d", got.Metrics["open"])
	}
	if got.Metrics["volume"] != 160 {
		t.Fatalf("expected volume 160, got %d", got.Metrics["volume"])
	}
	if got.TickCount != 3 {
		t.Fatalf("expected tick_count 3, got %d", got.TickCount)
	}
}

func TestWriter_MultipleDimensions(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	reader := NewReader(engine)

	var seriesID uint16 = 1

	aapl := map[string]string{"symbol": "AAPL"}
	goog := map[string]string{"symbol": "GOOG"}

	ticks := []core.Tick{
		{TimestampNs: 1000, Dimensions: aapl, Fields: map[string]int64{"price": 17520}, Sequence: 1},
		{TimestampNs: 1000, Dimensions: goog, Fields: map[string]int64{"price": 14000}, Sequence: 1},
		{TimestampNs: 2000, Dimensions: aapl, Fields: map[string]int64{"price": 17525}, Sequence: 2},
	}

	if err := writer.WriteTicks(seriesID, ticks); err != nil {
		t.Fatalf("write ticks: %v", err)
	}

	// Read AAPL only
	aaplHash := core.DimensionHash(aapl)
	got, err := reader.ReadTicks(seriesID, aaplHash, 0, 10000)
	if err != nil {
		t.Fatalf("read ticks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 AAPL ticks, got %d", len(got))
	}

	// Read GOOG only
	googHash := core.DimensionHash(goog)
	got, err = reader.ReadTicks(seriesID, googHash, 0, 10000)
	if err != nil {
		t.Fatalf("read ticks: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 GOOG tick, got %d", len(got))
	}
}

func TestMeta_WriteRead(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	reader := NewReader(engine)

	key := []byte("checkpoint:market_ticks")
	value := []byte(`{"last_bucket":1000000000}`)

	if err := writer.WriteMeta(key, value); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	got, err := reader.ReadMeta(key)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("expected %q, got %q", value, got)
	}
}

func TestMeta_ReadMissing(t *testing.T) {
	engine := newTestEngine(t)
	reader := NewReader(engine)

	got, err := reader.ReadMeta([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing key, got %v", got)
	}
}
