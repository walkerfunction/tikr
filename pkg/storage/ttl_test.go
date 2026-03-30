package storage

import (
	"testing"
	"time"

	"github.com/walkerfunction/tikr/pkg/core"
)

func TestTTL_TicksExpiredOnRead(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	reader := NewReaderWithTTL(engine, TTLConfig{TicksTTL: 1 * time.Hour})

	var seriesID uint16 = 1
	dims := map[string]string{"symbol": "AAPL"}
	dimHash := core.DimensionHash(dims)

	now := uint64(time.Now().UnixNano())
	oldTs := now - uint64(2*time.Hour)    // expired
	newTs := now - uint64(30*time.Minute) // valid

	ticks := []core.Tick{
		{TimestampNs: oldTs, Dimensions: dims, Fields: map[string]int64{"price": 100}, Sequence: 1},
		{TimestampNs: oldTs + 1000, Dimensions: dims, Fields: map[string]int64{"price": 101}, Sequence: 2},
		{TimestampNs: newTs, Dimensions: dims, Fields: map[string]int64{"price": 200}, Sequence: 3},
	}

	if err := writer.WriteTicks(seriesID, ticks); err != nil {
		t.Fatalf("write ticks: %v", err)
	}

	// Reader with TTL should only return the non-expired tick
	got, err := reader.ReadTicks(seriesID, dimHash, 0, ^uint64(0), 0)
	if err != nil {
		t.Fatalf("read ticks: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 tick (TTL filtered), got %d", len(got))
	}
	if got[0].Fields["price"] != 200 {
		t.Fatalf("expected price=200, got %d", got[0].Fields["price"])
	}

	// Reader without TTL should return all 3
	readerNoTTL := NewReader(engine)
	all, err := readerNoTTL.ReadTicks(seriesID, dimHash, 0, ^uint64(0), 0)
	if err != nil {
		t.Fatalf("read ticks no TTL: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 ticks (no TTL), got %d", len(all))
	}
}

func TestTTL_BarsExpiredOnRead(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	reader := NewReaderWithTTL(engine, TTLConfig{RollupTTL: 1 * time.Hour})

	var seriesID uint16 = 1
	dims := map[string]string{"symbol": "AAPL"}
	dimHash := core.DimensionHash(dims)

	now := uint64(time.Now().UnixNano())
	oldBucket := now - uint64(4*time.Hour)    // expired
	newBucket := now - uint64(30*time.Minute) // valid

	bars := []*core.Bar{
		{Series: "test", BucketTs: oldBucket, Dimensions: dims, Metrics: map[string]int64{"v": 1}, TickCount: 1},
		{Series: "test", BucketTs: newBucket, Dimensions: dims, Metrics: map[string]int64{"v": 2}, TickCount: 1},
	}

	if err := writer.WriteBars(seriesID, bars); err != nil {
		t.Fatalf("write bars: %v", err)
	}

	got, err := reader.ReadBars(seriesID, dimHash, 0, ^uint64(0))
	if err != nil {
		t.Fatalf("read bars: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 bar (TTL filtered), got %d", len(got))
	}
	if got[0].Metrics["v"] != 2 {
		t.Fatalf("expected v=2, got %d", got[0].Metrics["v"])
	}
}

func TestTTL_ZeroMeansNoExpiry(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	reader := NewReaderWithTTL(engine, TTLConfig{}) // zero TTL = no filtering

	var seriesID uint16 = 1
	dims := map[string]string{"symbol": "AAPL"}
	dimHash := core.DimensionHash(dims)

	ticks := []core.Tick{
		{TimestampNs: 1000, Dimensions: dims, Fields: map[string]int64{"price": 42}, Sequence: 1},
	}
	if err := writer.WriteTicks(seriesID, ticks); err != nil {
		t.Fatal(err)
	}

	got, err := reader.ReadTicks(seriesID, dimHash, 0, ^uint64(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 tick with zero TTL, got %d", len(got))
	}
}

func TestReaper_TombstonesExpiredTicks(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	readerRaw := NewReader(engine) // no TTL filter — sees everything

	var seriesID uint16 = 1
	dims := map[string]string{"symbol": "AAPL"}
	dimHash := core.DimensionHash(dims)

	now := uint64(time.Now().UnixNano())
	oldTs := now - uint64(2*time.Hour)
	newTs := now - uint64(10*time.Minute)

	ticks := []core.Tick{
		{TimestampNs: oldTs, Dimensions: dims, Fields: map[string]int64{"price": 100}, Sequence: 1},
		{TimestampNs: oldTs + 1000, Dimensions: dims, Fields: map[string]int64{"price": 101}, Sequence: 2},
		{TimestampNs: newTs, Dimensions: dims, Fields: map[string]int64{"price": 200}, Sequence: 3},
	}
	if err := writer.WriteTicks(seriesID, ticks); err != nil {
		t.Fatal(err)
	}

	// Before reap: raw reader sees all 3
	got, _ := readerRaw.ReadTicks(seriesID, dimHash, 0, ^uint64(0), 0)
	if len(got) != 3 {
		t.Fatalf("before reap: expected 3 ticks, got %d", len(got))
	}

	// Run reaper with 1h TTL
	reaper := NewReaper(engine, TTLConfig{TicksTTL: 1 * time.Hour}, time.Hour)
	reaper.reap()

	// After reap: even raw reader can't see tombstoned keys
	got, _ = readerRaw.ReadTicks(seriesID, dimHash, 0, ^uint64(0), 0)
	if len(got) != 1 {
		t.Fatalf("after reap: expected 1 tick, got %d", len(got))
	}
	if got[0].Fields["price"] != 200 {
		t.Fatalf("expected price=200, got %d", got[0].Fields["price"])
	}
}

func TestReaper_TombstonesExpiredBars(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	readerRaw := NewReader(engine)

	var seriesID uint16 = 1
	dims := map[string]string{"symbol": "AAPL"}
	dimHash := core.DimensionHash(dims)

	now := uint64(time.Now().UnixNano())
	oldBucket := now - uint64(3*time.Hour)
	newBucket := now - uint64(5*time.Minute)

	bars := []*core.Bar{
		{Series: "test", BucketTs: oldBucket, Dimensions: dims, Metrics: map[string]int64{"v": 1}, TickCount: 1},
		{Series: "test", BucketTs: newBucket, Dimensions: dims, Metrics: map[string]int64{"v": 2}, TickCount: 1},
	}
	if err := writer.WriteBars(seriesID, bars); err != nil {
		t.Fatal(err)
	}

	reaper := NewReaper(engine, TTLConfig{RollupTTL: 1 * time.Hour}, time.Hour)
	reaper.reap()

	got, _ := readerRaw.ReadBars(seriesID, dimHash, 0, ^uint64(0))
	if len(got) != 1 {
		t.Fatalf("after reap: expected 1 bar, got %d", len(got))
	}
	if got[0].Metrics["v"] != 2 {
		t.Fatalf("expected v=2, got %d", got[0].Metrics["v"])
	}
}

func TestReaper_MultipleGroups(t *testing.T) {
	engine := newTestEngine(t)
	writer := NewWriter(engine)
	readerRaw := NewReader(engine)

	now := uint64(time.Now().UnixNano())
	oldTs := now - uint64(3*time.Hour)
	newTs := now - uint64(5*time.Minute)

	aapl := map[string]string{"symbol": "AAPL"}
	goog := map[string]string{"symbol": "GOOG"}

	// Series 1 AAPL: old (expired)
	if err := writer.WriteTicks(1, []core.Tick{
		{TimestampNs: oldTs, Dimensions: aapl, Fields: map[string]int64{"p": 10}, Sequence: 1},
	}); err != nil {
		t.Fatal(err)
	}
	// Series 1 GOOG: new (valid)
	if err := writer.WriteTicks(1, []core.Tick{
		{TimestampNs: newTs, Dimensions: goog, Fields: map[string]int64{"p": 20}, Sequence: 1},
	}); err != nil {
		t.Fatal(err)
	}
	// Series 2 AAPL: old (expired)
	if err := writer.WriteTicks(2, []core.Tick{
		{TimestampNs: oldTs, Dimensions: aapl, Fields: map[string]int64{"p": 30}, Sequence: 1},
	}); err != nil {
		t.Fatal(err)
	}

	reaper := NewReaper(engine, TTLConfig{TicksTTL: 1 * time.Hour}, time.Hour)
	reaper.reap()

	// Series 1 AAPL: gone
	got, _ := readerRaw.ReadTicks(1, core.DimensionHash(aapl), 0, ^uint64(0), 0)
	if len(got) != 0 {
		t.Fatalf("s1 AAPL: expected 0, got %d", len(got))
	}

	// Series 1 GOOG: still there
	got, _ = readerRaw.ReadTicks(1, core.DimensionHash(goog), 0, ^uint64(0), 0)
	if len(got) != 1 {
		t.Fatalf("s1 GOOG: expected 1, got %d", len(got))
	}

	// Series 2 AAPL: gone
	got, _ = readerRaw.ReadTicks(2, core.DimensionHash(aapl), 0, ^uint64(0), 0)
	if len(got) != 0 {
		t.Fatalf("s2 AAPL: expected 0, got %d", len(got))
	}
}
