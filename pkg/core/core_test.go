package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDimensionHash_Deterministic(t *testing.T) {
	dims := map[string]string{
		"symbol": "AAPL",
	}
	h1 := DimensionHash(dims)
	h2 := DimensionHash(dims)
	if h1 != h2 {
		t.Fatalf("hash not deterministic: %d != %d", h1, h2)
	}
}

func TestDimensionHash_OrderIndependent(t *testing.T) {
	dims1 := map[string]string{"a": "1", "b": "2", "c": "3"}
	dims2 := map[string]string{"c": "3", "a": "1", "b": "2"}
	if DimensionHash(dims1) != DimensionHash(dims2) {
		t.Fatal("hash should be order-independent")
	}
}

func TestDimensionHash_Different(t *testing.T) {
	h1 := DimensionHash(map[string]string{"symbol": "AAPL"})
	h2 := DimensionHash(map[string]string{"symbol": "GOOG"})
	if h1 == h2 {
		t.Fatal("different dimensions should (almost certainly) have different hashes")
	}
}

func TestTickKey_RoundTrip(t *testing.T) {
	var seriesID uint16 = 1
	var dimHash uint64 = 0xDEADBEEFCAFE
	var tsNs uint64 = 1711670400_000_000_000
	var seq uint32 = 42

	key := EncodeTickKey(seriesID, dimHash, tsNs, seq)
	if len(key) != 22 {
		t.Fatalf("expected key length 22, got %d", len(key))
	}

	gotSeries, gotHash, gotTs, gotSeq, err := DecodeTickKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if gotSeries != seriesID || gotHash != dimHash || gotTs != tsNs || gotSeq != seq {
		t.Fatalf("roundtrip failed: got (%d, %d, %d, %d)", gotSeries, gotHash, gotTs, gotSeq)
	}
}

func TestRollupKey_RoundTrip(t *testing.T) {
	var seriesID uint16 = 5
	var dimHash uint64 = 0x1234567890
	var bucketTs uint64 = 1711670400_000_000_000

	key := EncodeRollupKey(seriesID, dimHash, bucketTs)
	if len(key) != 18 {
		t.Fatalf("expected key length 18, got %d", len(key))
	}

	gotSeries, gotHash, gotTs, err := DecodeRollupKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if gotSeries != seriesID || gotHash != dimHash || gotTs != bucketTs {
		t.Fatalf("roundtrip failed: got (%d, %d, %d)", gotSeries, gotHash, gotTs)
	}
}

func TestTickKey_Ordering(t *testing.T) {
	// Keys with same series+dim but different timestamps should be ordered chronologically
	key1 := EncodeTickKey(1, 100, 1000, 0)
	key2 := EncodeTickKey(1, 100, 2000, 0)

	if string(key1) >= string(key2) {
		t.Fatal("earlier timestamp should produce lexicographically smaller key")
	}
}

func TestTickKey_SameTimeDifferentSeq(t *testing.T) {
	key1 := EncodeTickKey(1, 100, 1000, 1)
	key2 := EncodeTickKey(1, 100, 1000, 2)

	if string(key1) >= string(key2) {
		t.Fatal("lower sequence should produce lexicographically smaller key")
	}
}

func TestLoadSpec_MarketTicks(t *testing.T) {
	specPath := filepath.Join(findRepoRoot(t), "config", "specs", "market_ticks.yaml")
	spec, err := LoadSpec(specPath)
	if err != nil {
		t.Fatal(err)
	}

	if spec.Series != "market_ticks" {
		t.Fatalf("expected series 'market_ticks', got %q", spec.Series)
	}
	if len(spec.Dimensions) != 1 || spec.Dimensions[0].Name != "symbol" {
		t.Fatal("expected 1 dimension: symbol")
	}
	if len(spec.Metrics) != 6 {
		t.Fatalf("expected 6 metrics, got %d", len(spec.Metrics))
	}

	// Verify OHLCV mapping
	expected := map[string]AggregationType{
		"open":       AggFirst,
		"high":       AggMax,
		"low":        AggMin,
		"close":      AggLast,
		"volume":     AggSum,
		"tick_count": AggCount,
	}
	for _, m := range spec.Metrics {
		if want, ok := expected[m.Name]; ok {
			if m.Aggregation != want {
				t.Errorf("metric %s: expected aggregation %s, got %s", m.Name, want, m.Aggregation)
			}
		}
	}
}

func TestLoadSpec_Validation(t *testing.T) {
	dir := t.TempDir()

	// Missing series name
	bad := `
timestamp:
  field: ts
  precision: nanosecond
metrics:
  - name: val
    field: v
    type: int64
    aggregation: sum
granularity:
  rollup: 1s
`
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(bad), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSpec(path); err == nil {
		t.Fatal("expected validation error for missing series name")
	}
}

func TestLoadSpec_BadAggregation(t *testing.T) {
	dir := t.TempDir()
	bad := `
series: test
timestamp:
  field: ts
  precision: nanosecond
metrics:
  - name: val
    field: v
    type: int64
    aggregation: median
granularity:
  rollup: 1s
`
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(bad), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSpec(path); err == nil {
		t.Fatal("expected validation error for unsupported aggregation")
	}
}

func TestLoadSpecs_Directory(t *testing.T) {
	specsDir := filepath.Join(findRepoRoot(t), "config", "specs")
	specs, err := LoadSpecs(specsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}

	names := make(map[string]bool)
	for _, s := range specs {
		names[s.Series] = true
	}
	if !names["market_ticks"] || !names["network_flows"] {
		t.Fatalf("expected market_ticks and network_flows, got %v", names)
	}
}

func TestLoadSpecs_DuplicateSeriesName(t *testing.T) {
	dir := t.TempDir()
	spec := `
series: dupe
timestamp:
  field: ts
  precision: nanosecond
metrics:
  - name: val
    field: v
    type: int64
    aggregation: sum
granularity:
  rollup: 1s
`
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(spec), 0644)
	os.WriteFile(filepath.Join(dir, "b.yaml"), []byte(spec), 0644)

	if _, err := LoadSpecs(dir); err == nil {
		t.Fatal("expected error for duplicate series name")
	}
}

func TestRollupDuration(t *testing.T) {
	spec := &SeriesSpec{Granularity: GranularitySpec{Rollup: "1s"}}
	d, err := spec.RollupDuration()
	if err != nil {
		t.Fatal(err)
	}
	if d.Seconds() != 1.0 {
		t.Fatalf("expected 1s, got %v", d)
	}
}

func TestDimensionString(t *testing.T) {
	dims := map[string]string{"b": "2", "a": "1"}
	s := DimensionString(dims)
	if s != "a=1,b=2" {
		t.Fatalf("expected 'a=1,b=2', got %q", s)
	}
}

// findRepoRoot walks up from the test file to find the project root (where go.mod is).
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repo root")
		}
		dir = parent
	}
}
