package agg

import (
	"testing"
	"time"

	"github.com/walkerfunction/tikr/pkg/core"
)

func TestAggFunctions(t *testing.T) {
	tests := []struct {
		name   string
		agg    AggFunc
		values []int64
		want   int64
	}{
		{"first", &FirstAgg{}, []int64{10, 20, 30}, 10},
		{"last", &LastAgg{}, []int64{10, 20, 30}, 30},
		{"min", &MinAgg{}, []int64{20, 10, 30}, 10},
		{"max", &MaxAgg{}, []int64{20, 30, 10}, 30},
		{"sum", &SumAgg{}, []int64{10, 20, 30}, 60},
		{"count", &CountAgg{}, []int64{0, 0, 0}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, v := range tt.values {
				ts := uint64((i + 1) * 1000)
				if i == 0 {
					tt.agg.Init(v, ts)
				} else {
					tt.agg.Update(v, ts)
				}
			}
			got := tt.agg.Result()
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFirstAgg_OutOfOrder(t *testing.T) {
	a := &FirstAgg{}
	a.Init(20, 2000)
	a.Update(10, 1000) // earlier timestamp
	a.Update(30, 3000)
	if a.Result() != 10 {
		t.Errorf("first should be value with earliest timestamp, got %d", a.Result())
	}
}

func TestLastAgg_OutOfOrder(t *testing.T) {
	a := &LastAgg{}
	a.Init(20, 2000)
	a.Update(30, 3000)
	a.Update(10, 1000) // earlier timestamp
	if a.Result() != 30 {
		t.Errorf("last should be value with latest timestamp, got %d", a.Result())
	}
}

func TestNewAggFunc(t *testing.T) {
	for _, name := range []string{"first", "last", "min", "max", "sum", "count"} {
		if NewAggFunc(name) == nil {
			t.Errorf("NewAggFunc(%q) returned nil", name)
		}
	}
	if NewAggFunc("median") != nil {
		t.Error("NewAggFunc('median') should return nil")
	}
}

func TestRollupEngine_BasicOHLCV(t *testing.T) {
	spec := &core.SeriesSpec{
		Series: "test",
		Timestamp: core.TimestampSpec{
			Field:     "timestamp_ns",
			Precision: "nanosecond",
		},
		Dimensions: []core.DimensionSpec{{Name: "symbol", Type: "string"}},
		Metrics: []core.MetricSpec{
			{Name: "open", Field: "price", Type: core.FieldInt64, Aggregation: core.AggFirst},
			{Name: "high", Field: "price", Type: core.FieldInt64, Aggregation: core.AggMax},
			{Name: "low", Field: "price", Type: core.FieldInt64, Aggregation: core.AggMin},
			{Name: "close", Field: "price", Type: core.FieldInt64, Aggregation: core.AggLast},
			{Name: "volume", Field: "quantity", Type: core.FieldUint64, Aggregation: core.AggSum},
			{Name: "tick_count", Type: core.FieldUint64, Aggregation: core.AggCount},
		},
		Granularity: core.GranularitySpec{Rollup: "1s"},
	}

	engine, err := NewRollupEngine(spec, 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	dims := map[string]string{"symbol": "AAPL"}
	baseNs := uint64(1_000_000_000) // 1 second in ns

	// 5 ticks in the first 1-second bucket
	ticks := []core.Tick{
		{TimestampNs: baseNs + 100, Dimensions: dims, Fields: map[string]int64{"price": 17520, "quantity": 50}, Sequence: 1},
		{TimestampNs: baseNs + 250, Dimensions: dims, Fields: map[string]int64{"price": 17525, "quantity": 30}, Sequence: 2},
		{TimestampNs: baseNs + 400, Dimensions: dims, Fields: map[string]int64{"price": 17510, "quantity": 80}, Sequence: 3},
		{TimestampNs: baseNs + 700, Dimensions: dims, Fields: map[string]int64{"price": 17530, "quantity": 20}, Sequence: 4},
		{TimestampNs: baseNs + 900, Dimensions: dims, Fields: map[string]int64{"price": 17515, "quantity": 60}, Sequence: 5},
	}

	for i := range ticks {
		engine.ProcessTick(&ticks[i])
	}

	// Send a tick in the NEXT bucket to trigger flush of the first
	nextBucket := core.Tick{
		TimestampNs: 2*baseNs + 100,
		Dimensions:  dims,
		Fields:      map[string]int64{"price": 17540, "quantity": 10},
		Sequence:    6,
	}
	engine.ProcessTick(&nextBucket)

	// Read the flushed bar
	select {
	case bar := <-engine.BarChan():
		if bar.Metrics["open"] != 17520 {
			t.Errorf("open: got %d, want 17520", bar.Metrics["open"])
		}
		if bar.Metrics["high"] != 17530 {
			t.Errorf("high: got %d, want 17530", bar.Metrics["high"])
		}
		if bar.Metrics["low"] != 17510 {
			t.Errorf("low: got %d, want 17510", bar.Metrics["low"])
		}
		if bar.Metrics["close"] != 17515 {
			t.Errorf("close: got %d, want 17515", bar.Metrics["close"])
		}
		if bar.Metrics["volume"] != 240 {
			t.Errorf("volume: got %d, want 240", bar.Metrics["volume"])
		}
		if bar.Metrics["tick_count"] != 5 {
			t.Errorf("tick_count: got %d, want 5", bar.Metrics["tick_count"])
		}
		if bar.BucketTs != baseNs {
			t.Errorf("bucket_ts: got %d, want %d", bar.BucketTs, baseNs)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bar")
	}
}

func TestRollupEngine_MultiDimension(t *testing.T) {
	spec := &core.SeriesSpec{
		Series:    "test",
		Timestamp: core.TimestampSpec{Field: "timestamp_ns", Precision: "nanosecond"},
		Dimensions: []core.DimensionSpec{
			{Name: "src_ip", Type: "string"},
			{Name: "dst_ip", Type: "string"},
		},
		Metrics: []core.MetricSpec{
			{Name: "bytes_total", Field: "bytes", Type: core.FieldUint64, Aggregation: core.AggSum},
			{Name: "flow_count", Type: core.FieldUint64, Aggregation: core.AggCount},
		},
		Granularity: core.GranularitySpec{Rollup: "1s"},
	}

	engine, err := NewRollupEngine(spec, 2, nil)
	if err != nil {
		t.Fatal(err)
	}

	baseNs := uint64(1_000_000_000)

	// Two different dimension combinations in same bucket
	dims1 := map[string]string{"src_ip": "10.0.0.1", "dst_ip": "10.0.0.2"}
	dims2 := map[string]string{"src_ip": "10.0.0.3", "dst_ip": "10.0.0.4"}

	tick1 := core.Tick{TimestampNs: baseNs + 100, Dimensions: dims1, Fields: map[string]int64{"bytes": 1000}}
	tick2 := core.Tick{TimestampNs: baseNs + 200, Dimensions: dims2, Fields: map[string]int64{"bytes": 2000}}
	tick3 := core.Tick{TimestampNs: baseNs + 300, Dimensions: dims1, Fields: map[string]int64{"bytes": 500}}

	engine.ProcessTick(&tick1)
	engine.ProcessTick(&tick2)
	engine.ProcessTick(&tick3)

	// Trigger flush by sending ticks in the next bucket
	next1 := core.Tick{TimestampNs: 2*baseNs + 100, Dimensions: dims1, Fields: map[string]int64{"bytes": 100}}
	next2 := core.Tick{TimestampNs: 2*baseNs + 100, Dimensions: dims2, Fields: map[string]int64{"bytes": 100}}
	engine.ProcessTick(&next1)
	engine.ProcessTick(&next2)

	// Should get 2 bars (one per dimension combination)
	bars := make(map[string]*core.Bar)
	for i := 0; i < 2; i++ {
		select {
		case bar := <-engine.BarChan():
			key := core.DimensionString(bar.Dimensions)
			bars[key] = bar
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for bar %d", i)
		}
	}

	if len(bars) != 2 {
		t.Fatalf("expected 2 bars, got %d", len(bars))
	}
}

func TestRollupEngine_FlushAll(t *testing.T) {
	spec := &core.SeriesSpec{
		Series:     "test",
		Timestamp:  core.TimestampSpec{Field: "ts", Precision: "nanosecond"},
		Dimensions: []core.DimensionSpec{{Name: "k", Type: "string"}},
		Metrics: []core.MetricSpec{
			{Name: "v", Field: "v", Type: core.FieldInt64, Aggregation: core.AggSum},
		},
		Granularity: core.GranularitySpec{Rollup: "1s"},
	}

	engine, err := NewRollupEngine(spec, 1, nil)
	if err != nil {
		t.Fatal(err)
	}

	tick := core.Tick{
		TimestampNs: 1_000_000_100,
		Dimensions:  map[string]string{"k": "a"},
		Fields:      map[string]int64{"v": 42},
	}
	engine.ProcessTick(&tick)

	// FlushAll should emit the partial bucket
	engine.FlushAll()

	select {
	case bar := <-engine.BarChan():
		if bar.Metrics["v"] != 42 {
			t.Errorf("expected 42, got %d", bar.Metrics["v"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for flushed bar")
	}
}
