package agg

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/walkerfunction/tikr/pkg/core"
)

// BucketState holds the in-memory aggregation state for one dimension key in one time bucket.
type BucketState struct {
	BucketTs       uint64
	Dimensions     map[string]string          // stored so flush-by-timer/shutdown has dims
	Aggregators    map[string]AggFunc         // metric_name → running aggregator
	FirstTimestamp uint64
	LastTimestamp   uint64
	TickCount      uint64
	initialized    bool
}

// RollupEngine is the spec-driven aggregation engine.
// It buffers ticks in-memory and flushes rolled-up bars when the bucket boundary is crossed.
type RollupEngine struct {
	spec       *core.SeriesSpec
	seriesID   uint16
	rollupNs   uint64 // rollup granularity in nanoseconds

	mu      sync.Mutex
	buckets map[uint64]*BucketState // dimHash → current bucket state

	// Output channels
	barCh       chan *core.Bar
	hook        BarHook
	barsDropped atomic.Int64 // bars dropped due to full channel
}

// NewRollupEngine creates a rollup engine for a given series spec.
func NewRollupEngine(spec *core.SeriesSpec, seriesID uint16, hook BarHook) (*RollupEngine, error) {
	dur, err := spec.RollupDuration()
	if err != nil {
		return nil, err
	}

	if hook == nil {
		hook = NoopHook{}
	}

	return &RollupEngine{
		spec:     spec,
		seriesID: seriesID,
		rollupNs: uint64(dur / time.Nanosecond),
		buckets:  make(map[uint64]*BucketState),
		barCh:    make(chan *core.Bar, 1024),
		hook:     hook,
	}, nil
}

// BarChan returns the channel that receives flushed bars.
func (r *RollupEngine) BarChan() <-chan *core.Bar {
	return r.barCh
}

// ProcessTick feeds a tick into the rollup engine.
func (r *RollupEngine) ProcessTick(tick *core.Tick) {
	dimHash := core.DimensionHash(tick.Dimensions)
	bucketTs := (tick.TimestampNs / r.rollupNs) * r.rollupNs

	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.buckets[dimHash]

	if exists && state.BucketTs != bucketTs {
		// Bucket boundary crossed — flush the old bucket
		r.flushBucketLocked(dimHash, state)
		exists = false
	}

	if !exists {
		// Initialize new bucket — copy dimensions so mutations to the source map
		// (e.g., protobuf struct reuse, buffer pooling) cannot corrupt stored state.
		dimsCopy := make(map[string]string, len(tick.Dimensions))
		for k, v := range tick.Dimensions {
			dimsCopy[k] = v
		}
		state = &BucketState{
			BucketTs:    bucketTs,
			Dimensions:  dimsCopy,
			Aggregators: make(map[string]AggFunc),
		}
		for _, m := range r.spec.Metrics {
			state.Aggregators[m.Name] = NewAggFunc(string(m.Aggregation))
		}
		r.buckets[dimHash] = state
	}

	// Update aggregators
	for _, m := range r.spec.Metrics {
		agg := state.Aggregators[m.Name]
		if agg == nil {
			continue
		}

		var value int64
		if m.Aggregation == core.AggCount {
			value = 1 // count doesn't use a source field
		} else {
			value = tick.Fields[m.Field]
		}

		if !state.initialized {
			agg.Init(value, tick.TimestampNs)
		} else {
			agg.Update(value, tick.TimestampNs)
		}
	}

	if !state.initialized {
		state.FirstTimestamp = tick.TimestampNs
		state.initialized = true
	}
	state.LastTimestamp = tick.TimestampNs
	state.TickCount++
}

// flushBucketLocked converts a bucket state into a Bar and sends it to the output channel.
// Must be called with r.mu held.
func (r *RollupEngine) flushBucketLocked(dimHash uint64, state *BucketState) {
	bar := &core.Bar{
		Series:         r.spec.Series,
		BucketTs:       state.BucketTs,
		Dimensions:     state.Dimensions,
		Metrics:        make(map[string]int64, len(state.Aggregators)),
		FirstTimestamp: state.FirstTimestamp,
		LastTimestamp:   state.LastTimestamp,
		TickCount:      state.TickCount,
	}

	for name, agg := range state.Aggregators {
		bar.Metrics[name] = agg.Result()
	}

	// Non-blocking send — log and count drops if channel is full
	select {
	case r.barCh <- bar:
	default:
		dropped := r.barsDropped.Add(1)
		log.Printf("rollup: bar dropped for %s (channel full, total dropped: %d)", r.spec.Series, dropped)
	}

	delete(r.buckets, dimHash)

	// Call hook outside mutex to avoid blocking the rollup engine under backpressure
	r.mu.Unlock()
	_ = r.hook.OnBarFlushed(context.Background(), bar)
	r.mu.Lock()
}

// BarsDropped returns the total number of bars dropped due to full channel.
func (r *RollupEngine) BarsDropped() int64 {
	return r.barsDropped.Load()
}

// FlushStale force-flushes any bucket that hasn't received a tick in `maxAge`.
// Called periodically by a safety timer.
func (r *RollupEngine) FlushStale(maxAge time.Duration) {
	nowNs := time.Now().UnixNano()
	thresholdNs := nowNs - maxAge.Nanoseconds()
	if thresholdNs < 0 {
		return // nothing is stale yet
	}
	threshold := uint64(thresholdNs)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Collect stale keys first since flushBucketLocked releases and re-acquires the mutex
	var staleKeys []uint64
	for dimHash, state := range r.buckets {
		if state.LastTimestamp < threshold {
			staleKeys = append(staleKeys, dimHash)
		}
	}
	for _, dimHash := range staleKeys {
		if state, ok := r.buckets[dimHash]; ok {
			r.flushBucketLocked(dimHash, state)
		}
	}
}

// FlushAll force-flushes all active buckets. Used during graceful shutdown.
func (r *RollupEngine) FlushAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Collect keys first since flushBucketLocked releases and re-acquires the mutex
	keys := make([]uint64, 0, len(r.buckets))
	for dimHash := range r.buckets {
		keys = append(keys, dimHash)
	}
	for _, dimHash := range keys {
		if state, ok := r.buckets[dimHash]; ok {
			r.flushBucketLocked(dimHash, state)
		}
	}
}

// Close flushes remaining data and closes the bar channel.
func (r *RollupEngine) Close() {
	r.FlushAll()
	close(r.barCh)
}
