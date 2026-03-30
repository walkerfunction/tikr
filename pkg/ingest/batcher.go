package ingest

import (
	"sync"
	"time"

	"github.com/walkerfunction/tikr/pkg/core"
)

// BatcherConfig controls batching behavior.
type BatcherConfig struct {
	MaxSize     int           // max ticks per batch before flush
	MaxLatency  time.Duration // max time before flush
}

// DefaultBatcherConfig returns sensible defaults.
func DefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		MaxSize:    5000,
		MaxLatency: time.Millisecond,
	}
}

// Batcher accumulates ticks into micro-batches for efficient writes.
type Batcher struct {
	cfg     BatcherConfig
	flushFn func([]core.Tick) // called with each batch

	mu      sync.Mutex
	buf     []core.Tick
	timer   *time.Timer
	closed  bool
}

// NewBatcher creates a batcher that calls flushFn with each accumulated batch.
func NewBatcher(cfg BatcherConfig, flushFn func([]core.Tick)) *Batcher {
	return &Batcher{
		cfg:     cfg,
		flushFn: flushFn,
		buf:     make([]core.Tick, 0, cfg.MaxSize),
	}
}

// Add adds a tick to the current batch.
func (b *Batcher) Add(tick core.Tick) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.buf = append(b.buf, tick)

	// Start latency timer on first tick in batch
	if len(b.buf) == 1 && b.cfg.MaxLatency > 0 {
		b.timer = time.AfterFunc(b.cfg.MaxLatency, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.flushLocked()
		})
	}

	// Flush if batch is full
	if len(b.buf) >= b.cfg.MaxSize {
		b.flushLocked()
	}
}

// flushLocked flushes the current batch. Must be called with b.mu held.
func (b *Batcher) flushLocked() {
	if len(b.buf) == 0 {
		return
	}

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	batch := b.buf
	b.buf = make([]core.Tick, 0, b.cfg.MaxSize)

	// Call flush function outside the critical path? No — keep it simple.
	// The flushFn should be fast (just enqueue to a channel).
	b.flushFn(batch)
}

// Flush forces a flush of the current batch.
func (b *Batcher) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// Close flushes remaining ticks and marks the batcher as closed.
func (b *Batcher) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
	b.closed = true
}
