package ingest

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"

	"github.com/walkerfunction/tikr/pkg/agg"
	"github.com/walkerfunction/tikr/pkg/core"
	"github.com/walkerfunction/tikr/pkg/storage"
	"github.com/walkerfunction/tikr/pkg/telemetry"
)

// SeriesPipeline holds the processing components for a single series.
type SeriesPipeline struct {
	Spec     *core.SeriesSpec
	SeriesID uint16
	Rollup   *agg.RollupEngine
	Batcher  *Batcher
}

// Pipeline orchestrates ingestion across all configured series.
type Pipeline struct {
	mu       sync.RWMutex
	series   map[string]*SeriesPipeline // series name → pipeline
	writer   *storage.Writer
	barSink  func(*core.Bar)        // called for each flushed bar (e.g., write to storage + push to Kafka)
	metrics  *telemetry.Metrics     // nil-safe
	wg       sync.WaitGroup         // tracks consumeBars goroutines
}

// PipelineConfig holds the configuration for creating a pipeline.
type PipelineConfig struct {
	Specs      []*core.SeriesSpec
	Writer     *storage.Writer
	BatcherCfg BatcherConfig

	// Hook is the primary bar side-effect handler (e.g., Kafka producer).
	// It is called synchronously from the rollup engine for each flushed bar.
	Hook agg.BarHook

	// OnBarFlushed is an optional observability callback (e.g., logging, metrics).
	// It is called from the consumeBars goroutine after the bar is written to storage.
	// Do not use this for side-effects that duplicate the Hook path (e.g., Kafka).
	OnBarFlushed func(*core.Bar)

	// Metrics is optional OTel instrumentation. When set, the pipeline records
	// BarsFlushedTotal in the consumeBars goroutine.
	Metrics *telemetry.Metrics
}

// NewPipeline creates a pipeline for all configured series.
func NewPipeline(cfg PipelineConfig) (*Pipeline, error) {
	p := &Pipeline{
		series:  make(map[string]*SeriesPipeline),
		writer:  cfg.Writer,
		barSink: cfg.OnBarFlushed,
		metrics: cfg.Metrics,
	}

	// Verify series ID uniqueness before proceeding (H-1)
	seenIDs := make(map[uint16]string, len(cfg.Specs))
	for _, spec := range cfg.Specs {
		id := stableSeriesID(spec.Series)
		if existing, conflict := seenIDs[id]; conflict {
			return nil, fmt.Errorf("series ID collision: %q and %q both hash to %d", spec.Series, existing, id)
		}
		seenIDs[id] = spec.Series
	}

	for _, spec := range cfg.Specs {
		seriesID := stableSeriesID(spec.Series)

		rollup, err := agg.NewRollupEngine(spec, seriesID, cfg.Hook)
		if err != nil {
			return nil, fmt.Errorf("creating rollup engine for %s: %w", spec.Series, err)
		}

		sp := &SeriesPipeline{
			Spec:     spec,
			SeriesID: seriesID,
			Rollup:   rollup,
		}

		// Batcher flushes to storage writer + feeds aggregation engine
		batcher := NewBatcher(cfg.BatcherCfg, func(ticks []core.Tick) {
			p.handleBatch(sp, ticks)
		})
		sp.Batcher = batcher

		p.series[spec.Series] = sp

		// Start bar consumer goroutine (reads flushed bars from rollup engine)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.consumeBars(sp)
		}()
	}

	return p, nil
}

// Ingest routes a tick to the appropriate series pipeline.
func (p *Pipeline) Ingest(tick core.Tick) error {
	p.mu.RLock()
	sp, ok := p.series[tick.Series]
	p.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown series: %s", tick.Series)
	}

	sp.Batcher.Add(tick)

	if p.metrics != nil {
		// context.Background() is fine for counter increments — OTel Add is a simple
		// atomic operation that does not need caller trace/span context.
		// TODO: accept context.Context parameter to enable trace correlation when tracing is added.
		p.metrics.TicksTotal.Add(context.Background(), 1)
	}

	return nil
}

// handleBatch writes a batch of ticks to storage and feeds them to the rollup engine.
func (p *Pipeline) handleBatch(sp *SeriesPipeline, ticks []core.Tick) {
	// Write to Pebble
	if err := p.writer.WriteTicks(sp.SeriesID, ticks); err != nil {
		log.Printf("error writing tick batch for %s: %v", sp.Spec.Series, err)
	}

	// Feed each tick to the rollup engine
	for i := range ticks {
		sp.Rollup.ProcessTick(&ticks[i])
	}
}

// consumeBars reads flushed bars from the rollup engine and processes them.
func (p *Pipeline) consumeBars(sp *SeriesPipeline) {
	for bar := range sp.Rollup.BarChan() {
		// Write bar to local storage
		if err := p.writer.WriteBar(sp.SeriesID, bar); err != nil {
			log.Printf("error writing bar for %s: %v", sp.Spec.Series, err)
		}

		if p.metrics != nil {
			p.metrics.BarsFlushedTotal.Add(context.Background(), 1)
		}

		// Call external sink (e.g., Kafka push)
		if p.barSink != nil {
			p.barSink(bar)
		}
	}
}

// GetSeriesPipeline returns the pipeline for a given series name.
func (p *Pipeline) GetSeriesPipeline(name string) (*SeriesPipeline, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	sp, ok := p.series[name]
	return sp, ok
}

// SeriesIDs returns a map of series name → series ID.
func (p *Pipeline) SeriesIDs() map[string]uint16 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ids := make(map[string]uint16, len(p.series))
	for name, sp := range p.series {
		ids[name] = sp.SeriesID
	}
	return ids
}

// stableSeriesID derives a deterministic series ID from the series name.
// This ensures adding/removing/reordering spec files doesn't change existing IDs.
// ID 0 is reserved. Collisions are possible in a 16-bit space; NewPipeline
// detects them at startup and returns an error.
func stableSeriesID(name string) uint16 {
	h := fnv.New32a()
	h.Write([]byte(name))
	id := uint16(h.Sum32()%65535) + 1 // range [1, 65535]
	return id
}

// Close gracefully shuts down all series pipelines.
func (p *Pipeline) Close() {
	p.mu.Lock()
	for _, sp := range p.series {
		sp.Batcher.Close()
		sp.Rollup.Close() // closes BarChan, which unblocks consumeBars
	}
	p.mu.Unlock()

	p.wg.Wait() // wait for all consumeBars goroutines to finish
}
