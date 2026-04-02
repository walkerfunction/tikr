package storage

import (
	"encoding/json"
	"fmt"

	"github.com/walkerfunction/tikr/pkg/core"
)

// Writer handles batch writes to the storage backend.
type Writer struct {
	engine *Engine
}

// NewWriter creates a new Writer.
func NewWriter(engine *Engine) *Writer {
	return &Writer{engine: engine}
}

// tickValue serializes tick fields for storage.
type tickValue struct {
	Dimensions map[string]string `json:"d"`
	Fields     map[string]int64  `json:"f"`
}

// WriteTicks writes a batch of ticks to the ticks prefix.
func (w *Writer) WriteTicks(seriesID uint16, ticks []core.Tick) error {
	batch := w.engine.blob.NewBatch()
	defer batch.Close()

	for _, tick := range ticks {
		dimHash := core.DimensionHash(tick.Dimensions)
		rawKey := core.EncodeTickKey(seriesID, dimHash, tick.TimestampNs, tick.Sequence)
		key := PrefixedKey(PrefixTicks, rawKey)

		val, err := json.Marshal(tickValue{
			Dimensions: tick.Dimensions,
			Fields:     tick.Fields,
		})
		if err != nil {
			return fmt.Errorf("marshaling tick: %w", err)
		}

		if err := batch.Set(key, val); err != nil {
			return fmt.Errorf("batch set tick: %w", err)
		}
	}

	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing tick batch: %w", err)
	}

	return nil
}

// WriteBar writes a single rolled-up bar to the rollup prefix.
func (w *Writer) WriteBar(seriesID uint16, bar *core.Bar) error {
	dimHash := core.DimensionHash(bar.Dimensions)
	rawKey := core.EncodeRollupKey(seriesID, dimHash, bar.BucketTs)
	key := PrefixedKey(PrefixRollup, rawKey)

	val, err := json.Marshal(bar)
	if err != nil {
		return fmt.Errorf("marshaling bar: %w", err)
	}

	if err := w.engine.blob.Set(key, val, true); err != nil {
		return fmt.Errorf("writing bar: %w", err)
	}

	return nil
}

// WriteBars writes multiple rolled-up bars in a batch.
func (w *Writer) WriteBars(seriesID uint16, bars []*core.Bar) error {
	batch := w.engine.blob.NewBatch()
	defer batch.Close()

	for _, bar := range bars {
		dimHash := core.DimensionHash(bar.Dimensions)
		rawKey := core.EncodeRollupKey(seriesID, dimHash, bar.BucketTs)
		key := PrefixedKey(PrefixRollup, rawKey)

		val, err := json.Marshal(bar)
		if err != nil {
			return fmt.Errorf("marshaling bar: %w", err)
		}

		if err := batch.Set(key, val); err != nil {
			return fmt.Errorf("batch set bar: %w", err)
		}
	}

	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing bar batch: %w", err)
	}

	return nil
}

// WriteMeta writes a key-value pair to the meta prefix.
func (w *Writer) WriteMeta(key, value []byte) error {
	pk := PrefixedKey(PrefixMeta, key)
	if err := w.engine.blob.Set(pk, value, true); err != nil {
		return fmt.Errorf("writing meta: %w", err)
	}
	return nil
}
