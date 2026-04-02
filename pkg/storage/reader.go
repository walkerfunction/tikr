package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/walkerfunction/tikr/pkg/core"
)

// Reader handles reads from the storage backend.
type Reader struct {
	engine *Engine
	ttl    TTLConfig
}

// NewReader creates a new Reader.
func NewReader(engine *Engine) *Reader {
	return &Reader{engine: engine}
}

// NewReaderWithTTL creates a Reader that skips expired keys on read.
func NewReaderWithTTL(engine *Engine, ttl TTLConfig) *Reader {
	return &Reader{engine: engine, ttl: ttl}
}

// ReadTicks scans ticks for a given series+dimension in a time range.
// If limit > 0, iteration stops after that many results.
func (r *Reader) ReadTicks(seriesID uint16, dimHash uint64, startNs, endNs uint64, limit uint32) ([]core.Tick, error) {
	startRaw, endRaw := core.TickKeyRange(seriesID, dimHash, startNs, endNs)
	startKey := PrefixedKey(PrefixTicks, startRaw)
	endKey := PrefixedKey(PrefixTicks, endRaw)

	iter, err := r.engine.blob.NewIterator(startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("creating tick iterator: %w", err)
	}
	defer iter.Close()

	var ticks []core.Tick
	cutoff := r.ttl.ticksCutoffNs()

	for iter.First(); iter.Valid(); iter.Next() {
		if limit > 0 && uint32(len(ticks)) >= limit {
			break
		}

		// Strip the prefix byte before decoding
		rawKey := iter.Key()[1:]

		// Lazy TTL: skip expired keys without decoding the value.
		// Length guard preserves DecodeTickKey's clean error for malformed keys.
		if cutoff > 0 && len(rawKey) >= 18 {
			tsNs := binary.BigEndian.Uint64(rawKey[10:18])
			if tsNs < cutoff {
				continue
			}
		}

		_, _, tsNs, seq, err := core.DecodeTickKey(rawKey)
		if err != nil {
			return nil, fmt.Errorf("decoding tick key: %w", err)
		}

		var tv tickValue
		if err := json.Unmarshal(iter.Value(), &tv); err != nil {
			return nil, fmt.Errorf("unmarshaling tick value: %w", err)
		}

		ticks = append(ticks, core.Tick{
			TimestampNs: tsNs,
			Dimensions:  tv.Dimensions,
			Fields:      tv.Fields,
			Sequence:    seq,
		})
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating ticks: %w", err)
	}

	return ticks, nil
}

// ReadBars scans rolled-up bars for a given series+dimension in a time range.
func (r *Reader) ReadBars(seriesID uint16, dimHash uint64, startTs, endTs uint64) ([]*core.Bar, error) {
	startRaw, endRaw := core.RollupKeyRange(seriesID, dimHash, startTs, endTs)
	startKey := PrefixedKey(PrefixRollup, startRaw)
	endKey := PrefixedKey(PrefixRollup, endRaw)

	iter, err := r.engine.blob.NewIterator(startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("creating bar iterator: %w", err)
	}
	defer iter.Close()

	var bars []*core.Bar
	cutoff := r.ttl.rollupCutoffNs()

	for iter.First(); iter.Valid(); iter.Next() {
		// Lazy TTL: skip expired keys without decoding the value.
		if cutoff > 0 {
			rawKey := iter.Key()[1:]
			if len(rawKey) >= 18 {
				bucketTs := binary.BigEndian.Uint64(rawKey[10:18])
				if bucketTs < cutoff {
					continue
				}
			}
		}

		var bar core.Bar
		if err := json.Unmarshal(iter.Value(), &bar); err != nil {
			return nil, fmt.Errorf("unmarshaling bar: %w", err)
		}
		bars = append(bars, &bar)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating bars: %w", err)
	}

	return bars, nil
}

// ReadMeta reads a value from the meta prefix.
func (r *Reader) ReadMeta(key []byte) ([]byte, error) {
	pk := PrefixedKey(PrefixMeta, key)
	val, err := r.engine.blob.Get(pk)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading meta: %w", err)
	}
	return val, nil
}
