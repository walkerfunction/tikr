package storage

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// Key prefixes act as virtual "column families" within a single Pebble instance.
// All keys are prefixed with one byte to separate data types.
const (
	PrefixTicks  byte = 0x01
	PrefixRollup byte = 0x02
	PrefixMeta   byte = 0x03
)

// EngineConfig holds storage tuning parameters.
type EngineConfig struct {
	DataDir       string
	TicksTTL      time.Duration
	TicksMaxSize  int64 // bytes
	RollupTTL     time.Duration
	RollupMaxSize int64 // bytes
}

// Engine wraps a Pebble instance.
type Engine struct {
	db *pebble.DB
}

// NewEngine opens Pebble with tuned options.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	opts := &pebble.Options{
		// Use prefix-based bloom filter for efficient prefix scans
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 64 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{TargetFileSize: 128 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{TargetFileSize: 256 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
		},
		MemTableSize:                64 * 1024 * 1024, // 64MB memtable
		MemTableStopWritesThreshold: 4,
		MaxOpenFiles:                1000,
	}

	db, err := pebble.Open(cfg.DataDir, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble at %s: %w", cfg.DataDir, err)
	}

	return &Engine{db: db}, nil
}

// DB returns the underlying Pebble instance.
func (e *Engine) DB() *pebble.DB {
	return e.db
}

// Close shuts down the engine cleanly.
func (e *Engine) Close() error {
	return e.db.Close()
}

// PrefixedKey prepends the prefix byte to a key.
func PrefixedKey(prefix byte, key []byte) []byte {
	pk := make([]byte, 1+len(key))
	pk[0] = prefix
	copy(pk[1:], key)
	return pk
}

// PrefixUpperBound returns the upper bound for prefix iteration.
// For prefix byte 0x01, this returns 0x02.
func PrefixUpperBound(prefix byte) []byte {
	return []byte{prefix + 1}
}
