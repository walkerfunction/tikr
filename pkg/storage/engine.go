package storage

import (
	"fmt"
	"log"
	"time"
)

// Key prefixes act as virtual "column families" within a single Blob store.
// All keys are prefixed with one byte to separate data types.
const (
	PrefixTicks  byte = 0x01
	PrefixRollup byte = 0x02
	PrefixMeta   byte = 0x03
)

// Backend identifies the storage backend.
type Backend string

const (
	BackendPebble Backend = "pebble"
	// BackendRocksDB Backend = "rocksdb" // future
)

// EngineConfig holds storage tuning parameters.
type EngineConfig struct {
	Backend       Backend
	DataDir       string
	TicksTTL      time.Duration
	TicksMaxSize  int64 // bytes
	RollupTTL     time.Duration
	RollupMaxSize int64 // bytes
}

// Engine wraps a Blob store and manages its lifecycle.
type Engine struct {
	blob Blob
}

// NewEngine opens the configured storage backend.
// Backend defaults to "pebble" if empty.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	backend := cfg.Backend
	if backend == "" {
		backend = BackendPebble
	}

	var blob Blob
	var err error

	switch backend {
	case BackendPebble:
		blob, err = OpenPebble(PebbleConfig{Dir: cfg.DataDir})
		if err != nil {
			return nil, fmt.Errorf("opening pebble: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown storage backend: %q", backend)
	}

	// If the backend supports native TTL, delegate to it
	if ts, ok := blob.(TTLSupport); ok {
		if cfg.TicksTTL > 0 {
			if err := ts.SetTTL("ticks", cfg.TicksTTL.Nanoseconds()); err != nil {
				log.Printf("storage: failed to apply backend TTL for ticks (TTL will not be enforced): %v", err)
			}
		}
		if cfg.RollupTTL > 0 {
			if err := ts.SetTTL("rollup", cfg.RollupTTL.Nanoseconds()); err != nil {
				log.Printf("storage: failed to apply backend TTL for rollup (TTL will not be enforced): %v", err)
			}
		}
	} else if cfg.TicksTTL > 0 || cfg.RollupTTL > 0 {
		log.Printf("storage: %s does not support TTL; configured TTLs (ticks=%s, rollup=%s) will not be enforced", backend, cfg.TicksTTL, cfg.RollupTTL)
	}

	if cfg.TicksMaxSize > 0 || cfg.RollupMaxSize > 0 {
		log.Printf("storage: max_size_gb is advisory only — %s does not enforce size-based limits", backend)
	}

	log.Printf("storage: opened %s at %s", backend, cfg.DataDir)
	return &Engine{blob: blob}, nil
}

// Blob returns the underlying Blob store.
func (e *Engine) Blob() Blob {
	return e.blob
}

// Close shuts down the engine cleanly.
func (e *Engine) Close() error {
	return e.blob.Close()
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
// Panics if prefix is 0xFF (no valid upper bound exists).
func PrefixUpperBound(prefix byte) []byte {
	if prefix == 0xFF {
		panic("PrefixUpperBound: cannot compute upper bound for prefix 0xFF")
	}
	return []byte{prefix + 1}
}
