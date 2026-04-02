package storage

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
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
	BackendPebble  Backend = "pebble"
	BackendRocksDB Backend = "rocksdb"
)

// BackendOpener is a function that opens a Blob store at the given directory.
type BackendOpener func(dir string) (Blob, error)

// backends is the registry of available storage backends.
// Backends register themselves via init() using RegisterBackend.
var (
	backendsMu sync.Mutex
	backends   = map[Backend]BackendOpener{}
)

// RegisterBackend registers a storage backend opener.
// Called from init() in backend-specific files (e.g., rocksdb.go).
// Panics on nil opener or duplicate registration.
func RegisterBackend(name Backend, opener BackendOpener) {
	if opener == nil {
		panic(fmt.Sprintf("storage.RegisterBackend: nil opener for %q", name))
	}
	backendsMu.Lock()
	defer backendsMu.Unlock()
	if _, dup := backends[name]; dup {
		panic(fmt.Sprintf("storage.RegisterBackend: duplicate backend %q", name))
	}
	backends[name] = opener
}

// availableBackends returns a sorted list of registered backend names
// (always includes "pebble" which is hard-coded).
func availableBackends() string {
	backendsMu.Lock()
	defer backendsMu.Unlock()
	names := []string{string(BackendPebble)}
	for name := range backends {
		names = append(names, string(name))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

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
	blob   Blob
	reaper *Reaper
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
		backendsMu.Lock()
		opener, ok := backends[backend]
		backendsMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("unknown storage backend: %q (available: %s)", backend, availableBackends())
		}
		blob, err = opener(cfg.DataDir)
		if err != nil {
			return nil, fmt.Errorf("opening %s: %w", backend, err)
		}
	}

	// If the backend supports native TTL, delegate to it.
	// Otherwise, start the reaper for application-level TTL enforcement.
	nativeTTL := false
	if ts, ok := blob.(TTLSupport); ok {
		nativeTTL = true
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
	}

	if cfg.TicksMaxSize > 0 || cfg.RollupMaxSize > 0 {
		log.Printf("storage: max_size_gb is advisory only — %s does not enforce size-based limits", backend)
	}

	log.Printf("storage: opened %s at %s", backend, cfg.DataDir)

	e := &Engine{blob: blob}

	// Start reaper only when the backend lacks native TTL support.
	// Backends with native TTL handle expiry in compaction — no reaper needed.
	if !nativeTTL && (cfg.TicksTTL > 0 || cfg.RollupTTL > 0) {
		ttl := TTLConfig{TicksTTL: cfg.TicksTTL, RollupTTL: cfg.RollupTTL}
		reaper := NewReaper(e, ttl, 10*time.Minute)
		reaper.Start()
		e.reaper = reaper
		log.Printf("storage: reaper started (ticks TTL: %s, rollup TTL: %s, interval: 10m)", cfg.TicksTTL, cfg.RollupTTL)
	}

	return e, nil
}

// Blob returns the underlying Blob store.
func (e *Engine) Blob() Blob {
	return e.blob
}

// Close shuts down the engine cleanly.
func (e *Engine) Close() error {
	if e.reaper != nil {
		e.reaper.Stop()
	}
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
