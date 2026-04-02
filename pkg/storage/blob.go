package storage

import "errors"

// ErrNotFound is returned by Blob.Get when a key does not exist.
var ErrNotFound = errors.New("key not found")

// Blob is the storage abstraction for a sorted key-value store.
// Implementations may be backed by Pebble, RocksDB, or any LSM/B-tree engine.
//
// All keys are opaque byte slices, lexicographically ordered.
// Values are opaque blobs — the storage layer does not interpret them.
type Blob interface {
	// Get retrieves the value for a key. Returns ErrNotFound if the key
	// does not exist. The returned byte slice is owned by the caller.
	Get(key []byte) ([]byte, error)

	// Set writes a key-value pair. If sync is true, the write is durable
	// before returning (WAL flush).
	Set(key, value []byte, sync bool) error

	// Delete removes a single key.
	Delete(key []byte, sync bool) error

	// NewBatch creates a write batch for atomic multi-key mutations.
	NewBatch() Batch

	// NewIterator creates a forward iterator bounded by [lower, upper).
	// Both bounds are optional: nil lower starts at the beginning,
	// nil upper iterates to the end.
	NewIterator(lower, upper []byte) (Iterator, error)

	// Close shuts down the store and releases resources.
	Close() error
}

// Batch accumulates mutations and applies them atomically on Commit.
type Batch interface {
	// Set adds a key-value write to the batch.
	Set(key, value []byte) error

	// Delete adds a single-key delete to the batch.
	Delete(key []byte) error

	// DeleteRange adds a range tombstone for keys in [start, end).
	DeleteRange(start, end []byte) error

	// Commit applies all accumulated mutations atomically.
	// If sync is true, the write is durable before returning.
	Commit(sync bool) error

	// Close releases batch resources. Must be called even after Commit.
	Close() error
}

// Iterator provides forward iteration over key-value pairs.
// Usage pattern:
//
//	iter, _ := blob.NewIterator(lower, upper)
//	defer iter.Close()
//	for iter.First(); iter.Valid(); iter.Next() {
//	    key, val := iter.Key(), iter.Value()
//	}
//	if err := iter.Error(); err != nil { ... }
type Iterator interface {
	// First moves to the first key in the range. Returns true if valid.
	First() bool

	// Next advances to the next key. Returns true if valid.
	Next() bool

	// Valid returns true if the iterator is positioned at a valid key.
	Valid() bool

	// SeekGE moves to the first key >= target. Returns true if valid.
	SeekGE(target []byte) bool

	// Key returns the current key. Only valid when Valid() is true.
	// The returned slice is only valid until the next iterator operation.
	Key() []byte

	// Value returns the current value. Only valid when Valid() is true.
	// The returned slice is only valid until the next iterator operation.
	Value() []byte

	// Error returns any accumulated error from iteration.
	Error() error

	// Close releases iterator resources.
	Close() error
}

// NamespaceSupport is an optional interface for backends that support
// native namespaces (e.g., RocksDB column families). Backends that don't
// support this use key-prefix emulation instead (the default).
type NamespaceSupport interface {
	// CreateNamespace creates a named namespace. No-op if already exists.
	CreateNamespace(name string) error

	// DropNamespace deletes a namespace and all its data.
	DropNamespace(name string) error

	// Namespace returns a Blob scoped to the given namespace.
	// All operations on the returned Blob are isolated to that namespace.
	Namespace(name string) (Blob, error)
}

// TTLSupport is an optional interface for backends with native TTL
// (e.g., RocksDB with CompactionFilter). When available, the engine
// delegates expiry to the backend instead of running the reaper.
type TTLSupport interface {
	// SetTTL configures automatic expiry for keys written after this call.
	// ttlNanos is the time-to-live in nanoseconds for newly written keys.
	// Pass 0 to disable TTL.
	SetTTL(namespace string, ttlNanos int64) error
}
