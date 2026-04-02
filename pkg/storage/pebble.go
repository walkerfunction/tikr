package storage

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// PebbleBlob implements Blob backed by a Pebble LSM store.
// Pebble does not support native namespaces or TTL — those are handled
// by key-prefix emulation and the Reaper, respectively.
type PebbleBlob struct {
	db *pebble.DB
}

// PebbleConfig holds Pebble-specific tuning parameters.
type PebbleConfig struct {
	Dir          string
	MemTableSize int // bytes, default 64MB
	MaxOpenFiles int // default 1000
}

// OpenPebble creates a new PebbleBlob with tuned defaults.
func OpenPebble(cfg PebbleConfig) (*PebbleBlob, error) {
	memSize := cfg.MemTableSize
	if memSize <= 0 {
		memSize = 64 * 1024 * 1024
	}
	maxOpen := cfg.MaxOpenFiles
	if maxOpen <= 0 {
		maxOpen = 1000
	}

	opts := &pebble.Options{
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 64 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{TargetFileSize: 128 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.SnappyCompression},
			{TargetFileSize: 256 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
			{TargetFileSize: 512 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.ZstdCompression},
		},
		MemTableSize:                uint64(memSize),
		MemTableStopWritesThreshold: 4,
		MaxOpenFiles:                maxOpen,
	}

	db, err := pebble.Open(cfg.Dir, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble at %s: %w", cfg.Dir, err)
	}

	return &PebbleBlob{db: db}, nil
}

func (p *PebbleBlob) Get(key []byte) ([]byte, error) {
	val, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	// Copy — val is only valid until closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

func (p *PebbleBlob) Set(key, value []byte, sync bool) error {
	return p.db.Set(key, value, pebbleWriteOpts(sync))
}

func (p *PebbleBlob) Delete(key []byte, sync bool) error {
	return p.db.Delete(key, pebbleWriteOpts(sync))
}

func (p *PebbleBlob) NewBatch() Batch {
	return &pebbleBatch{b: p.db.NewBatch()}
}

func (p *PebbleBlob) NewIterator(lower, upper []byte) (Iterator, error) {
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	return &pebbleIterator{iter: iter}, nil
}

func (p *PebbleBlob) Close() error {
	return p.db.Close()
}

// pebbleBatch wraps pebble.Batch to implement Batch.
type pebbleBatch struct {
	b *pebble.Batch
}

func (b *pebbleBatch) Set(key, value []byte) error {
	return b.b.Set(key, value, pebble.NoSync)
}

func (b *pebbleBatch) Delete(key []byte) error {
	return b.b.Delete(key, pebble.NoSync)
}

func (b *pebbleBatch) DeleteRange(start, end []byte) error {
	return b.b.DeleteRange(start, end, pebble.NoSync)
}

func (b *pebbleBatch) Commit(sync bool) error {
	return b.b.Commit(pebbleWriteOpts(sync))
}

func (b *pebbleBatch) Close() error {
	return b.b.Close()
}

// pebbleIterator wraps pebble.Iterator to implement Iterator.
type pebbleIterator struct {
	iter *pebble.Iterator
}

func (it *pebbleIterator) First() bool    { return it.iter.First() }
func (it *pebbleIterator) Next() bool     { return it.iter.Next() }
func (it *pebbleIterator) Valid() bool     { return it.iter.Valid() }
func (it *pebbleIterator) Key() []byte     { return it.iter.Key() }
func (it *pebbleIterator) Value() []byte   { return it.iter.Value() }
func (it *pebbleIterator) Error() error    { return it.iter.Error() }
func (it *pebbleIterator) Close() error    { return it.iter.Close() }

func (it *pebbleIterator) SeekGE(target []byte) bool {
	return it.iter.SeekGE(target)
}

func pebbleWriteOpts(sync bool) *pebble.WriteOptions {
	if sync {
		return pebble.Sync
	}
	return pebble.NoSync
}
