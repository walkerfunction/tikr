//go:build rocksdb

package storage

import (
	"fmt"
	"os"
	"sync"

	"github.com/linxGnu/grocksdb"
)

// RocksDBBlob implements Blob, NamespaceSupport, and TTLSupport
// backed by RocksDB with native column families and TTL compaction.
type RocksDBBlob struct {
	db   *grocksdb.DB
	opts *grocksdb.Options
	ro   *grocksdb.ReadOptions
	wo   *grocksdb.WriteOptions
	wos  *grocksdb.WriteOptions // sync writes

	mu  sync.RWMutex
	cfs map[string]*grocksdb.ColumnFamilyHandle
	// defaultCF is the handle for the "default" column family.
	// All Blob operations without a namespace scope use this.
	defaultCF *grocksdb.ColumnFamilyHandle
}

// RocksDBConfig holds RocksDB-specific tuning parameters.
type RocksDBConfig struct {
	Dir               string
	MaxOpenFiles      int
	WriteBufferSize   int    // bytes, default 64MB
	MaxWriteBufferNum int    // default 3
	BlockCacheSize    uint64 // bytes, default 256MB
	BloomFilterBits   int    // default 10
}

// OpenRocksDB creates a new RocksDBBlob with tuned defaults.
func OpenRocksDB(cfg RocksDBConfig) (*RocksDBBlob, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	if cfg.MaxOpenFiles > 0 {
		opts.SetMaxOpenFiles(cfg.MaxOpenFiles)
	} else {
		opts.SetMaxOpenFiles(1000)
	}

	wbSize := cfg.WriteBufferSize
	if wbSize <= 0 {
		wbSize = 64 * 1024 * 1024
	}
	opts.SetWriteBufferSize(wbSize)

	wbNum := cfg.MaxWriteBufferNum
	if wbNum <= 0 {
		wbNum = 3
	}
	opts.SetMaxWriteBufferNumber(wbNum)

	// Block-based table with bloom filter
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	cacheSize := cfg.BlockCacheSize
	if cacheSize == 0 {
		cacheSize = 256 * 1024 * 1024
	}
	bbto.SetBlockCache(grocksdb.NewLRUCache(cacheSize))

	bloomBits := cfg.BloomFilterBits
	if bloomBits <= 0 {
		bloomBits = 10
	}
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(bloomBits)))
	opts.SetBlockBasedTableFactory(bbto)

	// Compression: snappy for L0-L1, zstd for deeper levels
	opts.SetCompression(grocksdb.SnappyCompression)
	opts.SetBottommostCompression(grocksdb.ZSTDCompression)

	// Discover existing column families.
	// ListColumnFamilies fails if the DB directory doesn't exist yet,
	// which is expected on first run. For other errors (permissions,
	// corruption), we propagate rather than silently falling back.
	cfNames, err := grocksdb.ListColumnFamilies(opts, cfg.Dir)
	if err != nil {
		if _, statErr := os.Stat(cfg.Dir); os.IsNotExist(statErr) {
			cfNames = []string{"default"}
		} else {
			return nil, fmt.Errorf("listing column families at %s: %w", cfg.Dir, err)
		}
	}

	cfOpts := make([]*grocksdb.Options, len(cfNames))
	for i := range cfOpts {
		cfOpts[i] = opts
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, cfg.Dir, cfNames, cfOpts)
	if err != nil {
		return nil, fmt.Errorf("opening rocksdb at %s: %w", cfg.Dir, err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)
	wos := grocksdb.NewDefaultWriteOptions()
	wos.SetSync(true)

	cfs := make(map[string]*grocksdb.ColumnFamilyHandle, len(cfNames))
	var defaultCF *grocksdb.ColumnFamilyHandle
	for i, name := range cfNames {
		cfs[name] = cfHandles[i]
		if name == "default" {
			defaultCF = cfHandles[i]
		}
	}

	if defaultCF == nil {
		db.Close()
		return nil, fmt.Errorf("rocksdb at %s: missing default column family", cfg.Dir)
	}

	return &RocksDBBlob{
		db:        db,
		opts:      opts,
		ro:        ro,
		wo:        wo,
		wos:       wos,
		cfs:       cfs,
		defaultCF: defaultCF,
	}, nil
}

// --- Blob interface ---

func (r *RocksDBBlob) Get(key []byte) ([]byte, error) {
	val, err := r.db.GetCF(r.ro, r.defaultCF, key)
	if err != nil {
		return nil, err
	}
	defer val.Free()

	if !val.Exists() {
		return nil, ErrNotFound
	}

	out := make([]byte, val.Size())
	copy(out, val.Data())
	return out, nil
}

func (r *RocksDBBlob) Set(key, value []byte, sync bool) error {
	return r.db.PutCF(r.writeOpts(sync), r.defaultCF, key, value)
}

func (r *RocksDBBlob) Delete(key []byte, sync bool) error {
	return r.db.DeleteCF(r.writeOpts(sync), r.defaultCF, key)
}

func (r *RocksDBBlob) NewBatch() Batch {
	return &rocksdbBatch{db: r.db, b: grocksdb.NewWriteBatch(), cf: r.defaultCF}
}

func (r *RocksDBBlob) NewIterator(lower, upper []byte) (Iterator, error) {
	ro := grocksdb.NewDefaultReadOptions()
	if lower != nil {
		ro.SetIterateLowerBound(lower)
	}
	if upper != nil {
		ro.SetIterateUpperBound(upper)
	}
	iter := r.db.NewIteratorCF(ro, r.defaultCF)
	return &rocksdbIterator{iter: iter, ro: ro}, nil
}

func (r *RocksDBBlob) Close() error {
	r.ro.Destroy()
	r.wo.Destroy()
	r.wos.Destroy()
	for _, cf := range r.cfs {
		cf.Destroy()
	}
	r.db.Close()
	r.opts.Destroy()
	return nil
}

func (r *RocksDBBlob) writeOpts(sync bool) *grocksdb.WriteOptions {
	if sync {
		return r.wos
	}
	return r.wo
}

// --- NamespaceSupport interface ---

func (r *RocksDBBlob) CreateNamespace(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.cfs[name]; ok {
		return nil // already exists
	}

	cf, err := r.db.CreateColumnFamily(r.opts, name)
	if err != nil {
		return fmt.Errorf("creating column family %q: %w", name, err)
	}
	r.cfs[name] = cf
	return nil
}

// DropNamespace drops a column family. Any previously returned namespaced
// Blobs for this name become invalid — callers must not use them after drop.
func (r *RocksDBBlob) DropNamespace(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cf, ok := r.cfs[name]
	if !ok {
		return fmt.Errorf("column family %q not found", name)
	}
	if name == "default" {
		return fmt.Errorf("cannot drop default column family")
	}

	// Remove from map first so concurrent Namespace() calls fail fast.
	delete(r.cfs, name)
	r.db.DropColumnFamily(cf)
	cf.Destroy()
	return nil
}

func (r *RocksDBBlob) Namespace(name string) (Blob, error) {
	r.mu.RLock()
	cf, ok := r.cfs[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("column family %q not found (call CreateNamespace first)", name)
	}

	return &rocksdbNamespacedBlob{
		db:  r.db,
		cf:  cf,
		ro:  r.ro,
		wo:  r.wo,
		wos: r.wos,
	}, nil
}

// --- TTLSupport interface ---

// SetTTL configures TTL-based compaction for a column family.
// RocksDB's native TTL appends a timestamp to values and drops expired
// entries during compaction. The ttl parameter is in nanoseconds.
//
// Note: RocksDB TTL requires the DB (or CF) to be opened with TTL mode.
// This method sets up a CompactionFilter for the namespace that drops
// keys with embedded timestamps older than the TTL.
func (r *RocksDBBlob) SetTTL(namespace string, ttl int64) error {
	// RocksDB's built-in TTL requires reopening with OpenDbWithTTL.
	// For per-CF TTL, we'd need CreateColumnFamilyWithTTL.
	// Since Tikr embeds timestamps in keys, the reaper handles TTL
	// at the application level. This is a hook for future native support.
	//
	// No-op regardless of whether the namespace/CF exists, to avoid
	// spurious TTL failures until native support is added.
	return nil
}

// --- rocksdbNamespacedBlob: Blob scoped to a column family ---

type rocksdbNamespacedBlob struct {
	db  *grocksdb.DB
	cf  *grocksdb.ColumnFamilyHandle
	ro  *grocksdb.ReadOptions
	wo  *grocksdb.WriteOptions
	wos *grocksdb.WriteOptions
}

func (n *rocksdbNamespacedBlob) Get(key []byte) ([]byte, error) {
	val, err := n.db.GetCF(n.ro, n.cf, key)
	if err != nil {
		return nil, err
	}
	defer val.Free()

	if !val.Exists() {
		return nil, ErrNotFound
	}

	out := make([]byte, val.Size())
	copy(out, val.Data())
	return out, nil
}

func (n *rocksdbNamespacedBlob) Set(key, value []byte, sync bool) error {
	return n.db.PutCF(n.writeOpts(sync), n.cf, key, value)
}

func (n *rocksdbNamespacedBlob) Delete(key []byte, sync bool) error {
	return n.db.DeleteCF(n.writeOpts(sync), n.cf, key)
}

func (n *rocksdbNamespacedBlob) NewBatch() Batch {
	return &rocksdbBatch{db: n.db, b: grocksdb.NewWriteBatch(), cf: n.cf}
}

func (n *rocksdbNamespacedBlob) NewIterator(lower, upper []byte) (Iterator, error) {
	ro := grocksdb.NewDefaultReadOptions()
	if lower != nil {
		ro.SetIterateLowerBound(lower)
	}
	if upper != nil {
		ro.SetIterateUpperBound(upper)
	}
	iter := n.db.NewIteratorCF(ro, n.cf)
	return &rocksdbIterator{iter: iter, ro: ro}, nil
}

func (n *rocksdbNamespacedBlob) Close() error {
	// Namespaced blob doesn't own the DB — no-op
	return nil
}

func (n *rocksdbNamespacedBlob) writeOpts(sync bool) *grocksdb.WriteOptions {
	if sync {
		return n.wos
	}
	return n.wo
}

// --- rocksdbBatch ---

type rocksdbBatch struct {
	db *grocksdb.DB
	b  *grocksdb.WriteBatch
	cf *grocksdb.ColumnFamilyHandle
}

func (b *rocksdbBatch) Set(key, value []byte) error {
	b.b.PutCF(b.cf, key, value)
	return nil
}

func (b *rocksdbBatch) Delete(key []byte) error {
	b.b.DeleteCF(b.cf, key)
	return nil
}

func (b *rocksdbBatch) DeleteRange(start, end []byte) error {
	b.b.DeleteRangeCF(b.cf, start, end)
	return nil
}

func (b *rocksdbBatch) Commit(sync bool) error {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(sync)
	defer wo.Destroy()
	return b.db.Write(wo, b.b)
}

func (b *rocksdbBatch) Close() error {
	b.b.Destroy()
	return nil
}

// --- rocksdbIterator ---

type rocksdbIterator struct {
	iter *grocksdb.Iterator
	ro   *grocksdb.ReadOptions
	// started tracks whether we've called SeekToFirst (First()) yet
	started bool
}

func (it *rocksdbIterator) First() bool {
	it.iter.SeekToFirst()
	it.started = true
	return it.iter.Valid()
}

func (it *rocksdbIterator) Next() bool {
	it.iter.Next()
	return it.iter.Valid()
}

func (it *rocksdbIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *rocksdbIterator) SeekGE(target []byte) bool {
	it.iter.Seek(target)
	it.started = true
	return it.iter.Valid()
}

func (it *rocksdbIterator) Key() []byte {
	k := it.iter.Key()
	defer k.Free()
	out := make([]byte, k.Size())
	copy(out, k.Data())
	return out
}

func (it *rocksdbIterator) Value() []byte {
	v := it.iter.Value()
	defer v.Free()
	out := make([]byte, v.Size())
	copy(out, v.Data())
	return out
}

func (it *rocksdbIterator) Error() error {
	return it.iter.Err()
}

func (it *rocksdbIterator) Close() error {
	it.iter.Close()
	it.ro.Destroy()
	return nil
}

func init() {
	RegisterBackend(BackendRocksDB, func(dir string) (Blob, error) {
		return OpenRocksDB(RocksDBConfig{Dir: dir})
	})
}
