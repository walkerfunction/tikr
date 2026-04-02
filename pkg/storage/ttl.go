package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

// TTLConfig holds the TTL durations for lazy expiry checks on read.
type TTLConfig struct {
	TicksTTL  time.Duration
	RollupTTL time.Duration
}

// ticksCutoffNs returns the nanosecond cutoff for ticks.
// Keys with timestamps before this value are considered expired.
// Returns 0 if no TTL is configured (nothing expires).
// If the TTL is so large that the cutoff would be before the Unix epoch,
// returns 0 to prevent underflow (nothing expires).
func (c TTLConfig) ticksCutoffNs() uint64 {
	return cutoffNs(c.TicksTTL)
}

// rollupCutoffNs returns the nanosecond cutoff for rollup bars.
func (c TTLConfig) rollupCutoffNs() uint64 {
	return cutoffNs(c.RollupTTL)
}

// cutoffNs computes the nanosecond cutoff for a given TTL duration.
// Returns 0 if TTL is zero/negative or if the cutoff would be before the epoch.
func cutoffNs(ttl time.Duration) uint64 {
	if ttl <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-ttl)
	if cutoff.UnixNano() < 0 {
		return 0 // TTL is longer than the epoch; nothing expires
	}
	return uint64(cutoff.UnixNano())
}

// cutoffNsAt computes the nanosecond cutoff for a given TTL relative to a fixed
// "now" timestamp. This avoids clock skew when multiple cutoffs are needed.
func cutoffNsAt(ttl time.Duration, now time.Time) uint64 {
	if ttl <= 0 {
		return 0
	}
	cutoff := now.Add(-ttl)
	if cutoff.UnixNano() < 0 {
		return 0
	}
	return uint64(cutoff.UnixNano())
}

// Watermark meta key prefix for storing the last-reaped cutoff per data prefix.
// Full key: "reap:hwm:" + [data_prefix:1] → value: [cutoff_ns:8]
const watermarkPrefix = "reap:hwm:"

// readWatermark reads the last-reaped cutoff for a data prefix from meta.
// Returns 0 if no watermark exists (first reap cycle).
func readWatermark(blob Blob, dataPrefix byte) (uint64, error) {
	key := PrefixedKey(PrefixMeta, append([]byte(watermarkPrefix), dataPrefix))
	val, err := blob.Get(key)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading watermark: %w", err)
	}

	if len(val) < 8 {
		return 0, nil // corrupt watermark, treat as first run
	}
	return binary.BigEndian.Uint64(val[:8]), nil
}

// writeWatermark persists the last-reaped cutoff for a data prefix.
func writeWatermark(blob Blob, dataPrefix byte, cutoffNs uint64) error {
	key := PrefixedKey(PrefixMeta, append([]byte(watermarkPrefix), dataPrefix))
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, cutoffNs)
	return blob.Set(key, val, true)
}

// Reaper writes DeleteRange tombstones for expired data.
//
// Design: registry-free, watermark-based incremental tombstoning.
//
// Instead of maintaining a group registry in meta (which the writer must
// populate and the reaper must prune), the reaper discovers groups by
// scanning the data keyspace directly. It hops between groups using SeekGE
// — cost is O(groups) seeks, each O(log N) in Pebble.
//
// A per-prefix watermark tracks the last-reaped cutoff. Each cycle issues
// DeleteRange(last_cutoff, new_cutoff) per group — never overlapping with
// previous cycles. This eliminates write amplification from redundant
// tombstones.
//
// DeleteRange is [start, end) — exclusive upper bound. We use cutoff+1 as
// the end key to include keys at exactly the cutoff timestamp.
//
// Compaction automatically discards tombstoned keys during merges.
type Reaper struct {
	engine   *Engine
	ttl      TTLConfig
	interval time.Duration

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// TickReapErrors counts consecutive tick reap failures. Exposed for monitoring.
	TickReapErrors uint64
	// RollupReapErrors counts consecutive rollup reap failures.
	RollupReapErrors uint64
}

// NewReaper creates a reaper that periodically tombstones expired data.
// Panics if interval is not positive (same contract as time.NewTicker).
func NewReaper(engine *Engine, ttl TTLConfig, interval time.Duration) *Reaper {
	if interval <= 0 {
		panic(fmt.Sprintf("storage.NewReaper: interval must be positive, got %s", interval))
	}
	return &Reaper{
		engine:   engine,
		ttl:      ttl,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

// Start begins the background reap loop.
func (r *Reaper) Start() {
	r.wg.Add(1)
	go r.loop()
}

// Stop signals the reaper to stop and waits for it to finish.
// Safe to call multiple times.
func (r *Reaper) Stop() {
	r.stopOnce.Do(func() { close(r.stop) })
	r.wg.Wait()
}

// ReapOnce runs a single reap cycle synchronously.
// Useful for testing and manual invocation.
func (r *Reaper) ReapOnce() {
	r.reap()
}

func (r *Reaper) loop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.reap()
		}
	}
}

func (r *Reaper) reap() {
	now := time.Now() // single timestamp for consistent cutoffs

	if r.ttl.TicksTTL > 0 {
		cutoff := cutoffNsAt(r.ttl.TicksTTL, now)
		if cutoff != 0 {
			n, err := r.tombstonePrefix(PrefixTicks, cutoff)
			if err != nil {
				r.TickReapErrors++
				log.Printf("reaper: ticks error (consecutive=%d): %v", r.TickReapErrors, err)
			} else {
				r.TickReapErrors = 0
				if n > 0 {
					log.Printf("reaper: tombstoned %d tick group(s)", n)
				}
			}
		}
	}

	if r.ttl.RollupTTL > 0 {
		cutoff := cutoffNsAt(r.ttl.RollupTTL, now)
		if cutoff != 0 {
			n, err := r.tombstonePrefix(PrefixRollup, cutoff)
			if err != nil {
				r.RollupReapErrors++
				log.Printf("reaper: rollup error (consecutive=%d): %v", r.RollupReapErrors, err)
			} else {
				r.RollupReapErrors = 0
				if n > 0 {
					log.Printf("reaper: tombstoned %d rollup group(s)", n)
				}
			}
		}
	}
}

// tombstonePrefix discovers all (series_id, dim_hash) groups in the data
// prefix by hopping through the keyspace, then issues an incremental
// DeleteRange per group from the last watermark to the new cutoff.
//
// Data key layout: [prefix:1][series_id:2][dim_hash:8][timestamp:8][optional seq]
// Group identity:  [prefix:1][series_id:2][dim_hash:8] = 11 bytes
//
// For each group discovered, we issue:
//
//	DeleteRange([prefix][sid][dh][last_cutoff], [prefix][sid][dh][new_cutoff+1])
//
// The +1 on new_cutoff makes the half-open range [start, end) inclusive of
// keys at exactly new_cutoff.
func (r *Reaper) tombstonePrefix(dataPrefix byte, newCutoff uint64) (int, error) {
	if dataPrefix == 0xFF {
		return 0, fmt.Errorf("cannot reap prefix 0xFF: no valid upper bound")
	}

	blob := r.engine.blob

	// Read watermark: where we left off last cycle
	lastCutoff, err := readWatermark(blob, dataPrefix)
	if err != nil {
		return 0, fmt.Errorf("reading watermark: %w", err)
	}

	// Nothing new to expire since last cycle
	if newCutoff <= lastCutoff {
		return 0, nil
	}

	// Discover groups by hopping through the data keyspace
	prefixStart := []byte{dataPrefix}
	prefixEnd := []byte{dataPrefix + 1}

	iter, err := blob.NewIterator(prefixStart, prefixEnd)
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer iter.Close()

	type group struct {
		seriesID uint16
		dimHash  uint64
	}
	var groups []group

	// Start at the beginning of the prefix
	for iter.First(); iter.Valid(); {
		key := iter.Key()
		// Need at least prefix(1) + sid(2) + dh(8) = 11 bytes
		if len(key) < 11 {
			iter.Next()
			continue
		}

		sid := binary.BigEndian.Uint16(key[1:3])
		dh := binary.BigEndian.Uint64(key[3:11])
		groups = append(groups, group{seriesID: sid, dimHash: dh})

		// Hop to the next group by seeking past this (sid, dh)
		seekKey := nextGroupKey(dataPrefix, sid, dh)
		if seekKey == nil {
			break // exhausted the prefix
		}
		iter.SeekGE(seekKey)
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("discovering groups: %w", err)
	}

	if len(groups) == 0 {
		return 0, nil
	}

	batch := blob.NewBatch()
	defer batch.Close()

	for _, g := range groups {
		// Range start: [prefix][sid][dh][last_cutoff]
		start := make([]byte, 1+2+8+8)
		start[0] = dataPrefix
		binary.BigEndian.PutUint16(start[1:3], g.seriesID)
		binary.BigEndian.PutUint64(start[3:11], g.dimHash)
		binary.BigEndian.PutUint64(start[11:19], lastCutoff)

		// Range end: [prefix][sid][dh][new_cutoff+1]
		// DeleteRange is [start, end) — adding 1 makes it inclusive
		// of keys at exactly newCutoff.
		end := make([]byte, 1+2+8+8)
		end[0] = dataPrefix
		binary.BigEndian.PutUint16(end[1:3], g.seriesID)
		binary.BigEndian.PutUint64(end[3:11], g.dimHash)
		binary.BigEndian.PutUint64(end[11:19], newCutoff+1)

		if err := batch.DeleteRange(start, end); err != nil {
			return 0, fmt.Errorf("delete range: %w", err)
		}
	}

	if err := batch.Commit(true); err != nil {
		return 0, fmt.Errorf("committing tombstones: %w", err)
	}

	// Advance the watermark after successful commit
	if err := writeWatermark(blob, dataPrefix, newCutoff); err != nil {
		// Tombstones are committed; watermark write failure means next cycle
		// will re-tombstone the same range (harmless overlap, not data loss).
		log.Printf("reaper: warning: watermark write failed (will retry): %v", err)
	}

	return len(groups), nil
}

// nextGroupKey returns the seek key for the next group after (sid, dh).
// Handles overflow: dh=MaxUint64 → increment sid; sid=MaxUint16 → nil (done).
func nextGroupKey(prefix byte, sid uint16, dh uint64) []byte {
	if dh < math.MaxUint64 {
		key := make([]byte, 1+2+8)
		key[0] = prefix
		binary.BigEndian.PutUint16(key[1:3], sid)
		binary.BigEndian.PutUint64(key[3:11], dh+1)
		return key
	}
	if sid < math.MaxUint16 {
		key := make([]byte, 1+2)
		key[0] = prefix
		binary.BigEndian.PutUint16(key[1:3], sid+1)
		return key
	}
	return nil // exhausted all groups in this prefix
}
