package storage

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
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

// regHeaderLen is the byte offset into a registry key where the group payload
// starts: meta prefix (1) + "grp:" (4) + data prefix (1) = 6.
const regHeaderLen = 1 + 4 + 1

// Reaper writes DeleteRange tombstones for expired key groups.
// It reads a group registry from the meta prefix (written by the Writer)
// instead of scanning all data keys — cost is O(groups), not O(keys).
// Pebble's compaction automatically discards tombstoned keys during merges.
//
// Note: data that is already older than the TTL at write time (e.g. late-arriving
// backfill) will be reaped on the next cycle. There is no grace period.
type Reaper struct {
	engine   *Engine
	ttl      TTLConfig
	interval time.Duration

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// ReapErrors counts consecutive reap failures. Exposed for monitoring.
	ReapErrors uint64
}

// NewReaper creates a reaper that periodically tombstones expired data.
func NewReaper(engine *Engine, ttl TTLConfig, interval time.Duration) *Reaper {
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
	now := time.Now() // M1: single timestamp for consistent cutoffs

	if r.ttl.TicksTTL > 0 {
		cutoff := cutoffNsAt(r.ttl.TicksTTL, now)
		n, err := r.tombstoneGroups(PrefixTicks, cutoff)
		if err != nil {
			r.ReapErrors++
			log.Printf("reaper: ticks error (consecutive=%d): %v", r.ReapErrors, err)
		} else {
			r.ReapErrors = 0
			if n > 0 {
				log.Printf("reaper: tombstoned %d tick group(s)", n)
			}
		}
	}

	if r.ttl.RollupTTL > 0 {
		cutoff := cutoffNsAt(r.ttl.RollupTTL, now)
		n, err := r.tombstoneGroups(PrefixRollup, cutoff)
		if err != nil {
			r.ReapErrors++
			log.Printf("reaper: rollup error (consecutive=%d): %v", r.ReapErrors, err)
		} else {
			r.ReapErrors = 0
			if n > 0 {
				log.Printf("reaper: tombstoned %d rollup group(s)", n)
			}
		}
	}
}

// tombstoneGroups reads the group registry from meta and issues a
// DeleteRange(ts=0, ts=cutoff) for each registered (series_id, dim_hash) group.
//
// Registry key layout in meta: "grp:" [prefix:1][series_id:2][dim_hash:8]
// Data key layout: [prefix:1][series_id:2][dim_hash:8][timestamp:8][optional seq]
//
// For each group, we issue:
//   DeleteRange([prefix][sid][dh][ts=0], [prefix][sid][dh][ts=cutoff])
//
// This is O(registered groups), not O(data keys).
func (r *Reaper) tombstoneGroups(dataPrefix byte, cutoffNs uint64) (int, error) {
	// H3: guard against prefix overflow (0xFF + 1 wraps to 0x00)
	if dataPrefix == 0xFF {
		return 0, fmt.Errorf("cannot reap prefix 0xFF: no valid upper bound")
	}

	// H2: explicit byte construction instead of fragile append on []byte("grp:")
	regStart := PrefixedKey(PrefixMeta, []byte{'g', 'r', 'p', ':', dataPrefix})
	regEnd := PrefixedKey(PrefixMeta, []byte{'g', 'r', 'p', ':', dataPrefix + 1})

	iter, err := r.engine.db.NewIter(&pebble.IterOptions{
		LowerBound: regStart,
		UpperBound: regEnd,
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	// Collect all registered groups
	type group struct {
		seriesID uint16
		dimHash  uint64
	}
	var groups []group

	for iter.First(); iter.Valid(); iter.Next() {
		// Key: [meta_prefix:1]["grp:":4][data_prefix:1][series_id:2][dim_hash:8]
		raw := iter.Key()
		// L4: use named constant instead of magic number 6
		payload := raw[regHeaderLen:]
		if len(payload) < 10 {
			continue
		}
		groups = append(groups, group{
			seriesID: binary.BigEndian.Uint16(payload[0:2]),
			dimHash:  binary.BigEndian.Uint64(payload[2:10]),
		})
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}

	if len(groups) == 0 {
		return 0, nil
	}

	batch := r.engine.db.NewBatch()
	defer batch.Close()

	for _, g := range groups {
		// Range start: [data_prefix][series_id][dim_hash][ts=0]
		start := make([]byte, 1+2+8+8)
		start[0] = dataPrefix
		binary.BigEndian.PutUint16(start[1:3], g.seriesID)
		binary.BigEndian.PutUint64(start[3:11], g.dimHash)
		// start[11:19] = 0 (zero timestamp)

		// Range end: [data_prefix][series_id][dim_hash][ts=cutoff+1]
		// C1: Pebble DeleteRange is [start, end) exclusive. Adding 1 makes
		// the range inclusive of keys at exactly cutoffNs.
		end := make([]byte, 1+2+8+8)
		end[0] = dataPrefix
		binary.BigEndian.PutUint16(end[1:3], g.seriesID)
		binary.BigEndian.PutUint64(end[3:11], g.dimHash)
		binary.BigEndian.PutUint64(end[11:19], cutoffNs+1)

		if err := batch.DeleteRange(start, end, nil); err != nil {
			return 0, err
		}
	}

	// C2: use pebble.Sync so tombstones survive crashes (prevents data resurrection)
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, err
	}

	return len(groups), nil
}
