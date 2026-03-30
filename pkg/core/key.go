package core

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// Key encoding for RocksDB. All multi-byte integers are big-endian
// so that bytewise comparison yields chronological ordering.
//
// Tick key:   [series_id:2][dim_hash:8][ts_ns:8][seq:4] = 22 bytes
// Rollup key: [series_id:2][dim_hash:8][bucket_ts:8]    = 18 bytes
// Meta key:   [prefix:1][payload:variable]

// EncodeTickKey creates a RocksDB key for a raw tick.
func EncodeTickKey(seriesID uint16, dimHash uint64, tsNs uint64, seq uint32) []byte {
	key := make([]byte, 22)
	binary.BigEndian.PutUint16(key[0:2], seriesID)
	binary.BigEndian.PutUint64(key[2:10], dimHash)
	binary.BigEndian.PutUint64(key[10:18], tsNs)
	binary.BigEndian.PutUint32(key[18:22], seq)
	return key
}

// DecodeTickKey extracts components from a tick key.
func DecodeTickKey(key []byte) (seriesID uint16, dimHash uint64, tsNs uint64, seq uint32, err error) {
	if len(key) != 22 {
		return 0, 0, 0, 0, fmt.Errorf("invalid tick key length: %d", len(key))
	}
	seriesID = binary.BigEndian.Uint16(key[0:2])
	dimHash = binary.BigEndian.Uint64(key[2:10])
	tsNs = binary.BigEndian.Uint64(key[10:18])
	seq = binary.BigEndian.Uint32(key[18:22])
	return
}

// EncodeRollupKey creates a RocksDB key for a rolled-up bar.
func EncodeRollupKey(seriesID uint16, dimHash uint64, bucketTs uint64) []byte {
	key := make([]byte, 18)
	binary.BigEndian.PutUint16(key[0:2], seriesID)
	binary.BigEndian.PutUint64(key[2:10], dimHash)
	binary.BigEndian.PutUint64(key[10:18], bucketTs)
	return key
}

// DecodeRollupKey extracts components from a rollup key.
func DecodeRollupKey(key []byte) (seriesID uint16, dimHash uint64, bucketTs uint64, err error) {
	if len(key) != 18 {
		return 0, 0, 0, fmt.Errorf("invalid rollup key length: %d", len(key))
	}
	seriesID = binary.BigEndian.Uint16(key[0:2])
	dimHash = binary.BigEndian.Uint64(key[2:10])
	bucketTs = binary.BigEndian.Uint64(key[10:18])
	return
}

// DimensionHash computes a stable FNV-1a hash of sorted dimension key-value pairs.
func DimensionHash(dims map[string]string) uint64 {
	// Sort keys for deterministic hashing
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := fnv.New64a()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte("="))
		h.Write([]byte(dims[k]))
		h.Write([]byte("\x00"))
	}
	return h.Sum64()
}

// TickKeyRange returns start/end keys for range scanning ticks
// of a specific series+dimension in a time range.
// Keys are 18 bytes (series_id + dim_hash + timestamp) — intentionally shorter than
// the full 22-byte tick key. Pebble uses these as prefix bounds, so any 22-byte tick
// key sharing this 18-byte prefix falls within the scan range.
func TickKeyRange(seriesID uint16, dimHash uint64, startNs, endNs uint64) ([]byte, []byte) {
	start := make([]byte, 18)
	binary.BigEndian.PutUint16(start[0:2], seriesID)
	binary.BigEndian.PutUint64(start[2:10], dimHash)
	binary.BigEndian.PutUint64(start[10:18], startNs)

	end := make([]byte, 18)
	binary.BigEndian.PutUint16(end[0:2], seriesID)
	binary.BigEndian.PutUint64(end[2:10], dimHash)
	binary.BigEndian.PutUint64(end[10:18], endNs)

	return start, end
}

// RollupKeyRange returns start/end keys for range scanning rollups.
func RollupKeyRange(seriesID uint16, dimHash uint64, startTs, endTs uint64) ([]byte, []byte) {
	return TickKeyRange(seriesID, dimHash, startTs, endTs) // same prefix format
}

// DimensionString returns a canonical string representation of dimensions for display.
func DimensionString(dims map[string]string) string {
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + dims[k]
	}
	return strings.Join(parts, ",")
}
