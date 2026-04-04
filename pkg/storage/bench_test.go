// Storage benchmarks — Pebble vs RocksDB (with -tags rocksdb).
//
// Run:
//   go test ./pkg/storage/ -run=^$ -bench=. -benchtime=1x    (Pebble only)
//   go test ./pkg/storage/ -run=^$ -bench=. -benchtime=1x -tags rocksdb  (both)
//
// Each sub-benchmark gets a fresh engine + temp dir. Cleanup is automatic.
//
// Benchmarks:
//   BenchmarkWriteTicks  — 1M ticks, varies batch size (1000/5000) and dims (10/100)
//   BenchmarkWriteBars   — 10K rolled-up bars across dims
//   BenchmarkReadTicks   — range scans with limit variations on 1M pre-populated ticks
//   BenchmarkReadBars    — full dimension scan on 10K pre-populated bars
//   BenchmarkReaper      — tombstone cycle on 1M expired ticks
//
// Reported metrics:
//   ticks/sec, bars/sec  — write throughput
//   ms/op                — wall clock time for the full operation
//   disk_MB              — on-disk size after writes (SSTs + WAL + manifest)
//   compression_ratio    — raw logical bytes / actual disk bytes (>1 = smaller on disk)
//   rollup_bars          — how many 1s bars the ticks would produce at rollup
//
// Data reduction math (1s rollup granularity):
//
//   rollup_bars = dims × span_seconds
//
//    10 dims × 100s =  1,000 bars from 1M ticks  → 1000:1 reduction
//   100 dims × 100s = 10,000 bars from 1M ticks  →  100:1 reduction
//
// This is the core value of edge rollup: in a typical HFT setup with ~10
// symbols, 1M raw ticks compress to just 1,000 bars shipped to Kafka/cloud.
//
// Sample results (i9-9880H, Docker, Ubuntu 24.04):
//
//   WriteTicks (1M ticks, 100s span)
//
//   Backend   dims  batch   ticks/sec   ms/op   disk_MB   compression   rollup_bars
//   ────────────────────────────────────────────────────────────────────────────────
//   pebble     10   1000      426K      2349     50.7      1.94x          1,000
//   pebble     10   5000      540K      1850     51.7      1.90x          1,000
//   pebble    100   1000      357K      2803     51.3      1.91x         10,000
//   pebble    100   5000      590K      1696     52.2      1.88x         10,000
//   rocksdb    10   1000      210K      4769     43.5      2.26x          1,000
//   rocksdb    10   5000      249K      4018     43.3      2.27x          1,000
//   rocksdb   100   1000      183K      5459     44.2      2.22x         10,000
//   rocksdb   100   5000      243K      4112     44.0      2.23x         10,000
//
//   WriteBars (10K bars)
//
//   Backend   dims   bars/sec   ms/op   disk_MB   compression
//   ──────────────────────────────────────────────────────────
//   pebble     10      88K      114      2.2       0.74x
//   pebble    100      53K      189      2.2       0.74x
//   rocksdb    10      84K      119      2.2       0.73x
//   rocksdb   100      68K      147      2.2       0.73x
//
//   ReadTicks (1M pre-populated, single dimension scan)
//
//   Backend   dims   scope        ms/op
//   ─────────────────────────────────────
//   pebble     10    all           214
//   pebble     10    limit=1K      3.2
//   pebble     10    limit=10K     22
//   pebble    100    all           23
//   pebble    100    limit=1K      2.3
//   pebble    100    limit=10K     22
//   rocksdb    10    all           320
//   rocksdb    10    limit=1K      3.3
//   rocksdb    10    limit=10K     34
//   rocksdb   100    all           34
//   rocksdb   100    limit=1K      3.2
//   rocksdb   100    limit=10K     36
//
//   ReadBars (10K pre-populated)
//
//   Backend   dims   ms/op
//   ────────────────────────
//   pebble     10    14.6
//   pebble    100     1.7
//   rocksdb    10     5.7
//   rocksdb   100     1.0
//
//   Reaper (1M expired ticks)
//
//   Backend   dims   ms/op   groups
//   ─────────────────────────────────
//   pebble     10     1.9      10
//   pebble    100     2.4     100
//   rocksdb    10     3.0      10
//   rocksdb   100     3.2     100
//
// Key takeaways:
//   - Pebble is ~2x faster than RocksDB on writes (no cgo overhead)
//   - RocksDB compresses ~15% better (2.25x vs 1.9x) due to Snappy+ZSTD tuning
//   - Batch=5000 is ~30-40% faster than batch=1000 on both backends
//   - Read latency is comparable; RocksDB slightly slower on full scans
//   - RocksDB bar reads are faster (native column families vs prefix scan)
//   - Reaper tombstones 1M expired ticks in <4ms on both backends
//   - 10 dims × 100s = 1,000 bars from 1M ticks (1000:1 data reduction)
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/walkerfunction/tikr/pkg/core"
)

// dirSize returns the total size in bytes of all files under a directory.
func dirSize(path string) int64 {
	var size int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		size += info.Size()
		return nil
	})
	return size
}

// reportDiskMetrics reports disk usage and compression ratio for a benchmark.
// rawBytes is the estimated uncompressed logical size (key + value bytes).
func reportDiskMetrics(b *testing.B, dataDir string, rawBytes int64) {
	b.Helper()
	diskBytes := dirSize(dataDir)
	b.ReportMetric(float64(diskBytes)/(1024*1024), "disk_MB")
	if diskBytes > 0 {
		b.ReportMetric(float64(rawBytes)/float64(diskBytes), "compression_ratio")
	}
}

// benchBackend describes a storage backend available for benchmarking.
type benchBackend struct {
	name   string
	config EngineConfig // base config; DataDir is overwritten per run
}

// benchBackendRegistry is populated by init() in backend-specific files.
// Pebble is always registered here; RocksDB is added in bench_rocksdb_test.go
// (compiled only with -tags rocksdb).
var benchBackendRegistry []benchBackend

func init() {
	benchBackendRegistry = append(benchBackendRegistry, benchBackend{
		name:   "pebble",
		config: EngineConfig{TicksTTL: 6 * time.Hour},
	})
}

// benchEngine holds an engine and its data directory for disk metrics.
type benchEngine struct {
	*Engine
	dataDir string
}

// newBenchEngine creates a fresh engine for a benchmark. Caller must Close().
func newBenchEngine(b *testing.B, backend benchBackend) benchEngine {
	b.Helper()
	cfg := backend.config
	cfg.DataDir = b.TempDir()
	engine, err := NewEngine(cfg)
	if err != nil {
		b.Fatalf("%s engine: %v", backend.name, err)
	}
	return benchEngine{Engine: engine, dataDir: cfg.DataDir}
}

// generateTicks creates n ticks spread across dims dimension groups over spanSec seconds.
func generateTicks(n int, dims int, spanSec int) []core.Tick {
	ticks := make([]core.Tick, n)
	baseTs := uint64(time.Now().Add(-time.Duration(spanSec) * time.Second).UnixNano())
	nsPerTick := uint64(spanSec) * 1_000_000_000 / uint64(n)

	for i := range ticks {
		dimIdx := i % dims
		ticks[i] = core.Tick{
			TimestampNs: baseTs + uint64(i)*nsPerTick,
			Dimensions:  map[string]string{"symbol": fmt.Sprintf("SYM%04d", dimIdx)},
			Fields:      map[string]int64{"price": int64(10000 + i%1000), "qty": int64(100 + i%50)},
			Sequence:    uint32(i),
		}
	}
	return ticks
}

// generateBars creates n bars spread across dims dimension groups.
func generateBars(n int, dims int) []*core.Bar {
	bars := make([]*core.Bar, n)
	baseTs := uint64(time.Now().Add(-time.Duration(n) * time.Second).UnixNano())

	for i := range bars {
		dimIdx := i % dims
		bars[i] = &core.Bar{
			Series:     "bench",
			BucketTs:   baseTs + uint64(i)*1_000_000_000,
			Dimensions: map[string]string{"symbol": fmt.Sprintf("SYM%04d", dimIdx)},
			Metrics:    map[string]int64{"open": 100, "high": 110, "low": 90, "close": 105, "volume": 5000},
			TickCount:  1000,
		}
	}
	return bars
}

func BenchmarkWriteTicks(b *testing.B) {
	const totalTicks = 1_000_000
	const spanSec = 100

	for _, backend := range benchBackendRegistry {
		for _, dims := range []int{10, 100} {
			for _, batchSize := range []int{1000, 5000} {
				name := fmt.Sprintf("%s/dims=%d/batch=%d", backend.name, dims, batchSize)
				b.Run(name, func(b *testing.B) {
					ticks := generateTicks(totalTicks, dims, spanSec)

					// Estimate raw logical bytes: 1B prefix + 22B key + ~80B JSON value per tick
					const rawBytesPerTick = 103
					rawBytes := int64(totalTicks) * rawBytesPerTick

					// At 1s rollup granularity: dims × spanSec bars produced
					rollupBars := dims * spanSec
					b.ReportMetric(float64(rollupBars), "rollup_bars")

					for i := 0; i < b.N; i++ {
						be := newBenchEngine(b, backend)
						writer := NewWriter(be.Engine)

						start := time.Now()

						for off := 0; off < len(ticks); off += batchSize {
							end := off + batchSize
							if end > len(ticks) {
								end = len(ticks)
							}
							if err := writer.WriteTicks(1, ticks[off:end]); err != nil {
								b.Fatalf("write ticks: %v", err)
							}
						}

						elapsed := time.Since(start)
						b.ReportMetric(float64(totalTicks)/elapsed.Seconds(), "ticks/sec")
						b.ReportMetric(elapsed.Seconds()*1000, "ms/op")

						b.StopTimer()
						be.Close()
						reportDiskMetrics(b, be.dataDir, rawBytes)
					}
				})
			}
		}
	}
}

func BenchmarkWriteBars(b *testing.B) {
	const totalBars = 10_000
	const batchSize = 100

	for _, backend := range benchBackendRegistry {
		for _, dims := range []int{10, 100} {
			name := fmt.Sprintf("%s/dims=%d", backend.name, dims)
			b.Run(name, func(b *testing.B) {
				bars := generateBars(totalBars, dims)

				// Estimate raw logical bytes: 1B prefix + 18B key + ~150B JSON value per bar
				const rawBytesPerBar = 169
				rawBytes := int64(totalBars) * rawBytesPerBar

				for i := 0; i < b.N; i++ {
					be := newBenchEngine(b, backend)
					writer := NewWriter(be.Engine)

					start := time.Now()

					for off := 0; off < len(bars); off += batchSize {
						end := off + batchSize
						if end > len(bars) {
							end = len(bars)
						}
						if err := writer.WriteBars(1, bars[off:end]); err != nil {
							b.Fatalf("write bars: %v", err)
						}
					}

					elapsed := time.Since(start)
					b.ReportMetric(float64(totalBars)/elapsed.Seconds(), "bars/sec")
					b.ReportMetric(elapsed.Seconds()*1000, "ms/op")

					b.StopTimer()
					be.Close()
					reportDiskMetrics(b, be.dataDir, rawBytes)
				}
			})
		}
	}
}

func BenchmarkReadTicks(b *testing.B) {
	const totalTicks = 1_000_000
	const spanSec = 100

	for _, backend := range benchBackendRegistry {
		for _, dims := range []int{10, 100} {
			for _, limit := range []uint32{0, 1000, 10000} {
				limitStr := "all"
				if limit > 0 {
					limitStr = fmt.Sprintf("limit=%d", limit)
				}
				name := fmt.Sprintf("%s/dims=%d/%s", backend.name, dims, limitStr)
				b.Run(name, func(b *testing.B) {
					ticks := generateTicks(totalTicks, dims, spanSec)
					be := newBenchEngine(b, backend)
					b.Cleanup(func() { be.Close() })
					writer := NewWriter(be.Engine)
					reader := NewReader(be.Engine)

					// Pre-populate
					for off := 0; off < len(ticks); off += 5000 {
						end := off + 5000
						if end > len(ticks) {
							end = len(ticks)
						}
						if err := writer.WriteTicks(1, ticks[off:end]); err != nil {
							b.Fatalf("populate: %v", err)
						}
					}

					// Pick first dimension for scan
					dimHash := core.DimensionHash(map[string]string{"symbol": "SYM0000"})

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						got, err := reader.ReadTicks(1, dimHash, 0, ^uint64(0), limit)
						if err != nil {
							b.Fatalf("read: %v", err)
						}
						if len(got) == 0 {
							b.Fatal("no ticks returned")
						}
					}
				})
			}
		}
	}
}

func BenchmarkReadBars(b *testing.B) {
	const totalBars = 10_000

	for _, backend := range benchBackendRegistry {
		for _, dims := range []int{10, 100} {
			name := fmt.Sprintf("%s/dims=%d", backend.name, dims)
			b.Run(name, func(b *testing.B) {
				bars := generateBars(totalBars, dims)
				be := newBenchEngine(b, backend)
				b.Cleanup(func() { be.Close() })
				writer := NewWriter(be.Engine)
				reader := NewReader(be.Engine)

				// Pre-populate
				for off := 0; off < len(bars); off += 100 {
					end := off + 100
					if end > len(bars) {
						end = len(bars)
					}
					if err := writer.WriteBars(1, bars[off:end]); err != nil {
						b.Fatalf("populate: %v", err)
					}
				}

				dimHash := core.DimensionHash(map[string]string{"symbol": "SYM0000"})

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					got, err := reader.ReadBars(1, dimHash, 0, ^uint64(0))
					if err != nil {
						b.Fatalf("read: %v", err)
					}
					if len(got) == 0 {
						b.Fatal("no bars returned")
					}
				}
			})
		}
	}
}

func BenchmarkReaper(b *testing.B) {
	const totalTicks = 1_000_000
	const spanSec = 100

	for _, backend := range benchBackendRegistry {
		for _, dims := range []int{10, 100} {
			name := fmt.Sprintf("%s/dims=%d", backend.name, dims)
			b.Run(name, func(b *testing.B) {
				// Generate all-expired ticks (2h old, TTL=1h)
				ticks := make([]core.Tick, totalTicks)
				baseTs := uint64(time.Now().Add(-2 * time.Hour).UnixNano())
				nsPerTick := uint64(spanSec) * 1_000_000_000 / uint64(totalTicks)

				for i := range ticks {
					dimIdx := i % dims
					ticks[i] = core.Tick{
						TimestampNs: baseTs + uint64(i)*nsPerTick,
						Dimensions:  map[string]string{"symbol": fmt.Sprintf("SYM%04d", dimIdx)},
						Fields:      map[string]int64{"price": int64(10000 + i%1000)},
						Sequence:    uint32(i),
					}
				}

				for i := 0; i < b.N; i++ {
					be := newBenchEngine(b, backend)
					writer := NewWriter(be.Engine)

					// Pre-populate
					for off := 0; off < len(ticks); off += 5000 {
						end := off + 5000
						if end > len(ticks) {
							end = len(ticks)
						}
						if err := writer.WriteTicks(1, ticks[off:end]); err != nil {
							b.Fatalf("populate: %v", err)
						}
					}

					reaper := NewReaper(be.Engine, TTLConfig{TicksTTL: 1 * time.Hour}, time.Hour)

					b.ResetTimer()
					reaper.ReapOnce()
					b.StopTimer()

					be.Close()
				}

				b.ReportMetric(float64(dims), "groups/op")
			})
		}
	}
}
