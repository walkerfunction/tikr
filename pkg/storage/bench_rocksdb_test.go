//go:build rocksdb

package storage

import "time"

func init() {
	benchBackendRegistry = append(benchBackendRegistry, benchBackend{
		name:   "rocksdb",
		config: EngineConfig{Backend: BackendRocksDB, TicksTTL: 6 * time.Hour},
	})
}
