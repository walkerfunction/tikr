// Package storage provides a Pebble-backed storage engine for ticks and
// rolled-up bars. It handles batch writes, range scans, and automatic data
// expiration via FIFO compaction with configurable TTL and size limits.
package storage
