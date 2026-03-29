package core

// Bar represents a rolled-up aggregation bucket.
// It is schema-agnostic — the metric names and values come from the spec.
type Bar struct {
	Series         string
	BucketTs       uint64            // bucket start timestamp (nanoseconds)
	Dimensions     map[string]string // dimension_name → value
	Metrics        map[string]int64  // metric_name → aggregated value
	FirstTimestamp uint64            // earliest tick timestamp in this bucket
	LastTimestamp   uint64            // latest tick timestamp in this bucket
	TickCount      uint64
}
