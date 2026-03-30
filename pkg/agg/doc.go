// Package agg implements spec-driven time series aggregation. It provides
// pluggable aggregation functions (first, last, min, max, sum, count) and a
// rollup engine that buckets ticks by time interval and flushes completed bars.
package agg
