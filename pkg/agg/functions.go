package agg

import "math"

// AggFunc is the interface for all aggregation functions.
// The DB applies these mechanically per the YAML spec — it has no knowledge
// of what the values represent (prices, bytes, latency, etc.).
type AggFunc interface {
	// Init sets the initial value from the first tick in a bucket.
	Init(value int64, timestampNs uint64)
	// Update incorporates a new tick value.
	Update(value int64, timestampNs uint64)
	// Result returns the final aggregated value.
	Result() int64
	// Reset clears the aggregator for reuse.
	Reset()
	// Clone returns a deep copy.
	Clone() AggFunc
}

// FirstAgg keeps the first value by timestamp.
type FirstAgg struct {
	value int64
	ts    uint64
	set   bool
}

func (a *FirstAgg) Init(value int64, ts uint64)   { a.value = value; a.ts = ts; a.set = true }
func (a *FirstAgg) Update(value int64, ts uint64) {
	if !a.set || ts < a.ts {
		a.value = value
		a.ts = ts
		a.set = true
	}
}
func (a *FirstAgg) Result() int64 { return a.value }
func (a *FirstAgg) Reset()        { a.value = 0; a.ts = 0; a.set = false }
func (a *FirstAgg) Clone() AggFunc {
	c := *a
	return &c
}

// LastAgg keeps the last value by timestamp.
type LastAgg struct {
	value int64
	ts    uint64
	set   bool
}

func (a *LastAgg) Init(value int64, ts uint64)   { a.value = value; a.ts = ts; a.set = true }
func (a *LastAgg) Update(value int64, ts uint64) {
	if !a.set || ts > a.ts {
		a.value = value
		a.ts = ts
		a.set = true
	}
}
func (a *LastAgg) Result() int64 { return a.value }
func (a *LastAgg) Reset()        { a.value = 0; a.ts = 0; a.set = false }
func (a *LastAgg) Clone() AggFunc {
	c := *a
	return &c
}

// MinAgg keeps the minimum value.
type MinAgg struct {
	value int64
	set   bool
}

func (a *MinAgg) Init(value int64, _ uint64) { a.value = value; a.set = true }
func (a *MinAgg) Update(value int64, _ uint64) {
	if !a.set || value < a.value {
		a.value = value
		a.set = true
	}
}
func (a *MinAgg) Result() int64 { return a.value }
func (a *MinAgg) Reset()        { a.value = math.MaxInt64; a.set = false }
func (a *MinAgg) Clone() AggFunc {
	c := *a
	return &c
}

// MaxAgg keeps the maximum value.
type MaxAgg struct {
	value int64
	set   bool
}

func (a *MaxAgg) Init(value int64, _ uint64) { a.value = value; a.set = true }
func (a *MaxAgg) Update(value int64, _ uint64) {
	if !a.set || value > a.value {
		a.value = value
		a.set = true
	}
}
func (a *MaxAgg) Result() int64 { return a.value }
func (a *MaxAgg) Reset()        { a.value = math.MinInt64; a.set = false }
func (a *MaxAgg) Clone() AggFunc {
	c := *a
	return &c
}

// SumAgg keeps a running sum.
type SumAgg struct {
	value int64
}

func (a *SumAgg) Init(value int64, _ uint64)   { a.value = value }
func (a *SumAgg) Update(value int64, _ uint64) { a.value += value }
func (a *SumAgg) Result() int64                { return a.value }
func (a *SumAgg) Reset()                       { a.value = 0 }
func (a *SumAgg) Clone() AggFunc {
	c := *a
	return &c
}

// CountAgg counts the number of ticks.
type CountAgg struct {
	count int64
}

func (a *CountAgg) Init(_ int64, _ uint64)   { a.count = 1 }
func (a *CountAgg) Update(_ int64, _ uint64) { a.count++ }
func (a *CountAgg) Result() int64            { return a.count }
func (a *CountAgg) Reset()                   { a.count = 0 }
func (a *CountAgg) Clone() AggFunc {
	c := *a
	return &c
}

// NewAggFunc creates an aggregation function by type name.
func NewAggFunc(aggType string) AggFunc {
	switch aggType {
	case "first":
		return &FirstAgg{}
	case "last":
		return &LastAgg{}
	case "min":
		return &MinAgg{}
	case "max":
		return &MaxAgg{}
	case "sum":
		return &SumAgg{}
	case "count":
		return &CountAgg{}
	default:
		return nil
	}
}
