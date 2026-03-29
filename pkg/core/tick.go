package core

// Tick represents a generic incoming data point.
// Dimensions are the GROUP BY fields (e.g., symbol, src_ip).
// Fields are the numeric values that get aggregated per the spec.
type Tick struct {
	TimestampNs uint64
	Series      string
	Dimensions  map[string]string // dimension_name → value
	Fields      map[string]int64  // field_name → value (fixed-point)
	Sequence    uint32
}
