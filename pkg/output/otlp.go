package output

import (
	"sort"

	"github.com/walkerfunction/tikr/pkg/core"
	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Version is the instrumentation scope version used in OTLP output.
// Set from main via ldflags to keep it in sync with the binary version.
var Version = "dev"

// BarToOTLP converts a rolled-up bar into OTLP protobuf bytes.
//
// Each metric in the bar (open, high, low, close, volume, etc.) becomes a
// separate Gauge metric with the bar's dimensions as attributes. This makes
// the output consumable by any OTel-compatible backend (Grafana, Datadog,
// ClickHouse, etc.) without custom deserializers.
func BarToOTLP(bar *core.Bar) ([]byte, error) {
	// attrs is shared across all NumberDataPoint entries for performance.
	// This is safe because protobuf Marshal is read-only on the slice.
	// If future code needs per-metric attribute mutation, clone the slice per data point.
	attrs := dimensionsToAttributes(bar.Dimensions)

	// Design decision: Gauge is used for all bar metrics (including volume and tick_count)
	// because each bar represents a point-in-time aggregation result for a time bucket,
	// not a running counter. For backends that need rate calculations across buckets,
	// consider switching additive metrics to Sum with AggregationTemporality_DELTA.
	//
	// One OTLP Metric per bar metric field
	var metrics []*metricspb.Metric
	for name, value := range bar.Metrics {
		metrics = append(metrics, &metricspb.Metric{
			Name: bar.Series + "." + name,
			Data: &metricspb.Metric_Gauge{
				Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{{
						Attributes:        attrs,
						StartTimeUnixNano: bar.FirstTimestamp,
						TimeUnixNano:      bar.BucketTs,
						Value:             &metricspb.NumberDataPoint_AsInt{AsInt: value},
					}},
				},
			},
		})
	}

	// Add tick_count as a metric
	metrics = append(metrics, &metricspb.Metric{
		Name: bar.Series + ".tick_count",
		Data: &metricspb.Metric_Gauge{
			Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{{
					Attributes:        attrs,
					StartTimeUnixNano: bar.FirstTimestamp,
					TimeUnixNano:      bar.BucketTs,
					Value:             &metricspb.NumberDataPoint_AsInt{AsInt: int64(bar.TickCount)},
				}},
			},
		},
	})

	// Sort metrics by name for deterministic output
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].Name < metrics[j].Name
	})

	rm := &metricspb.ResourceMetrics{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				stringKV("service.name", "tikr"),
				stringKV("tikr.series", bar.Series),
			},
		},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{
				Name:    "tikr",
				Version: Version,
			},
			Metrics: metrics,
		}},
	}

	return proto.Marshal(rm)
}

// dimensionsToAttributes converts bar dimensions to OTLP KeyValue attributes.
func dimensionsToAttributes(dims map[string]string) []*commonpb.KeyValue {
	// Sort keys for deterministic ordering
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	attrs := make([]*commonpb.KeyValue, len(keys))
	for i, k := range keys {
		attrs[i] = stringKV(k, dims[k])
	}
	return attrs
}

// stringKV creates an OTLP string KeyValue.
func stringKV(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}
