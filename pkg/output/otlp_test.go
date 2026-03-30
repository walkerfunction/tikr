package output

import (
	"testing"

	"github.com/walkerfunction/tikr/pkg/core"
	"google.golang.org/protobuf/proto"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestBarToOTLP_RoundTrip(t *testing.T) {
	bar := &core.Bar{
		Series:         "market_ticks",
		BucketTs:       1_000_000_000,
		Dimensions:     map[string]string{"symbol": "AAPL"},
		Metrics:        map[string]int64{"open": 17520, "high": 17550, "low": 17480, "close": 17510, "volume": 450},
		FirstTimestamp: 1_000_000_001,
		LastTimestamp:  1_000_999_999,
		TickCount:      5,
	}

	data, err := BarToOTLP(bar)
	if err != nil {
		t.Fatalf("BarToOTLP: %v", err)
	}

	// Decode back
	var rm metricspb.ResourceMetrics
	if err := proto.Unmarshal(data, &rm); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Verify resource attributes
	var serviceName, series string
	for _, attr := range rm.Resource.Attributes {
		switch attr.Key {
		case "service.name":
			serviceName = attr.Value.GetStringValue()
		case "tikr.series":
			series = attr.Value.GetStringValue()
		}
	}
	if serviceName != "tikr" {
		t.Errorf("service.name: got %q, want %q", serviceName, "tikr")
	}
	if series != "market_ticks" {
		t.Errorf("tikr.series: got %q, want %q", series, "market_ticks")
	}

	// Verify metrics
	if len(rm.ScopeMetrics) != 1 {
		t.Fatalf("expected 1 ScopeMetrics, got %d", len(rm.ScopeMetrics))
	}

	sm := rm.ScopeMetrics[0]
	if sm.Scope.Name != "tikr" {
		t.Errorf("scope name: got %q, want %q", sm.Scope.Name, "tikr")
	}

	// 5 bar metrics + tick_count = 6 OTLP metrics
	if len(sm.Metrics) != 6 {
		t.Fatalf("expected 6 metrics, got %d", len(sm.Metrics))
	}

	// Build map of metric name → value for easy checking
	got := make(map[string]int64)
	for _, m := range sm.Metrics {
		gauge := m.GetGauge()
		if gauge == nil {
			t.Fatalf("metric %s: expected Gauge, got nil", m.Name)
		}
		if len(gauge.DataPoints) != 1 {
			t.Fatalf("metric %s: expected 1 data point, got %d", m.Name, len(gauge.DataPoints))
		}
		dp := gauge.DataPoints[0]
		got[m.Name] = dp.GetAsInt()

		// Verify timestamp
		if dp.TimeUnixNano != bar.BucketTs {
			t.Errorf("metric %s: TimeUnixNano got %d, want %d", m.Name, dp.TimeUnixNano, bar.BucketTs)
		}

		// Verify dimension attribute
		if len(dp.Attributes) != 1 || dp.Attributes[0].Key != "symbol" || dp.Attributes[0].Value.GetStringValue() != "AAPL" {
			t.Errorf("metric %s: unexpected attributes %v", m.Name, dp.Attributes)
		}
	}

	// Verify values
	want := map[string]int64{
		"market_ticks.open":       17520,
		"market_ticks.high":       17550,
		"market_ticks.low":        17480,
		"market_ticks.close":      17510,
		"market_ticks.volume":     450,
		"market_ticks.tick_count": 5,
	}
	for name, wantVal := range want {
		if got[name] != wantVal {
			t.Errorf("%s: got %d, want %d", name, got[name], wantVal)
		} else {
			t.Logf("%s: %d", name, got[name])
		}
	}
}
