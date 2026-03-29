package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// AggregationType defines the supported aggregation functions.
type AggregationType string

const (
	AggFirst AggregationType = "first"
	AggLast  AggregationType = "last"
	AggMin   AggregationType = "min"
	AggMax   AggregationType = "max"
	AggSum   AggregationType = "sum"
	AggCount AggregationType = "count"
)

// FieldType defines supported metric value types.
type FieldType string

const (
	FieldInt64  FieldType = "int64"
	FieldUint64 FieldType = "uint64"
)

// DimensionSpec defines a dimension (GROUP BY) column.
type DimensionSpec struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // "string" for now
}

// MetricSpec defines a metric column with its aggregation function.
type MetricSpec struct {
	Name        string          `yaml:"name"`
	Field       string          `yaml:"field"` // source field name in tick; empty for count
	Type        FieldType       `yaml:"type"`
	Aggregation AggregationType `yaml:"aggregation"`
}

// TimestampSpec defines the timestamp field.
type TimestampSpec struct {
	Field     string `yaml:"field"`
	Precision string `yaml:"precision"` // "nanosecond", "microsecond", "millisecond"
}

// GranularitySpec defines rollup time bucketing.
type GranularitySpec struct {
	Rollup string `yaml:"rollup"` // "1s", "5s", "1m", etc.
}

// KafkaOutputSpec defines Kafka output configuration for a series.
type KafkaOutputSpec struct {
	Topic     string `yaml:"topic"`
	Encoding  string `yaml:"encoding"`   // "proto" | "json"
	OnFailure string `yaml:"on_failure"` // "drop"
}

// OutputSpec defines where rolled-up bars are pushed.
type OutputSpec struct {
	Kafka KafkaOutputSpec `yaml:"kafka"`
}

// SeriesSpec is the top-level spec parsed from a YAML file.
type SeriesSpec struct {
	Series      string          `yaml:"series"`
	Timestamp   TimestampSpec   `yaml:"timestamp"`
	Dimensions  []DimensionSpec `yaml:"dimensions"`
	Metrics     []MetricSpec    `yaml:"metrics"`
	Granularity GranularitySpec `yaml:"granularity"`
	Output      OutputSpec      `yaml:"output"`
}

// RollupDuration returns the rollup interval as a time.Duration.
func (s *SeriesSpec) RollupDuration() (time.Duration, error) {
	r := strings.TrimSpace(s.Granularity.Rollup)
	if r == "" {
		return 0, fmt.Errorf("empty rollup granularity")
	}
	return time.ParseDuration(r)
}

// Validate checks the spec for errors.
func (s *SeriesSpec) Validate() error {
	if s.Series == "" {
		return fmt.Errorf("series name is required")
	}
	if s.Timestamp.Field == "" {
		return fmt.Errorf("timestamp field is required")
	}
	if len(s.Metrics) == 0 {
		return fmt.Errorf("at least one metric is required")
	}
	if _, err := s.RollupDuration(); err != nil {
		return fmt.Errorf("invalid rollup granularity %q: %w", s.Granularity.Rollup, err)
	}

	for i, m := range s.Metrics {
		switch m.Aggregation {
		case AggFirst, AggLast, AggMin, AggMax, AggSum, AggCount:
		default:
			return fmt.Errorf("metric[%d] %q: unsupported aggregation %q", i, m.Name, m.Aggregation)
		}
		if m.Aggregation != AggCount && m.Field == "" {
			return fmt.Errorf("metric[%d] %q: field is required for aggregation %q", i, m.Name, m.Aggregation)
		}
		switch m.Type {
		case FieldInt64, FieldUint64:
		default:
			return fmt.Errorf("metric[%d] %q: unsupported type %q", i, m.Name, m.Type)
		}
	}

	for i, d := range s.Dimensions {
		if d.Name == "" {
			return fmt.Errorf("dimension[%d]: name is required", i)
		}
	}

	return nil
}

// LoadSpec reads and parses a single YAML spec file.
func LoadSpec(path string) (*SeriesSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading spec %s: %w", path, err)
	}

	var spec SeriesSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parsing spec %s: %w", path, err)
	}

	if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("validating spec %s: %w", path, err)
	}

	return &spec, nil
}

// LoadSpecs reads all YAML spec files from a directory.
func LoadSpecs(dir string) ([]*SeriesSpec, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading specs directory %s: %w", dir, err)
	}

	var specs []*SeriesSpec
	seen := make(map[string]bool)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		spec, err := LoadSpec(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}

		if seen[spec.Series] {
			return nil, fmt.Errorf("duplicate series name %q", spec.Series)
		}
		seen[spec.Series] = true
		specs = append(specs, spec)
	}

	if len(specs) == 0 {
		return nil, fmt.Errorf("no spec files found in %s", dir)
	}

	return specs, nil
}
