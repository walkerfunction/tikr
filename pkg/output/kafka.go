package output

import (
	"context"
	"errors"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/walkerfunction/tikr/pkg/core"
	"github.com/walkerfunction/tikr/pkg/telemetry"
)

// KafkaProducer pushes rolled-up bars to Kafka topics.
// It implements the agg.BarHook interface.
type KafkaProducer struct {
	writers map[string]*kafka.Writer // series name -> writer
	metrics *telemetry.Metrics       // nil-safe
}

// NewKafkaProducer creates a Kafka writer per series topic.
// Each writer is configured for async, fire-and-forget delivery.
func NewKafkaProducer(brokers []string, specs []*core.SeriesSpec) (*KafkaProducer, error) {
	writers := make(map[string]*kafka.Writer, len(specs))
	for _, spec := range specs {
		topic := spec.Output.Kafka.Topic
		if topic == "" {
			continue
		}
		w := &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			Async:        true,
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 1 * time.Second,
			RequiredAcks: kafka.RequireNone,
			Completion: func(msgs []kafka.Message, err error) {
				if err != nil {
					log.Printf("kafka: async write failed for %d message(s): %v", len(msgs), err)
				}
			},
		}
		writers[spec.Series] = w
	}
	return &KafkaProducer{writers: writers}, nil
}

// NewKafkaProducerWithMetrics creates a Kafka producer with OTel instrumentation.
func NewKafkaProducerWithMetrics(brokers []string, specs []*core.SeriesSpec, m *telemetry.Metrics) (*KafkaProducer, error) {
	kp, err := NewKafkaProducer(brokers, specs)
	if err != nil {
		return nil, err
	}
	kp.metrics = m
	return kp, nil
}

// OnBarFlushed encodes a Bar as OTLP protobuf and publishes it to Kafka.
// On any error the bar is dropped and the error is logged (fire-and-forget).
func (kp *KafkaProducer) OnBarFlushed(ctx context.Context, bar *core.Bar) error {
	w, ok := kp.writers[bar.Series]
	if !ok {
		return nil // no Kafka output configured for this series
	}

	data, err := BarToOTLP(bar)
	if err != nil {
		log.Printf("kafka: OTLP marshal failed for series %s: %v", bar.Series, err)
		return nil
	}

	key := dimensionKey(bar.Dimensions)

	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
	}); err != nil {
		log.Printf("kafka: write failed for series %s: %v", bar.Series, err)
		if kp.metrics != nil {
			kp.metrics.KafkaDropsTotal.Add(ctx, 1)
		}
		return nil
	}

	if kp.metrics != nil {
		kp.metrics.KafkaWritesTotal.Add(ctx, 1)
	}

	return nil
}

// Close shuts down all Kafka writers.
func (kp *KafkaProducer) Close() error {
	var errs []error
	for name, w := range kp.writers {
		if err := w.Close(); err != nil {
			log.Printf("kafka: error closing writer for series %s: %v", name, err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// dimensionKey builds a deterministic partition key by sorting dimension
// keys and joining their values with "|".
func dimensionKey(dims map[string]string) string {
	if len(dims) == 0 {
		return ""
	}
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = dims[k]
	}
	return strings.Join(vals, "|")
}
