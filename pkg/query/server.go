package query

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/walkerfunction/tikr/pkg/core"
	"github.com/walkerfunction/tikr/pkg/ingest"
	pb "github.com/walkerfunction/tikr/pkg/pb"
	"github.com/walkerfunction/tikr/pkg/storage"
	"github.com/walkerfunction/tikr/pkg/telemetry"
)

const maxQueryRangeNs = 24 * uint64(time.Hour)

// Pre-allocated attribute options to avoid per-call allocations.
var (
	attrTypeTicks = metric.WithAttributes(attribute.String("type", "ticks"))
	attrTypeBars  = metric.WithAttributes(attribute.String("type", "bars"))
)

// Server implements the gRPC Tikr query service.
type Server struct {
	pb.UnimplementedTikrServer
	reader   *storage.Reader
	pipeline *ingest.Pipeline
	specs    []*core.SeriesSpec
	metrics  *telemetry.Metrics // nil-safe
}

// NewServer creates a new query gRPC server.
func NewServer(reader *storage.Reader, pipeline *ingest.Pipeline, specs []*core.SeriesSpec) *Server {
	return &Server{
		reader:   reader,
		pipeline: pipeline,
		specs:    specs,
	}
}

// NewServerWithMetrics creates a query server with OTel instrumentation.
func NewServerWithMetrics(reader *storage.Reader, pipeline *ingest.Pipeline, specs []*core.SeriesSpec, m *telemetry.Metrics) *Server {
	return &Server{
		reader:   reader,
		pipeline: pipeline,
		specs:    specs,
		metrics:  m,
	}
}

// validateTimeRange checks that the requested time range is valid and bounded.
func validateTimeRange(startNs, endNs uint64) error {
	if endNs <= startNs {
		return fmt.Errorf("invalid time range: end must be after start")
	}
	if endNs-startNs > maxQueryRangeNs {
		return fmt.Errorf("time range too large: max 24h")
	}
	return nil
}

// QueryTicks returns raw ticks for a series+dimension in a time range.
func (s *Server) QueryTicks(req *pb.TickQuery, stream pb.Tikr_QueryTicksServer) error {
	start := time.Now()

	if err := validateTimeRange(req.StartNs, req.EndNs); err != nil {
		return err
	}

	sp, ok := s.pipeline.GetSeriesPipeline(req.Series)
	if !ok {
		return fmt.Errorf("unknown series: %s", req.Series)
	}

	dimHash := core.DimensionHash(req.Dimensions)
	ticks, err := s.reader.ReadTicks(sp.SeriesID, dimHash, req.StartNs, req.EndNs, req.Limit)
	if err != nil {
		return fmt.Errorf("reading ticks: %w", err)
	}

	for _, tick := range ticks {
		if err := stream.Send(&pb.TickData{
			TimestampNs: tick.TimestampNs,
			Dimensions:  tick.Dimensions,
			Fields:      tick.Fields,
			Sequence:    tick.Sequence,
		}); err != nil {
			return err
		}
	}

	if s.metrics != nil {
		ctx := stream.Context()
		s.metrics.QueryRequestsTotal.Add(ctx, 1, attrTypeTicks)
		s.metrics.QueryLatencyMs.Record(ctx, float64(time.Since(start).Milliseconds()))
	}

	return nil
}

// QueryBars returns rolled-up bars for a series+dimension in a time range.
func (s *Server) QueryBars(req *pb.BarQuery, stream pb.Tikr_QueryBarsServer) error {
	start := time.Now()

	if err := validateTimeRange(req.StartNs, req.EndNs); err != nil {
		return err
	}

	sp, ok := s.pipeline.GetSeriesPipeline(req.Series)
	if !ok {
		return fmt.Errorf("unknown series: %s", req.Series)
	}

	dimHash := core.DimensionHash(req.Dimensions)
	bars, err := s.reader.ReadBars(sp.SeriesID, dimHash, req.StartNs, req.EndNs)
	if err != nil {
		return fmt.Errorf("reading bars: %w", err)
	}

	for _, bar := range bars {
		if err := stream.Send(&pb.BarData{
			Series:         bar.Series,
			BucketTs:       bar.BucketTs,
			Dimensions:     bar.Dimensions,
			Metrics:        bar.Metrics,
			FirstTimestamp: bar.FirstTimestamp,
			LastTimestamp:   bar.LastTimestamp,
			TickCount:      bar.TickCount,
		}); err != nil {
			return err
		}
	}

	if s.metrics != nil {
		ctx := stream.Context()
		s.metrics.QueryRequestsTotal.Add(ctx, 1, attrTypeBars)
		s.metrics.QueryLatencyMs.Record(ctx, float64(time.Since(start).Milliseconds()))
	}

	return nil
}

// ListSeries returns all configured series.
func (s *Server) ListSeries(_ context.Context, _ *pb.Empty) (*pb.SeriesList, error) {
	var list []*pb.SeriesInfo
	for _, spec := range s.specs {
		dims := make([]string, len(spec.Dimensions))
		for i, d := range spec.Dimensions {
			dims[i] = d.Name
		}
		metrics := make([]string, len(spec.Metrics))
		for i, m := range spec.Metrics {
			metrics[i] = m.Name
		}
		list = append(list, &pb.SeriesInfo{
			Name:       spec.Series,
			Dimensions: dims,
			Metrics:    metrics,
			Rollup:     spec.Granularity.Rollup,
		})
	}
	return &pb.SeriesList{Series: list}, nil
}

// Version is set by main.init() from the ldflags-overridable main.version.
var Version = "dev"

// GetInfo returns server information.
func (s *Server) GetInfo(ctx context.Context, req *pb.Empty) (*pb.ServerInfo, error) {
	seriesList, err := s.ListSeries(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("listing series: %w", err)
	}
	return &pb.ServerInfo{
		Version: Version,
		Series:  seriesList.Series,
	}, nil
}
