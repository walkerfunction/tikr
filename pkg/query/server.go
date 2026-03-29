package query

import (
	"context"
	"fmt"

	"github.com/tikr-dev/tikr/pkg/core"
	"github.com/tikr-dev/tikr/pkg/ingest"
	pb "github.com/tikr-dev/tikr/pkg/pb"
	"github.com/tikr-dev/tikr/pkg/storage"
)

// Server implements the gRPC Tikr query service.
type Server struct {
	pb.UnimplementedTikrServer
	reader   *storage.Reader
	pipeline *ingest.Pipeline
	specs    []*core.SeriesSpec
}

// NewServer creates a new query gRPC server.
func NewServer(reader *storage.Reader, pipeline *ingest.Pipeline, specs []*core.SeriesSpec) *Server {
	return &Server{
		reader:   reader,
		pipeline: pipeline,
		specs:    specs,
	}
}

// QueryTicks returns raw ticks for a series+dimension in a time range.
func (s *Server) QueryTicks(req *pb.TickQuery, stream pb.Tikr_QueryTicksServer) error {
	sp, ok := s.pipeline.GetSeriesPipeline(req.Series)
	if !ok {
		return fmt.Errorf("unknown series: %s", req.Series)
	}

	dimHash := core.DimensionHash(req.Dimensions)
	ticks, err := s.reader.ReadTicks(sp.SeriesID, dimHash, req.StartNs, req.EndNs)
	if err != nil {
		return fmt.Errorf("reading ticks: %w", err)
	}

	for i, tick := range ticks {
		if req.Limit > 0 && uint32(i) >= req.Limit {
			break
		}
		if err := stream.Send(&pb.TickData{
			TimestampNs: tick.TimestampNs,
			Dimensions:  tick.Dimensions,
			Fields:      tick.Fields,
			Sequence:    tick.Sequence,
		}); err != nil {
			return err
		}
	}

	return nil
}

// QueryBars returns rolled-up bars for a series+dimension in a time range.
func (s *Server) QueryBars(req *pb.BarQuery, stream pb.Tikr_QueryBarsServer) error {
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

// GetInfo returns server information.
func (s *Server) GetInfo(_ context.Context, _ *pb.Empty) (*pb.ServerInfo, error) {
	seriesList, _ := s.ListSeries(context.Background(), nil)
	return &pb.ServerInfo{
		Version: "0.1.0",
		Series:  seriesList.Series,
	}, nil
}
