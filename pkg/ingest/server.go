package ingest

import (
	"io"
	"log"

	"github.com/walkerfunction/tikr/pkg/core"
	pb "github.com/walkerfunction/tikr/pkg/pb"
	"github.com/walkerfunction/tikr/pkg/telemetry"
)

// Server implements the gRPC Tikr ingest service.
type Server struct {
	pb.UnimplementedTikrServer
	pipeline *Pipeline
	metrics  *telemetry.Metrics // nil-safe: all methods check before recording
}

// NewServer creates a new ingest gRPC server.
// Pass nil for metrics if OTel instrumentation is not needed.
func NewServer(pipeline *Pipeline, m *telemetry.Metrics) *Server {
	return &Server{pipeline: pipeline, metrics: m}
}

// IngestTicks handles streaming tick ingestion.
func (s *Server) IngestTicks(stream pb.Tikr_IngestTicksServer) error {
	var totalTicks uint64

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.IngestResponse{
				TicksReceived: totalTicks,
			})
		}
		if err != nil {
			return err
		}

		seriesName := req.Series

		var batchIngested int64
		for _, t := range req.Ticks {
			tick := core.Tick{
				TimestampNs: t.TimestampNs,
				Series:      seriesName,
				Dimensions:  t.Dimensions,
				Fields:      t.Fields,
				Sequence:    t.Sequence,
			}

			if err := s.pipeline.Ingest(tick); err != nil {
				log.Printf("ingest error for series %s: %v", seriesName, err)
				continue
			}
			totalTicks++
			batchIngested++
		}

		// Note: TicksTotal is counted in Pipeline.Ingest (per-tick, regardless of caller).
		// Only record batch size here since it is server-specific.
		if s.metrics != nil {
			s.metrics.BatchSize.Record(stream.Context(), float64(batchIngested))
		}
	}
}
