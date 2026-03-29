package ingest

import (
	"io"
	"log"

	"github.com/tikr-dev/tikr/pkg/core"
	pb "github.com/tikr-dev/tikr/pkg/pb"
)

// Server implements the gRPC Tikr ingest service.
type Server struct {
	pb.UnimplementedTikrServer
	pipeline *Pipeline
}

// NewServer creates a new ingest gRPC server.
func NewServer(pipeline *Pipeline) *Server {
	return &Server{pipeline: pipeline}
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
		}
	}
}
