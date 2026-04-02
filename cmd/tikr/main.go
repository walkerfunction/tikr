package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"github.com/walkerfunction/tikr/pkg/agg"
	"github.com/walkerfunction/tikr/pkg/config"
	"github.com/walkerfunction/tikr/pkg/core"
	"github.com/walkerfunction/tikr/pkg/ingest"
	"github.com/walkerfunction/tikr/pkg/output"
	pb "github.com/walkerfunction/tikr/pkg/pb"
	"github.com/walkerfunction/tikr/pkg/query"
	"github.com/walkerfunction/tikr/pkg/storage"
	"github.com/walkerfunction/tikr/pkg/telemetry"
)

var version = "0.1.0" // overridden by ldflags: -X main.version=...

func init() {
	query.Version = version
	output.Version = version
}

func main() {
	configPath := flag.String("config", "/etc/tikr/default.yaml", "path to config file")
	flag.Parse()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Load series specs
	specs, err := core.LoadSpecs(cfg.SpecsDir)
	if err != nil {
		log.Fatalf("specs: %v", err)
	}

	// Initialize OpenTelemetry metrics
	metrics, err := telemetry.NewMetrics(cfg.Telemetry.ServiceName)
	if err != nil {
		log.Fatalf("telemetry: %v", err)
	}
	output.ServiceName = cfg.Telemetry.ServiceName

	fmt.Printf("Tikr %s\n", version)
	for _, s := range specs {
		fmt.Printf("  series: %s (rollup: %s, metrics: %d, dimensions: %d)\n",
			s.Series, s.Granularity.Rollup, len(s.Metrics), len(s.Dimensions))
	}

	// Open storage
	ticksTTL, err := cfg.Storage.Ticks.TTLDuration()
	if err != nil {
		log.Fatalf("invalid ticks TTL: %v", err)
	}
	rollupTTL, err := cfg.Storage.Rollup.TTLDuration()
	if err != nil {
		log.Fatalf("invalid rollup TTL: %v", err)
	}
	backend := storage.Backend(cfg.Storage.Backend)
	if envBackend := os.Getenv("TIKR_STORAGE_BACKEND"); envBackend != "" {
		backend = storage.Backend(envBackend)
	}

	engine, err := storage.NewEngine(storage.EngineConfig{
		Backend:       backend,
		DataDir:       cfg.Storage.DataDir,
		TicksTTL:      ticksTTL,
		TicksMaxSize:  int64(cfg.Storage.Ticks.MaxSizeGB) * 1024 * 1024 * 1024,
		RollupTTL:     rollupTTL,
		RollupMaxSize: int64(cfg.Storage.Rollup.MaxSizeGB) * 1024 * 1024 * 1024,
	})
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	writer := storage.NewWriter(engine)
	reader := storage.NewReader(engine)

	// Create Kafka producer (if brokers configured)
	brokers := cfg.Kafka.Brokers
	if envBrokers := os.Getenv("TIKR_KAFKA_BROKERS"); envBrokers != "" {
		brokers = strings.Split(envBrokers, ",")
	}

	var barHook agg.BarHook = agg.NoopHook{}
	var kafkaProducer *output.KafkaProducer
	if len(brokers) > 0 && brokers[0] != "" {
		kp, err := output.NewKafkaProducer(brokers, specs, metrics)
		if err != nil {
			log.Fatalf("kafka producer: %v", err)
		}
		kafkaProducer = kp
		barHook = kp
		fmt.Printf("  kafka: %v\n", brokers)
	}

	// Create ingest pipeline
	pipeline, err := ingest.NewPipeline(ingest.PipelineConfig{
		Specs:      specs,
		Writer:     writer,
		Hook:       barHook,
		BatcherCfg: ingest.DefaultBatcherConfig(),
		Metrics:    metrics,
		OnBarFlushed: func(bar *core.Bar) {
			log.Printf("bar: %s %s bucket=%d ticks=%d",
				bar.Series, core.DimensionString(bar.Dimensions), bar.BucketTs, bar.TickCount)
		},
	})
	if err != nil {
		log.Fatalf("pipeline: %v", err)
	}

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.GRPCPort))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024), // 16MB max ingest message
		grpc.MaxSendMsgSize(16*1024*1024), // 16MB max query response
	)
	combined := &combinedServer{
		ingest: ingest.NewServer(pipeline, metrics),
		query:  query.NewServer(reader, pipeline, specs, metrics),
	}
	pb.RegisterTikrServer(grpcServer, combined)

	go func() {
		fmt.Printf("  gRPC :%d\n", cfg.Server.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc: %v", err)
		}
	}()

	fmt.Println("  ready")

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Printf("\n%s — shutting down\n", sig)

	grpcServer.GracefulStop()
	pipeline.Close()
	if kafkaProducer != nil {
		kafkaProducer.Close()
	}
	_ = metrics.Shutdown(context.Background())
	if err := engine.Close(); err != nil {
		log.Printf("engine close: %v", err)
	}
	fmt.Println("bye")
}

// combinedServer merges ingest and query into one gRPC service registration.
type combinedServer struct {
	pb.UnimplementedTikrServer
	ingest *ingest.Server
	query  *query.Server
}

func (c *combinedServer) IngestTicks(stream pb.Tikr_IngestTicksServer) error {
	return c.ingest.IngestTicks(stream)
}

func (c *combinedServer) QueryTicks(req *pb.TickQuery, stream pb.Tikr_QueryTicksServer) error {
	return c.query.QueryTicks(req, stream)
}

func (c *combinedServer) QueryBars(req *pb.BarQuery, stream pb.Tikr_QueryBarsServer) error {
	return c.query.QueryBars(req, stream)
}

func (c *combinedServer) ListSeries(ctx context.Context, req *pb.Empty) (*pb.SeriesList, error) {
	return c.query.ListSeries(ctx, req)
}

func (c *combinedServer) GetInfo(ctx context.Context, req *pb.Empty) (*pb.ServerInfo, error) {
	return c.query.GetInfo(ctx, req)
}
