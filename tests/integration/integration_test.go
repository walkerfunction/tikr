package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/tikr-dev/tikr/pkg/pb"
)

// Package-level state shared across all tests via TestMain.
var (
	tikrAddr      string
	tikrContainer testcontainers.Container
)

// projectRoot returns the absolute path to the repo root.
func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// TestMain starts a single Tikr container shared by all tests,
// then tears it down after the suite completes.
func TestMain(m *testing.M) {
	ctx := context.Background()
	root := projectRoot()

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    root,
			Dockerfile: "docker/Dockerfile",
		},
		ExposedPorts: []string{"9876/tcp"},
		WaitingFor:   wait.ForLog("ready").WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start tikr container: %v\n", err)
		os.Exit(1)
	}
	tikrContainer = container

	host, err := container.Host(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get container host: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}
	port, err := container.MappedPort(ctx, "9876")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get mapped port: %v\n", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	tikrAddr = fmt.Sprintf("%s:%s", host, port.Port())
	fmt.Printf("tikr running at %s\n", tikrAddr)

	code := m.Run()

	_ = container.Terminate(ctx)
	os.Exit(code)
}

// dial creates a gRPC connection to the given address.
func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}

// TestGetInfo verifies the server starts and responds to GetInfo.
func TestGetInfo(t *testing.T) {
	ctx := context.Background()

	conn := dial(t, tikrAddr)
	defer conn.Close()
	client := pb.NewTikrClient(conn)

	info, err := client.GetInfo(ctx, &pb.Empty{})
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if info.Version == "" {
		t.Fatal("expected non-empty version")
	}

	t.Logf("version: %s, series: %d", info.Version, len(info.Series))

	if len(info.Series) < 2 {
		t.Fatalf("expected at least 2 series, got %d", len(info.Series))
	}

	var found bool
	for _, s := range info.Series {
		if s.Name == "market_ticks" {
			found = true
			if s.Rollup != "1s" {
				t.Errorf("market_ticks rollup: got %s, want 1s", s.Rollup)
			}
		}
	}
	if !found {
		t.Fatal("market_ticks series not found")
	}
}

// TestRollupCorrectness_MarketTicks verifies rollup math with deterministic data.
//
// This is the trust-building test: we ingest ticks with known values and verify
// that EVERY rolled-up bar metric matches the expected mathematical result exactly.
//
// Input: 5 ticks in one 1-second bucket, deterministic prices and quantities.
// Expected output: open=first, high=max, low=min, close=last, volume=sum, count=5.
func TestRollupCorrectness_MarketTicks(t *testing.T) {
	ctx := context.Background()

	conn := dial(t, tikrAddr)
	defer conn.Close()
	client := pb.NewTikrClient(conn)

	stream, err := client.IngestTicks(ctx)
	if err != nil {
		t.Fatalf("IngestTicks: %v", err)
	}

	// Use a unique symbol to isolate this test from others
	symbol := fmt.Sprintf("CORRECTNESS_%d", time.Now().UnixNano())

	// All ticks within one 1-second bucket — deterministic values
	baseNs := uint64(time.Now().Unix()) * 1_000_000_000
	type tickInput struct {
		price    int64
		quantity int64
	}
	inputs := []tickInput{
		{price: 17520, quantity: 100}, // ← open (first by time)
		{price: 17550, quantity: 50},  // ← high
		{price: 17480, quantity: 75},  // ← low
		{price: 17530, quantity: 200}, //
		{price: 17510, quantity: 25},  // ← close (last by time)
	}

	// Expected bar values
	wantOpen := int64(17520)  // first
	wantHigh := int64(17550)  // max
	wantLow := int64(17480)   // min
	wantClose := int64(17510) // last
	wantVolume := int64(100 + 50 + 75 + 200 + 25) // sum = 450
	wantCount := uint64(5)

	var ticks []*pb.TickData
	for i, inp := range inputs {
		ticks = append(ticks, &pb.TickData{
			TimestampNs: baseNs + uint64(i)*1_000_000, // 1ms apart, all in same 1s bucket
			Dimensions:  map[string]string{"symbol": symbol},
			Fields:      map[string]int64{"price": inp.price, "quantity": inp.quantity},
			Sequence:    uint32(i),
		})
	}

	if err := stream.Send(&pb.IngestRequest{
		Series: "market_ticks",
		Ticks:  ticks,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Send a tick in the NEXT second to force the rollup engine to flush the first bucket
	if err := stream.Send(&pb.IngestRequest{
		Series: "market_ticks",
		Ticks: []*pb.TickData{{
			TimestampNs: baseNs + 2*1_000_000_000,
			Dimensions:  map[string]string{"symbol": symbol},
			Fields:      map[string]int64{"price": 99999, "quantity": 1},
			Sequence:    0,
		}},
	}); err != nil {
		t.Fatalf("Send flush tick: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	t.Logf("ingested %d ticks", resp.TicksReceived)

	// Wait for rollup flush to propagate
	time.Sleep(3 * time.Second)

	// Query bars for the first bucket only
	barStream, err := client.QueryBars(ctx, &pb.BarQuery{
		Series:     "market_ticks",
		Dimensions: map[string]string{"symbol": symbol},
		StartNs:    baseNs - 1_000_000_000,
		EndNs:      baseNs + 1_000_000_000, // only first bucket
	})
	if err != nil {
		t.Fatalf("QueryBars: %v", err)
	}

	var bars []*pb.BarData
	for {
		bar, err := barStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv bar: %v", err)
		}
		bars = append(bars, bar)
	}

	if len(bars) != 1 {
		t.Fatalf("expected exactly 1 bar for the 1s bucket, got %d", len(bars))
	}

	bar := bars[0]
	t.Logf("bar: bucket=%d ticks=%d open=%d high=%d low=%d close=%d volume=%d",
		bar.BucketTs, bar.TickCount,
		bar.Metrics["open"], bar.Metrics["high"],
		bar.Metrics["low"], bar.Metrics["close"],
		bar.Metrics["volume"])

	// === EXACT VERIFICATION ===
	failures := 0
	check := func(name string, got, want int64) {
		if got != want {
			t.Errorf("  %s: got %d, want %d (MISMATCH)", name, got, want)
			failures++
		} else {
			t.Logf("  %s: %d ✓", name, got)
		}
	}

	check("open  (first)", bar.Metrics["open"], wantOpen)
	check("high  (max)", bar.Metrics["high"], wantHigh)
	check("low   (min)", bar.Metrics["low"], wantLow)
	check("close (last)", bar.Metrics["close"], wantClose)
	check("volume (sum)", bar.Metrics["volume"], wantVolume)

	if bar.TickCount != wantCount {
		t.Errorf("  tick_count: got %d, want %d (MISMATCH)", bar.TickCount, wantCount)
		failures++
	} else {
		t.Logf("  tick_count: %d ✓", bar.TickCount)
	}

	if failures > 0 {
		t.Fatalf("%d metric(s) failed verification — rollup is NOT correct", failures)
	}
	t.Log("ALL metrics match expected values — rollup verified correct")
}

// TestRollupCorrectness_ASICMetrics verifies rollup for hardware telemetry.
func TestRollupCorrectness_ASICMetrics(t *testing.T) {
	ctx := context.Background()

	conn := dial(t, tikrAddr)
	defer conn.Close()
	client := pb.NewTikrClient(conn)

	stream, err := client.IngestTicks(ctx)
	if err != nil {
		t.Fatalf("IngestTicks: %v", err)
	}

	// Unique dimension to isolate test
	deviceID := fmt.Sprintf("test-device-%d", time.Now().UnixNano())
	dims := map[string]string{
		"device_id": deviceID,
		"asic_id":   "memory-0",
		"port_id":   "Ethernet1/1",
	}

	baseNs := uint64(time.Now().Unix()) * 1_000_000_000

	type sample struct {
		temperature    int64
		voltageMV      int64
		crcErrorsDelta int64
		packetDrops    int64
		memoryUtilBps  int64
	}

	samples := []sample{
		{temperature: 6500, voltageMV: 850, crcErrorsDelta: 0, packetDrops: 1, memoryUtilBps: 4500},
		{temperature: 6800, voltageMV: 845, crcErrorsDelta: 3, packetDrops: 0, memoryUtilBps: 4600},
		{temperature: 9500, voltageMV: 820, crcErrorsDelta: 15, packetDrops: 10, memoryUtilBps: 7500}, // fault!
		{temperature: 9200, voltageMV: 825, crcErrorsDelta: 8, packetDrops: 5, memoryUtilBps: 7200},
		{temperature: 6600, voltageMV: 848, crcErrorsDelta: 0, packetDrops: 0, memoryUtilBps: 4550},
	}

	// Compute expected values
	wantTempMax := int64(9500)                                            // max
	wantTempMin := int64(6500)                                            // min
	wantTempAvgNum := int64(6500 + 6800 + 9500 + 9200 + 6600)           // sum = 38600
	wantVoltageMin := int64(820)                                          // min
	wantVoltageMax := int64(850)                                          // max
	wantCRCTotal := int64(0 + 3 + 15 + 8 + 0)                           // sum = 26
	wantDropsTotal := int64(1 + 0 + 10 + 5 + 0)                         // sum = 16
	wantMemUtilMax := int64(7500)                                         // max
	wantCount := uint64(5)

	var ticks []*pb.TickData
	for i, s := range samples {
		ticks = append(ticks, &pb.TickData{
			TimestampNs: baseNs + uint64(i)*1_000_000,
			Dimensions:  dims,
			Fields: map[string]int64{
				"temperature":      s.temperature,
				"voltage_mv":       s.voltageMV,
				"crc_errors_delta": s.crcErrorsDelta,
				"packet_drops":     s.packetDrops,
				"memory_util_bps":  s.memoryUtilBps,
			},
			Sequence: uint32(i),
		})
	}

	if err := stream.Send(&pb.IngestRequest{Series: "asic_metrics", Ticks: ticks}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Flush trigger: tick in next bucket
	if err := stream.Send(&pb.IngestRequest{
		Series: "asic_metrics",
		Ticks: []*pb.TickData{{
			TimestampNs: baseNs + 2*1_000_000_000,
			Dimensions:  dims,
			Fields:      map[string]int64{"temperature": 6500, "voltage_mv": 850, "crc_errors_delta": 0, "packet_drops": 0, "memory_util_bps": 4500},
		}},
	}); err != nil {
		t.Fatalf("Send flush: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	t.Logf("ingested %d samples", resp.TicksReceived)

	time.Sleep(3 * time.Second)

	barStream, err := client.QueryBars(ctx, &pb.BarQuery{
		Series:     "asic_metrics",
		Dimensions: dims,
		StartNs:    baseNs - 1_000_000_000,
		EndNs:      baseNs + 1_000_000_000,
	})
	if err != nil {
		t.Fatalf("QueryBars: %v", err)
	}

	var bars []*pb.BarData
	for {
		bar, err := barStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		bars = append(bars, bar)
	}

	if len(bars) != 1 {
		t.Fatalf("expected 1 bar, got %d", len(bars))
	}

	bar := bars[0]
	failures := 0
	check := func(name string, got, want int64) {
		if got != want {
			t.Errorf("  %s: got %d, want %d (MISMATCH)", name, got, want)
			failures++
		} else {
			t.Logf("  %s: %d ✓", name, got)
		}
	}

	check("temp_max", bar.Metrics["temp_max"], wantTempMax)
	check("temp_min", bar.Metrics["temp_min"], wantTempMin)
	check("temp_avg_numerator", bar.Metrics["temp_avg_numerator"], wantTempAvgNum)
	check("voltage_min", bar.Metrics["voltage_min"], wantVoltageMin)
	check("voltage_max", bar.Metrics["voltage_max"], wantVoltageMax)
	check("crc_errors_total", bar.Metrics["crc_errors_total"], wantCRCTotal)
	check("packet_drops_total", bar.Metrics["packet_drops_total"], wantDropsTotal)
	check("memory_util_max", bar.Metrics["memory_util_max"], wantMemUtilMax)

	if bar.TickCount != wantCount {
		t.Errorf("  sample_count: got %d, want %d (MISMATCH)", bar.TickCount, wantCount)
		failures++
	} else {
		t.Logf("  sample_count: %d ✓", bar.TickCount)
	}

	if failures > 0 {
		t.Fatalf("%d metric(s) failed — ASIC rollup is NOT correct", failures)
	}
	t.Log("ALL ASIC metrics match — rollup verified correct")
}

// TestRollupCorrectness_MultiBucket verifies rollup across multiple time buckets.
// Ensures bucket boundaries are correctly detected and each bucket is independent.
func TestRollupCorrectness_MultiBucket(t *testing.T) {
	ctx := context.Background()

	conn := dial(t, tikrAddr)
	defer conn.Close()
	client := pb.NewTikrClient(conn)

	stream, err := client.IngestTicks(ctx)
	if err != nil {
		t.Fatalf("IngestTicks: %v", err)
	}

	symbol := fmt.Sprintf("MULTI_%d", time.Now().UnixNano())
	baseNs := uint64(time.Now().Unix()) * 1_000_000_000

	// Bucket 1: prices [100, 200, 300] → open=100, high=300, low=100, close=300, volume=30
	// Bucket 2: prices [500, 400] → open=500, high=500, low=400, close=400, volume=20
	type bucketExpected struct {
		open, high, low, close, volume int64
		count                          uint64
	}
	buckets := []struct {
		ticks    []struct{ price, qty int64 }
		expected bucketExpected
	}{
		{
			ticks: []struct{ price, qty int64 }{
				{100, 10}, {200, 10}, {300, 10},
			},
			expected: bucketExpected{open: 100, high: 300, low: 100, close: 300, volume: 30, count: 3},
		},
		{
			ticks: []struct{ price, qty int64 }{
				{500, 10}, {400, 10},
			},
			expected: bucketExpected{open: 500, high: 500, low: 400, close: 400, volume: 20, count: 2},
		},
	}

	for secIdx, bucket := range buckets {
		var ticks []*pb.TickData
		for i, tick := range bucket.ticks {
			ticks = append(ticks, &pb.TickData{
				TimestampNs: baseNs + uint64(secIdx)*1_000_000_000 + uint64(i)*1_000_000,
				Dimensions:  map[string]string{"symbol": symbol},
				Fields:      map[string]int64{"price": tick.price, "quantity": tick.qty},
				Sequence:    uint32(i),
			})
		}
		if err := stream.Send(&pb.IngestRequest{Series: "market_ticks", Ticks: ticks}); err != nil {
			t.Fatalf("Send bucket %d: %v", secIdx, err)
		}
	}

	// Flush trigger: tick in bucket 3
	if err := stream.Send(&pb.IngestRequest{
		Series: "market_ticks",
		Ticks: []*pb.TickData{{
			TimestampNs: baseNs + 3*1_000_000_000,
			Dimensions:  map[string]string{"symbol": symbol},
			Fields:      map[string]int64{"price": 1, "quantity": 1},
		}},
	}); err != nil {
		t.Fatalf("Send flush: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	t.Logf("ingested %d ticks across %d buckets", resp.TicksReceived, len(buckets))

	time.Sleep(3 * time.Second)

	barStream, err := client.QueryBars(ctx, &pb.BarQuery{
		Series:     "market_ticks",
		Dimensions: map[string]string{"symbol": symbol},
		StartNs:    baseNs - 1_000_000_000,
		EndNs:      baseNs + 2*1_000_000_000,
	})
	if err != nil {
		t.Fatalf("QueryBars: %v", err)
	}

	var bars []*pb.BarData
	for {
		bar, err := barStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		bars = append(bars, bar)
	}

	if len(bars) != 2 {
		t.Fatalf("expected 2 bars (one per bucket), got %d", len(bars))
	}

	for i, bar := range bars {
		exp := buckets[i].expected
		t.Logf("bucket %d: open=%d high=%d low=%d close=%d vol=%d count=%d",
			i, bar.Metrics["open"], bar.Metrics["high"],
			bar.Metrics["low"], bar.Metrics["close"],
			bar.Metrics["volume"], bar.TickCount)

		failures := 0
		check := func(name string, got, want int64) {
			if got != want {
				t.Errorf("  bucket[%d] %s: got %d, want %d", i, name, got, want)
				failures++
			}
		}
		check("open", bar.Metrics["open"], exp.open)
		check("high", bar.Metrics["high"], exp.high)
		check("low", bar.Metrics["low"], exp.low)
		check("close", bar.Metrics["close"], exp.close)
		check("volume", bar.Metrics["volume"], exp.volume)
		if bar.TickCount != exp.count {
			t.Errorf("  bucket[%d] count: got %d, want %d", i, bar.TickCount, exp.count)
			failures++
		}
		if failures > 0 {
			t.Fatalf("bucket[%d]: %d metric(s) wrong", i, failures)
		}
	}
	t.Log("ALL buckets verified correct — multi-bucket rollup works")
}

// TestIngestAndQueryTicks ingests ticks and queries them back raw.
func TestIngestAndQueryTicks(t *testing.T) {
	ctx := context.Background()

	conn := dial(t, tikrAddr)
	defer conn.Close()
	client := pb.NewTikrClient(conn)

	stream, err := client.IngestTicks(ctx)
	if err != nil {
		t.Fatalf("IngestTicks: %v", err)
	}

	symbol := fmt.Sprintf("RAW_%d", time.Now().UnixNano())
	baseNs := uint64(time.Now().Unix()) * 1_000_000_000
	var ticks []*pb.TickData
	for i := 0; i < 10; i++ {
		ticks = append(ticks, &pb.TickData{
			TimestampNs: baseNs + uint64(i)*1_000_000,
			Dimensions:  map[string]string{"symbol": symbol},
			Fields:      map[string]int64{"price": int64(100 + i), "quantity": 1},
			Sequence:    uint32(i),
		})
	}

	if err := stream.Send(&pb.IngestRequest{Series: "market_ticks", Ticks: ticks}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if resp.TicksReceived != 10 {
		t.Fatalf("ticks_received: got %d, want 10", resp.TicksReceived)
	}

	time.Sleep(500 * time.Millisecond)

	tickStream, err := client.QueryTicks(ctx, &pb.TickQuery{
		Series:     "market_ticks",
		Dimensions: map[string]string{"symbol": symbol},
		StartNs:    baseNs - 1_000_000_000,
		EndNs:      baseNs + 60*1_000_000_000,
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("QueryTicks: %v", err)
	}

	var results []*pb.TickData
	for {
		tick, err := tickStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv tick: %v", err)
		}
		results = append(results, tick)
	}

	if len(results) != 10 {
		t.Fatalf("expected 10 ticks, got %d", len(results))
	}

	for i, tick := range results {
		if tick.Fields["price"] != int64(100+i) {
			t.Errorf("tick[%d] price: got %d, want %d", i, tick.Fields["price"], 100+i)
		}
	}
	t.Log("ALL raw ticks returned correctly — storage round-trip verified")
}

// TestListSeries verifies all configured series are returned.
func TestListSeries(t *testing.T) {
	ctx := context.Background()

	conn := dial(t, tikrAddr)
	defer conn.Close()
	client := pb.NewTikrClient(conn)

	list, err := client.ListSeries(ctx, &pb.Empty{})
	if err != nil {
		t.Fatalf("ListSeries: %v", err)
	}

	seriesNames := make(map[string]bool)
	for _, s := range list.Series {
		seriesNames[s.Name] = true
		t.Logf("series: %s (rollup=%s, dims=%v, metrics=%v)",
			s.Name, s.Rollup, s.Dimensions, s.Metrics)
	}

	for _, expected := range []string{"market_ticks", "network_flows", "asic_metrics"} {
		if !seriesNames[expected] {
			t.Errorf("missing series: %s", expected)
		}
	}
}

// TestKafkaIntegration will verify that rolled-up bars are published to Kafka.
func TestKafkaIntegration(t *testing.T) {
	t.Skip("kafka integration requires shared Docker network — enable after TIKR_KAFKA_BROKERS env wiring is verified")
}
