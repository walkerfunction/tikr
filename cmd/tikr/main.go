package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tikr-dev/tikr/pkg/config"
	"github.com/tikr-dev/tikr/pkg/core"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "/etc/tikr/default.yaml", "path to config file")
	flag.Parse()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Load series specs
	specs, err := core.LoadSpecs(cfg.SpecsDir)
	if err != nil {
		log.Fatalf("failed to load specs: %v", err)
	}

	fmt.Printf("Tikr %s starting\n", version)
	fmt.Printf("  gRPC port: %d\n", cfg.Server.GRPCPort)
	fmt.Printf("  HTTP port: %d\n", cfg.Server.HTTPPort)
	fmt.Printf("  data dir:  %s\n", cfg.Storage.DataDir)
	fmt.Printf("  series:    ")
	for _, s := range specs {
		fmt.Printf("%s ", s.Series)
	}
	fmt.Println()

	// TODO: Phase 1 complete — wire up storage, ingest, agg, query in subsequent phases

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Printf("\nReceived %s, shutting down...\n", sig)
}
