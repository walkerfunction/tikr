package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	GRPCPort int `yaml:"grpc_port"`
	HTTPPort int `yaml:"http_port"`
}

type StorageTierConfig struct {
	TTL       string `yaml:"ttl"`
	MaxSizeGB int    `yaml:"max_size_gb"`
}

func (c *StorageTierConfig) TTLDuration() (time.Duration, error) {
	return time.ParseDuration(c.TTL)
}

func (c *StorageTierConfig) Validate() error {
	d, err := c.TTLDuration()
	if err != nil {
		return fmt.Errorf("invalid TTL %q: %w", c.TTL, err)
	}
	if d < time.Hour {
		return fmt.Errorf("TTL %s is below minimum (1h)", c.TTL)
	}
	if d > 24*time.Hour {
		return fmt.Errorf("TTL %s exceeds maximum (24h)", c.TTL)
	}
	if c.MaxSizeGB <= 0 {
		return fmt.Errorf("max_size_gb must be positive")
	}
	return nil
}

type StorageConfig struct {
	DataDir string            `yaml:"data_dir"`
	Ticks   StorageTierConfig `yaml:"ticks"`
	Rollup  StorageTierConfig `yaml:"rollup"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
}

type TelemetryKafkaConfig struct {
	Enabled      bool     `yaml:"enabled"`
	Brokers      []string `yaml:"brokers"`
	MetricsTopic string   `yaml:"metrics_topic"`
	Encoding     string   `yaml:"encoding"`
}

type PrometheusConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

type TelemetryConfig struct {
	Enabled     bool                 `yaml:"enabled"`
	ServiceName string               `yaml:"service_name"`
	Kafka       TelemetryKafkaConfig `yaml:"kafka"`
	Prometheus  PrometheusConfig     `yaml:"prometheus"`
}

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Storage   StorageConfig   `yaml:"storage"`
	Kafka     KafkaConfig     `yaml:"kafka"`
	Telemetry TelemetryConfig `yaml:"telemetry"`
	SpecsDir  string          `yaml:"specs_dir"`
}

func (c *Config) Validate() error {
	if c.Server.GRPCPort <= 0 {
		return fmt.Errorf("grpc_port must be positive")
	}
	if c.Storage.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if err := c.Storage.Ticks.Validate(); err != nil {
		return fmt.Errorf("storage.ticks: %w", err)
	}
	if err := c.Storage.Rollup.Validate(); err != nil {
		return fmt.Errorf("storage.rollup: %w", err)
	}
	if c.SpecsDir == "" {
		return fmt.Errorf("specs_dir is required")
	}
	return nil
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}
