package config

import (
	"strings"
	"testing"
)

func validTier() StorageTierConfig {
	return StorageTierConfig{TTL: "6h", MaxSizeGB: 50}
}

func TestStorageTierConfig_ValidTTL(t *testing.T) {
	c := validTier()
	if err := c.Validate(); err != nil {
		t.Fatalf("expected valid, got: %v", err)
	}
}

func TestStorageTierConfig_TTLBelowMinimum(t *testing.T) {
	c := validTier()
	c.TTL = "30m"
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for TTL below 1h")
	}
	if !strings.Contains(err.Error(), "below minimum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStorageTierConfig_TTLAboveMaximum(t *testing.T) {
	c := validTier()
	c.TTL = "48h"
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for TTL above 24h")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStorageTierConfig_TTLBoundary(t *testing.T) {
	// Exactly 1h — valid
	c := validTier()
	c.TTL = "1h"
	if err := c.Validate(); err != nil {
		t.Fatalf("1h should be valid: %v", err)
	}

	// Exactly 24h — valid
	c.TTL = "24h"
	if err := c.Validate(); err != nil {
		t.Fatalf("24h should be valid: %v", err)
	}
}

func TestStorageTierConfig_InvalidTTLFormat(t *testing.T) {
	c := validTier()
	c.TTL = "not-a-duration"
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for invalid TTL format")
	}
	if !strings.Contains(err.Error(), "invalid TTL") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStorageTierConfig_ZeroMaxSize(t *testing.T) {
	c := validTier()
	c.MaxSizeGB = 0
	err := c.Validate()
	if err == nil {
		t.Fatal("expected error for zero max_size_gb")
	}
	if !strings.Contains(err.Error(), "max_size_gb") {
		t.Fatalf("unexpected error: %v", err)
	}
}
