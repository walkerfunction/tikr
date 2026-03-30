package agg

import (
	"context"

	"github.com/walkerfunction/tikr/pkg/core"
)

// BarHook is called when a rolled-up bar is flushed.
// Implementations can inspect/process the bar (e.g., ML anomaly detection).
// This interface is pre-wired for future use.
type BarHook interface {
	OnBarFlushed(ctx context.Context, bar *core.Bar) error
}

// NoopHook is the default hook that does nothing.
type NoopHook struct{}

func (NoopHook) OnBarFlushed(_ context.Context, _ *core.Bar) error { return nil }
