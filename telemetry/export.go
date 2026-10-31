package telemetry

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// Batch is one export: cumulative counters since Start.
type Batch struct {
	Start, Now time.Time
	Rules      []RuleSnapshot
}

// Exporter ships a batch to a telemetry pipeline. Implementations must honour
// ctx and must never be called from the decision path.
type Exporter interface {
	Export(ctx context.Context, b Batch) error
}

// Flusher periodically snapshots the Registry and exports it. Metrics are
// cumulative, so a slow or failed export loses nothing: the next one carries
// the totals. Aggregating in-process and flushing on a timer is what keeps
// per-decision telemetry off the hot path.
type Flusher struct {
	Registry *Registry
	Exporter Exporter
	Interval time.Duration // default 10s
	Logger   *slog.Logger

	ExportErrors atomic.Uint64
}

// Run blocks until ctx is cancelled, then performs one final flush.
func (f *Flusher) Run(ctx context.Context) {
	if f.Interval <= 0 {
		f.Interval = 10 * time.Second
	}
	if f.Logger == nil {
		f.Logger = slog.Default()
	}
	start := time.Now()
	t := time.NewTicker(f.Interval)
	defer t.Stop()
	flush := func(c context.Context) {
		if err := f.Exporter.Export(c, Batch{Start: start, Now: time.Now(), Rules: f.Registry.Snapshot()}); err != nil {
			f.ExportErrors.Add(1)
			f.Logger.Warn("telemetry export failed", "err", err)
		}
	}
	for {
		select {
		case <-t.C:
			cctx, cancel := context.WithTimeout(ctx, f.Interval)
			flush(cctx)
			cancel()
		case <-ctx.Done():
			cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			flush(cctx)
			cancel()
			return
		}
	}
}
