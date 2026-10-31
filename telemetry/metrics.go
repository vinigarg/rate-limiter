// Package telemetry records limiter decisions with near-zero hot-path cost and
// ships them off the request path: counters are striped atomics, latency is a
// sampled histogram, and a background Flusher exports cumulative snapshots to
// a pipeline (OTLP/HTTP to an OpenTelemetry Collector) or serves them to
// Prometheus. Nothing here ever blocks or fails a decision.
package telemetry

import (
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

// BoundsSeconds are the explicit histogram bucket upper bounds for decision
// latency, tuned for a microsecond-to-millisecond budget.
var BoundsSeconds = []float64{1e-6, 2.5e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 5e-3, 1e-2, 5e-2, 1e-1}

var boundsNs = func() []int64 {
	out := make([]int64, len(BoundsSeconds))
	for i, b := range BoundsSeconds {
		out[i] = int64(b * 1e9)
	}
	return out
}()

const stripes = 16

type paddedU64 struct {
	v atomic.Uint64
	_ [56]byte // own cache line: no false sharing between stripes
}

// striped is a contention-free counter: writers pick a random stripe (the
// runtime's per-thread RNG is a few ns), readers sum all stripes.
type striped [stripes]paddedU64

func (s *striped) add() { s[rand.Uint32()&(stripes-1)].v.Add(1) }
func (s *striped) load() uint64 {
	var t uint64
	for i := range s {
		t += s[i].v.Load()
	}
	return t
}

// RuleMetrics is the per-rule recorder. Resolve it once when a rule is
// compiled; the hot path then does no map lookups.
type RuleMetrics struct {
	Rule string

	allowed, denied, shadowDenied, degraded, errors striped

	hist  [16]atomic.Uint64 // len(BoundsSeconds)+1, non-cumulative
	sumNs atomic.Uint64
	count atomic.Uint64
}

// Observe records one decision outcome.
func (m *RuleMetrics) Observe(d rl.Decision) {
	switch {
	case d.Shadow:
		m.shadowDenied.add()
	case d.Allowed:
		m.allowed.add()
	default:
		m.denied.add()
	}
	if d.Degraded {
		m.degraded.add()
	}
}

// ObserveError counts a backend (Redis) failure that triggered a fallback.
func (m *RuleMetrics) ObserveError() { m.errors.add() }

// ObserveLatency adds one latency sample (callers sample ~1 in N).
func (m *RuleMetrics) ObserveLatency(d time.Duration) {
	ns := int64(d)
	i := 0
	for i < len(boundsNs) && ns > boundsNs[i] {
		i++
	}
	m.hist[i].Add(1)
	m.sumNs.Add(uint64(ns))
	m.count.Add(1)
}

// RuleSnapshot is a point-in-time copy of one rule's cumulative metrics.
type RuleSnapshot struct {
	Rule         string
	Allowed      uint64
	Denied       uint64
	ShadowDenied uint64
	Degraded     uint64
	Errors       uint64
	LatencyCount uint64
	LatencySum   float64 // seconds
	LatencyHist  []uint64
}

func (m *RuleMetrics) snapshot() RuleSnapshot {
	s := RuleSnapshot{
		Rule: m.Rule, Allowed: m.allowed.load(), Denied: m.denied.load(),
		ShadowDenied: m.shadowDenied.load(), Degraded: m.degraded.load(), Errors: m.errors.load(),
		LatencyCount: m.count.Load(), LatencySum: float64(m.sumNs.Load()) / 1e9,
		LatencyHist: make([]uint64, len(m.hist)),
	}
	for i := range m.hist {
		s.LatencyHist[i] = m.hist[i].Load()
	}
	return s
}

// Registry owns all RuleMetrics.
type Registry struct {
	mu    sync.RWMutex
	rules map[string]*RuleMetrics
}

func NewRegistry() *Registry { return &Registry{rules: map[string]*RuleMetrics{}} }

// Rule returns (creating if needed) the metrics for a rule ID. Cold path.
func (r *Registry) Rule(id string) *RuleMetrics {
	r.mu.RLock()
	m := r.rules[id]
	r.mu.RUnlock()
	if m != nil {
		return m
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m = r.rules[id]; m == nil {
		m = &RuleMetrics{Rule: id}
		r.rules[id] = m
	}
	return m
}

// Snapshot returns all rules sorted by ID.
func (r *Registry) Snapshot() []RuleSnapshot {
	r.mu.RLock()
	out := make([]RuleSnapshot, 0, len(r.rules))
	for _, m := range r.rules {
		out = append(out, m.snapshot())
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Rule < out[j].Rule })
	return out
}
