// Package engine is the rule engine: it matches a Request against
// client-configurable rules, builds the bucket key from the configured
// dimensions, runs the rule's strategy (local, Redis, or hybrid with a failure
// policy) and returns the strictest verdict. Rules live in an immutable
// snapshot swapped atomically, so reads take no locks and reloads never block
// traffic.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"sync/atomic"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/algo"
	"github.com/yourorg/ratelimiter/distributed"
	"github.com/yourorg/ratelimiter/resilience"
	"github.com/yourorg/ratelimiter/telemetry"
)

// Options wires the engine's dependencies.
type Options struct {
	Clock   rl.Clock
	Redis   distributed.Client // required for scope global/hybrid
	Metrics *telemetry.Registry
	Logger  *slog.Logger
	// LatencySampleEvery measures decision latency for ~1 in N checks (power
	// of two recommended). Default 16. 1 = every check.
	LatencySampleEvery uint32
	// RedisTimeout bounds each Redis call made for a decision. Default 5ms:
	// the latency budget, not Redis's patience, decides when to fall back.
	RedisTimeout time.Duration
	// MaxKeys caps keys per local limiter (see algo.Options).
	MaxKeys int
	// KeyPrefix namespaces Redis keys (e.g. "rl:prod:").
	KeyPrefix string
}

// Result is the engine's verdict.
type Result struct {
	rl.Decision
	RuleID string // the rule that denied, or the binding rule when allowed ("" if no rule matched)
}

type compiledRule struct {
	id       string
	priority int
	hash     string
	match    func(*Request) bool
	dims     []dimFn
	limiter  rl.Limiter
	shadow   bool
	policy   resilience.Policy
	metrics  *telemetry.RuleMetrics
}

func (c *compiledRule) key(buf []byte, r *Request) string {
	for i, d := range c.dims {
		if i > 0 {
			buf = append(buf, '|')
		}
		if v := d(r); v != "" {
			buf = append(buf, v...)
		} else {
			buf = append(buf, '-')
		}
	}
	return string(buf)
}

type snapshot struct{ rules []*compiledRule }

// Engine evaluates rules. Safe for concurrent use.
type Engine struct {
	o    Options
	snap atomic.Pointer[snapshot]
	mask uint32
}

// New creates an empty engine; call Reload to install rules.
func New(o Options) *Engine {
	if o.Clock == nil {
		o.Clock = rl.NewSystemClock()
	}
	if o.Metrics == nil {
		o.Metrics = telemetry.NewRegistry()
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.LatencySampleEvery == 0 {
		o.LatencySampleEvery = 16
	}
	if o.RedisTimeout <= 0 {
		o.RedisTimeout = 5 * time.Millisecond
	}
	e := &Engine{o: o, mask: o.LatencySampleEvery - 1}
	e.snap.Store(&snapshot{})
	return e
}

// Metrics exposes the registry (for /metrics and exporters).
func (e *Engine) Metrics() *telemetry.Registry { return e.o.Metrics }

// Check evaluates all matching rules. It never returns an error: failures of
// the distributed tier are converted into the rule's failure policy.
func (e *Engine) Check(ctx context.Context, r *Request) Result {
	cost := r.Cost
	if cost <= 0 {
		cost = 1
	}
	var buf [160]byte
	out := Result{Decision: rl.Decision{Allowed: true, Remaining: math.MaxInt64}}
	matched := false
	for _, cr := range e.snap.Load().rules {
		if !cr.match(r) {
			continue
		}
		matched = true
		key := cr.key(buf[:0], r)

		sample := e.mask == 0 || rand.Uint32()&e.mask == 0
		var t0 time.Time
		if sample {
			t0 = time.Now()
		}
		d, err := cr.limiter.Allow(ctx, key, cost)
		if sample {
			cr.metrics.ObserveLatency(time.Since(t0))
		}
		if err != nil { // only unguarded limiters can error; apply the rule policy
			cr.metrics.ObserveError()
			d = rl.Decision{Allowed: cr.policy != resilience.FailClosed, Degraded: true}
		}
		if cr.shadow && !d.Allowed {
			d.Allowed, d.Shadow = true, true
		}
		cr.metrics.Observe(d)
		if !d.Allowed {
			return Result{Decision: d, RuleID: cr.id}
		}
		if d.Remaining < out.Remaining {
			out = Result{Decision: d, RuleID: cr.id}
		}
	}
	if !matched {
		return Result{Decision: rl.Decision{Allowed: true}}
	}
	return out
}

// Reload validates cfg, compiles it off to the side and swaps it in
// atomically. On any error the running config is untouched. Rules whose
// definition did not change keep their limiter (and therefore their counters).
func (e *Engine) Reload(cfg Config) error {
	old := e.snap.Load()
	prev := make(map[string]*compiledRule, len(old.rules))
	for _, r := range old.rules {
		prev[r.id] = r
	}
	var next []*compiledRule
	var errs []error
	seen := map[string]bool{}
	for _, rule := range cfg.Rules {
		if seen[rule.ID] {
			errs = append(errs, fmt.Errorf("rule %q: duplicate id", rule.ID))
			continue
		}
		seen[rule.ID] = true
		h := hashRule(rule)
		if p := prev[rule.ID]; p != nil && p.hash == h {
			next = append(next, p)
			continue
		}
		cr, err := e.compile(rule, h)
		if err != nil {
			errs = append(errs, fmt.Errorf("rule %q: %w", rule.ID, err))
			continue
		}
		next = append(next, cr)
	}
	if err := errors.Join(errs...); err != nil {
		for _, cr := range next { // close limiters we built but will not install
			if p := prev[cr.id]; p != cr {
				closeLimiter(cr.limiter)
			}
		}
		return err
	}
	sort.SliceStable(next, func(i, j int) bool { return next[i].priority > next[j].priority })
	e.snap.Store(&snapshot{rules: next})

	// audit trail: every config change is a deploy-equivalent event
	installed := map[string]bool{}
	for _, cr := range next {
		installed[cr.id] = true
		switch p := prev[cr.id]; {
		case p == nil:
			e.o.Logger.Info("rule added", "rule", cr.id, "hash", cr.hash[:12])
		case p != cr:
			e.o.Logger.Info("rule changed", "rule", cr.id, "old_hash", p.hash[:12], "new_hash", cr.hash[:12])
			closeLimiter(p.limiter)
		}
	}
	for id, p := range prev {
		if !installed[id] {
			e.o.Logger.Info("rule removed", "rule", id)
			closeLimiter(p.limiter)
		}
	}
	return nil
}

func closeLimiter(l rl.Limiter) {
	if c, ok := l.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// Close stops background work of all current limiters.
func (e *Engine) Close() {
	for _, r := range e.snap.Load().rules {
		closeLimiter(r.limiter)
	}
}

func hashRule(r Rule) string {
	b, _ := json.Marshal(r)
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum)
}

func (e *Engine) compile(r Rule, hash string) (*compiledRule, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if r.Strategy == "" {
		r.Strategy = algo.TokenBucketName
	}
	if r.Scope == "" {
		r.Scope = "local"
	}
	policy, err := resilience.ParsePolicy(r.OnError)
	if err != nil {
		return nil, err
	}
	match, err := compileMatch(r.Match)
	if err != nil {
		return nil, err
	}
	cr := &compiledRule{id: r.ID, priority: r.Priority, hash: hash, match: match, shadow: r.Shadow,
		policy: policy, metrics: e.o.Metrics.Rule(r.ID)}
	for _, name := range r.KeyBy {
		fn, err := resolveDim(name)
		if err != nil {
			return nil, fmt.Errorf("key_by: %w", err)
		}
		cr.dims = append(cr.dims, fn)
	}

	lo := algo.Options{Clock: e.o.Clock, MaxKeys: e.o.MaxKeys}
	var ls []rl.Limiter
	fail := func(err error) (*compiledRule, error) {
		for _, l := range ls {
			closeLimiter(l)
		}
		return nil, err
	}
	scale := r.FallbackScale
	if scale <= 0 {
		scale = 1
	}
	for i, lim := range r.Limits {
		spec := rl.Spec{Limit: lim.Limit, Window: time.Duration(lim.Window), Burst: lim.Burst}
		if err := spec.Validate(); err != nil {
			return fail(fmt.Errorf("limits[%d]: %w", i, err))
		}
		switch r.Scope {
		case "local":
			l, err := algo.New(r.Strategy, spec, lo)
			if err != nil {
				return fail(err)
			}
			ls = append(ls, l)
		case "global", "hybrid":
			if e.o.Redis == nil {
				return fail(fmt.Errorf("scope %q needs a Redis client", r.Scope))
			}
			prefix := fmt.Sprintf("%s%s:%d:", e.o.KeyPrefix, r.ID, i)
			var primary rl.Limiter
			if r.Scope == "global" {
				primary, err = distributed.NewCentral(e.o.Redis, r.Strategy, spec, prefix)
			} else {
				if r.Strategy != algo.TokenBucketName {
					return fail(fmt.Errorf("scope hybrid leases tokens from a token bucket; set strategy to %q", algo.TokenBucketName))
				}
				primary, err = distributed.NewHybrid(e.o.Redis, spec, distributed.HybridOptions{
					LeaseSize: r.LeaseSize, LeaseTTL: time.Duration(r.LeaseTTL), Clock: e.o.Clock,
					MaxKeys: e.o.MaxKeys, Prefix: prefix, RefillTimeout: e.o.RedisTimeout})
			}
			if err != nil {
				return fail(err)
			}
			var fallback rl.Limiter
			if policy == resilience.FailLocal {
				fs := spec
				fs.Limit = max(1, int64(float64(spec.Limit)*scale))
				if fs.Burst > 0 {
					fs.Burst = max(1, int64(float64(spec.Burst)*scale))
				}
				fbStrategy := r.Strategy
				if fbStrategy != algo.TokenBucketName && !contains(algo.Strategies, fbStrategy) {
					fbStrategy = algo.TokenBucketName
				}
				if fallback, err = algo.New(fbStrategy, fs, lo); err != nil {
					return fail(err)
				}
			}
			m := cr.metrics
			guardTimeout := e.o.RedisTimeout // global: every decision hits Redis
			if r.Scope == "hybrid" {
				guardTimeout = 0 // hybrid applies the budget itself, on its slow path only
			}
			ls = append(ls, resilience.NewGuard(primary, fallback, resilience.Config{
				Policy: policy, Timeout: guardTimeout, Clock: e.o.Clock,
				OnFallback: func(error, rl.Decision) { m.ObserveError() },
			}))
		default:
			return fail(fmt.Errorf("unknown scope %q (local|global|hybrid)", r.Scope))
		}
	}
	cr.limiter = algo.NewMulti(ls...)
	return cr, nil
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// WatchFile polls path every interval and hot-reloads on change. A bad file is
// logged and ignored: the last good config keeps serving.
func (e *Engine) WatchFile(ctx context.Context, path string, interval time.Duration) {
	var lastMod time.Time
	var lastSize int64 = -1
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if st, err := os.Stat(path); err == nil && (st.ModTime() != lastMod || st.Size() != lastSize) {
			lastMod, lastSize = st.ModTime(), st.Size()
			if cfg, err := LoadConfig(path); err != nil {
				e.o.Logger.Error("config rejected", "path", path, "err", err)
			} else if err := e.Reload(cfg); err != nil {
				e.o.Logger.Error("config rejected", "path", path, "err", err)
			} else {
				e.o.Logger.Info("config reloaded", "path", path, "rules", len(cfg.Rules))
			}
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}
