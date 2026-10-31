// Package resilience wraps a fallible limiter (typically one that talks to
// Redis) with a timeout, a circuit breaker and an explicit failure policy.
//
// This is the "two-tier" design from the reference architecture: the primary
// distributed limiter is authoritative; when it errors, times out or its
// breaker is open, the Guard applies the configured policy:
//
//	FailOpen   - allow (API stays up, unprotected)
//	FailClosed - deny  (backend protected, limiter outage = API outage)
//	FailLocal  - fall back to an in-process limiter (stops the worst abuse
//	             without taking the API down). Recommended default.
package resilience

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

// Policy is what to do when the primary limiter cannot answer.
type Policy int

const (
	FailOpen Policy = iota
	FailClosed
	FailLocal
)

func (p Policy) String() string {
	return [...]string{"fail_open", "fail_closed", "fail_local"}[p]
}

// ParsePolicy parses "fail_open" | "fail_closed" | "fail_local".
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "", "fail_open":
		return FailOpen, nil
	case "fail_closed":
		return FailClosed, nil
	case "fail_local":
		return FailLocal, nil
	}
	return 0, fmt.Errorf("resilience: unknown policy %q", s)
}

// Config for a Guard.
type Config struct {
	Policy  Policy
	Timeout time.Duration // per-call budget for the primary; 0 = caller's ctx only
	// Breaker: open after FailureThreshold consecutive failures, stay open for
	// Cooldown, then let a single probe through (half-open).
	FailureThreshold int           // default 5
	Cooldown         time.Duration // default 1s
	// ClosedRetryAfter is the RetryAfter reported by FailClosed denials.
	ClosedRetryAfter time.Duration // default = Cooldown
	Clock            rl.Clock
	// OnFallback is invoked (off the decision's critical data path, but
	// synchronously) whenever a policy decision replaces the primary's answer.
	OnFallback func(err error, d rl.Decision)
}

// Guard implements rl.Limiter.
type Guard struct {
	primary  rl.Limiter
	fallback rl.Limiter // only for FailLocal
	cfg      Config

	failures  atomic.Int32
	openUntil atomic.Int64 // 0 = closed
	// observable counters for tests and metrics
	Fallbacks atomic.Uint64
	Opens     atomic.Uint64
}

// NewGuard builds a Guard. fallback is required for FailLocal; if nil, the
// policy degrades to FailOpen.
func NewGuard(primary, fallback rl.Limiter, cfg Config) *Guard {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = time.Second
	}
	if cfg.ClosedRetryAfter <= 0 {
		cfg.ClosedRetryAfter = cfg.Cooldown
	}
	if cfg.Clock == nil {
		cfg.Clock = rl.NewSystemClock()
	}
	if cfg.Policy == FailLocal && fallback == nil {
		cfg.Policy = FailOpen
	}
	return &Guard{primary: primary, fallback: fallback, cfg: cfg}
}

// BreakerOpen reports whether the breaker is currently rejecting calls.
func (g *Guard) BreakerOpen() bool {
	ou := g.openUntil.Load()
	return ou != 0 && g.cfg.Clock.Now() < ou
}

func (g *Guard) Allow(ctx context.Context, key string, cost int64) (rl.Decision, error) {
	if ou := g.openUntil.Load(); ou != 0 {
		now := g.cfg.Clock.Now()
		if now < ou {
			return g.degrade(ctx, key, cost, errBreakerOpen), nil
		}
		// half-open: exactly one caller wins the right to probe
		if !g.openUntil.CompareAndSwap(ou, now+int64(g.cfg.Cooldown)) {
			return g.degrade(ctx, key, cost, errBreakerOpen), nil
		}
	}
	if g.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.cfg.Timeout)
		defer cancel()
	}
	d, err := g.primary.Allow(ctx, key, cost)
	if err == nil {
		if g.failures.Load() != 0 {
			g.failures.Store(0)
		}
		if g.openUntil.Load() != 0 {
			g.openUntil.Store(0)
		}
		return d, nil
	}
	if int(g.failures.Add(1)) >= g.cfg.FailureThreshold {
		if g.openUntil.Swap(g.cfg.Clock.Now()+int64(g.cfg.Cooldown)) == 0 {
			g.Opens.Add(1)
		}
	}
	return g.degrade(ctx, key, cost, err), nil
}

var errBreakerOpen = fmt.Errorf("resilience: circuit breaker open")

func (g *Guard) degrade(ctx context.Context, key string, cost int64, cause error) rl.Decision {
	var d rl.Decision
	switch g.cfg.Policy {
	case FailClosed:
		d = rl.Decision{Allowed: false, RetryAfter: g.cfg.ClosedRetryAfter}
	case FailLocal:
		var err error
		if d, err = g.fallback.Allow(ctx, key, cost); err != nil {
			d = rl.Decision{Allowed: true} // the backstop itself failed: open
		}
	default:
		d = rl.Decision{Allowed: true}
	}
	d.Degraded = true
	g.Fallbacks.Add(1)
	if g.cfg.OnFallback != nil {
		g.cfg.OnFallback(cause, d)
	}
	return d
}

// Refund forwards to whichever tier supports it (best effort).
func (g *Guard) Refund(key string, cost int64) {
	if r, ok := g.primary.(rl.Refunder); ok {
		r.Refund(key, cost)
	}
}

// Close closes the closable members.
func (g *Guard) Close() error {
	for _, l := range []rl.Limiter{g.primary, g.fallback} {
		if c, ok := l.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	return nil
}
