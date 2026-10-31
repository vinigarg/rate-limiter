// Package ratelimiter defines the core contracts shared by every limiter in
// this module: the Limiter interface, the Decision it returns, the Spec that
// configures a limit, and a monotonic Clock that can be faked in tests.
package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Decision is the verdict for one (key, cost) check.
type Decision struct {
	Allowed bool
	// Limit is the configured ceiling (requests per window, or burst for
	// bucket algorithms). Used for RateLimit-Limit headers.
	Limit int64
	// Remaining is the quota left after this decision (never negative).
	Remaining int64
	// RetryAfter is how long a denied caller should wait. Zero when allowed.
	RetryAfter time.Duration
	// ResetAfter is when the quota is fully replenished (best effort).
	ResetAfter time.Duration
	// Delay is set by shaping algorithms (leaky bucket): the request is
	// admitted but should be released after this delay.
	Delay time.Duration
	// Degraded is true when the decision came from a failure policy
	// (fail-open / fail-closed / local fallback) instead of the primary limiter.
	Degraded bool
	// Shadow is true when the rule is in shadow mode and the request WOULD
	// have been denied. Allowed is forced to true.
	Shadow bool
}

// Limiter decides whether `cost` units may be spent on `key` right now.
// Implementations must be safe for concurrent use.
type Limiter interface {
	Allow(ctx context.Context, key string, cost int64) (Decision, error)
}

// Refunder is implemented by limiters that can give back previously spent
// cost. It lets Multi roll back windows when a sibling window denies, so a
// request never burns quota on a limit it was rejected by another limit.
type Refunder interface {
	Refund(key string, cost int64)
}

// Spec configures one limit: Limit units per Window, with optional Burst
// (bucket capacity). Burst==0 means Burst=Limit.
type Spec struct {
	Limit  int64
	Window time.Duration
	Burst  int64
}

// ErrInvalidSpec is returned for unusable limit configurations.
var ErrInvalidSpec = errors.New("ratelimiter: invalid spec")

// Validate checks the spec.
func (s Spec) Validate() error {
	switch {
	case s.Limit <= 0:
		return fmt.Errorf("%w: limit must be > 0", ErrInvalidSpec)
	case s.Window <= 0:
		return fmt.Errorf("%w: window must be > 0", ErrInvalidSpec)
	case s.Burst < 0:
		return fmt.Errorf("%w: burst must be >= 0", ErrInvalidSpec)
	case s.Window/time.Duration(s.Limit) < 1:
		return fmt.Errorf("%w: limit/window finer than 1ns per unit", ErrInvalidSpec)
	}
	return nil
}

// EffectiveBurst returns Burst, defaulting to Limit.
func (s Spec) EffectiveBurst() int64 {
	if s.Burst > 0 {
		return s.Burst
	}
	return s.Limit
}

// Clock returns monotonic nanoseconds from an arbitrary fixed origin.
// Monotonic time keeps limiters correct across NTP/wall-clock jumps.
type Clock interface{ Now() int64 }

type systemClock struct{ start time.Time }

func (c systemClock) Now() int64 { return int64(time.Since(c.start)) }

// NewSystemClock returns a monotonic real-time clock.
func NewSystemClock() Clock { return systemClock{start: time.Now()} }

// ManualClock is a deterministic clock for tests and simulations.
type ManualClock struct{ n atomic.Int64 }

// NewManualClock starts at 1h so window alignment math never sees zero.
func NewManualClock() *ManualClock {
	c := &ManualClock{}
	c.n.Store(int64(time.Hour))
	return c
}

func (c *ManualClock) Now() int64              { return c.n.Load() }
func (c *ManualClock) Advance(d time.Duration) { c.n.Add(int64(d)) }
