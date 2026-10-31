// Package algo contains the in-process rate limiting algorithms. Every
// algorithm keeps O(1) (or O(limit) for the log) state per key in a sharded,
// capped store and evicts idle keys in the background.
package algo

import (
	"fmt"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

// Strategy names accepted by New.
const (
	FixedWindow          = "fixed_window"
	SlidingWindowCounter = "sliding_window_counter"
	SlidingWindowLog     = "sliding_window_log"
	TokenBucketName      = "token_bucket"
	LeakyBucketName      = "leaky_bucket"
	GCRAName             = "gcra"
)

// Strategies lists every supported local strategy.
var Strategies = []string{FixedWindow, SlidingWindowCounter, SlidingWindowLog, TokenBucketName, LeakyBucketName, GCRAName}

// DefaultMaxKeys bounds memory per limiter instance.
const DefaultMaxKeys = 1_000_000

// Options tunes a local limiter.
type Options struct {
	Clock         rl.Clock      // default: monotonic system clock
	MaxKeys       int           // default DefaultMaxKeys; <0 disables the cap
	SweepInterval time.Duration // default 1s..1m derived from the window
}

func (o Options) clock() rl.Clock {
	if o.Clock != nil {
		return o.Clock
	}
	return rl.NewSystemClock()
}

func (o Options) maxKeys() int {
	switch {
	case o.MaxKeys < 0:
		return 0
	case o.MaxKeys == 0:
		return DefaultMaxKeys
	}
	return o.MaxKeys
}

func (o Options) sweep(window time.Duration) time.Duration {
	if o.SweepInterval > 0 {
		return o.SweepInterval
	}
	d := window
	if d < time.Second {
		d = time.Second
	}
	if d > time.Minute {
		d = time.Minute
	}
	return d
}

// Local is a limiter that lives in this process.
type Local interface {
	rl.Limiter
	rl.Refunder
	Close() error
}

// New builds a local limiter by strategy name.
func New(strategy string, spec rl.Spec, opts Options) (Local, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	switch strategy {
	case FixedWindow:
		return NewFixedWindow(spec, opts), nil
	case SlidingWindowCounter:
		return NewSlidingWindowCounter(spec, opts), nil
	case SlidingWindowLog:
		return NewSlidingWindowLog(spec, opts), nil
	case TokenBucketName:
		return NewTokenBucket(spec, opts), nil
	case LeakyBucketName:
		return NewLeakyBucket(spec, opts), nil
	case GCRAName:
		return NewGCRA(spec, opts), nil
	}
	return nil, fmt.Errorf("algo: unknown strategy %q (want one of %v)", strategy, Strategies)
}

func minI64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func maxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
