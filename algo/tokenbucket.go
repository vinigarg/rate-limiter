package algo

import (
	"context"
	"sync"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/internal/keystore"
)

type tbState struct {
	mu     sync.Mutex
	tokens float64
	last   int64
	init   bool
}

// TokenBucketLimiter refills Limit/Window tokens per unit time up to Burst.
// Refill is lazy (computed on access): no timers, two numbers per key.
type TokenBucketLimiter struct {
	clk       rl.Clock
	limit     int64
	window    time.Duration
	burst     float64
	ratePerNs float64
	fullNs    int64 // time to refill an empty bucket
	store     *keystore.Store[tbState]
	stop      func()
}

func NewTokenBucket(spec rl.Spec, o Options) *TokenBucketLimiter {
	b := float64(spec.EffectiveBurst())
	t := &TokenBucketLimiter{
		clk: o.clock(), limit: spec.Limit, window: spec.Window, burst: b,
		ratePerNs: float64(spec.Limit) / float64(spec.Window),
	}
	t.fullNs = int64(b / t.ratePerNs)
	t.store = keystore.New(o.maxKeys(), func() *tbState { return &tbState{} })
	t.stop = t.store.RunJanitor(o.sweep(spec.Window), func(s *tbState) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.init && t.clk.Now()-s.last >= t.fullNs
	})
	return t
}

func (t *TokenBucketLimiter) Allow(_ context.Context, key string, cost int64) (rl.Decision, error) {
	c := float64(cost)
	d := rl.Decision{Limit: int64(t.burst)}
	if c > t.burst { // can never be satisfied
		d.RetryAfter = t.window
		return d, nil
	}
	now := t.clk.Now()
	st := t.store.Get(key)
	st.mu.Lock()
	if !st.init {
		st.tokens, st.last, st.init = t.burst, now, true
	} else if now > st.last {
		st.tokens += float64(now-st.last) * t.ratePerNs
		if st.tokens > t.burst {
			st.tokens = t.burst
		}
		st.last = now
	}
	if st.tokens >= c {
		st.tokens -= c
		d.Allowed = true
	} else {
		d.RetryAfter = time.Duration((c - st.tokens) / t.ratePerNs)
	}
	d.Remaining = int64(st.tokens)
	d.ResetAfter = time.Duration((t.burst - st.tokens) / t.ratePerNs)
	st.mu.Unlock()
	return d, nil
}

func (t *TokenBucketLimiter) Refund(key string, cost int64) {
	if st := t.store.Peek(key); st != nil {
		st.mu.Lock()
		st.tokens += float64(cost)
		if st.tokens > t.burst {
			st.tokens = t.burst
		}
		st.mu.Unlock()
	}
}

func (t *TokenBucketLimiter) Close() error { t.stop(); return nil }
