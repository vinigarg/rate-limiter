package algo

import (
	"context"
	"math"
	"sync"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/internal/keystore"
)

type swcState struct {
	mu       sync.Mutex
	curStart int64
	cur      int64
	prev     int64
}

// SlidingWindowCounterLimiter keeps two counters per key and weights the
// previous window by how much of it still overlaps the sliding window:
//
//	estimate = prev*(1-elapsedFraction) + cur
//
// O(1) memory, no boundary burst, ~0.003% error in Cloudflare's measurements.
type SlidingWindowCounterLimiter struct {
	clk    rl.Clock
	limit  int64
	window int64
	store  *keystore.Store[swcState]
	stop   func()
}

func NewSlidingWindowCounter(spec rl.Spec, o Options) *SlidingWindowCounterLimiter {
	s := &SlidingWindowCounterLimiter{clk: o.clock(), limit: spec.Limit, window: int64(spec.Window)}
	s.store = keystore.New(o.maxKeys(), func() *swcState { return &swcState{} })
	s.stop = s.store.RunJanitor(o.sweep(spec.Window), func(st *swcState) bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return s.clk.Now()-st.curStart >= 2*s.window
	})
	return s
}

func (s *SlidingWindowCounterLimiter) Allow(_ context.Context, key string, cost int64) (rl.Decision, error) {
	now := s.clk.Now()
	start := now - now%s.window
	st := s.store.Get(key)
	st.mu.Lock()
	if start != st.curStart {
		if start-st.curStart == s.window {
			st.prev = st.cur
		} else {
			st.prev = 0
		}
		st.cur, st.curStart = 0, start
	}
	frac := float64(now-start) / float64(s.window)
	est := float64(st.prev)*(1-frac) + float64(st.cur)
	d := rl.Decision{Limit: s.limit, ResetAfter: time.Duration(start + s.window - now)}
	if est+float64(cost) <= float64(s.limit) {
		st.cur += cost
		est += float64(cost)
		d.Allowed = true
	} else {
		room := s.limit - st.cur - cost
		rem := float64(start + s.window - now)
		wait := rem
		switch {
		case st.prev > 0 && room >= 0:
			// the decaying previous window frees enough room inside this window
			wait = (1 - float64(room)/float64(st.prev) - frac) * float64(s.window)
		case st.cur > 0:
			// must wait for rollover, after which cur becomes prev and keeps decaying
			next := 1 - float64(s.limit-cost)/float64(st.cur)
			wait = rem + math.Max(next, 0)*float64(s.window)
		}
		d.RetryAfter = time.Duration(math.Max(wait, float64(time.Millisecond)))
	}
	d.Remaining = maxI64(0, int64(float64(s.limit)-est))
	st.mu.Unlock()
	return d, nil
}

func (s *SlidingWindowCounterLimiter) Refund(key string, cost int64) {
	if st := s.store.Peek(key); st != nil {
		st.mu.Lock()
		st.cur = maxI64(0, st.cur-cost)
		st.mu.Unlock()
	}
}

func (s *SlidingWindowCounterLimiter) Close() error { s.stop(); return nil }
