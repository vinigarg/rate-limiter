package algo

import (
	"context"
	"sync"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/internal/keystore"
)

type fwState struct {
	mu    sync.Mutex
	start int64
	count int64
}

// FixedWindowLimiter counts requests in aligned windows. Cheapest algorithm,
// but allows up to 2x Limit across a window boundary.
type FixedWindowLimiter struct {
	clk    rl.Clock
	limit  int64
	window int64
	store  *keystore.Store[fwState]
	stop   func()
}

func NewFixedWindow(spec rl.Spec, o Options) *FixedWindowLimiter {
	f := &FixedWindowLimiter{clk: o.clock(), limit: spec.Limit, window: int64(spec.Window)}
	f.store = keystore.New(o.maxKeys(), func() *fwState { return &fwState{} })
	f.stop = f.store.RunJanitor(o.sweep(spec.Window), func(s *fwState) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.start+f.window <= f.clk.Now()
	})
	return f
}

func (f *FixedWindowLimiter) Allow(_ context.Context, key string, cost int64) (rl.Decision, error) {
	now := f.clk.Now()
	ws := now - now%f.window
	st := f.store.Get(key)
	st.mu.Lock()
	if st.start != ws {
		st.start, st.count = ws, 0
	}
	d := rl.Decision{Limit: f.limit, ResetAfter: time.Duration(ws + f.window - now)}
	if st.count+cost <= f.limit {
		st.count += cost
		d.Allowed = true
	} else {
		d.RetryAfter = d.ResetAfter
	}
	d.Remaining = maxI64(0, f.limit-st.count)
	st.mu.Unlock()
	return d, nil
}

func (f *FixedWindowLimiter) Refund(key string, cost int64) {
	if st := f.store.Peek(key); st != nil {
		st.mu.Lock()
		st.count = maxI64(0, st.count-cost)
		st.mu.Unlock()
	}
}

func (f *FixedWindowLimiter) Close() error { f.stop(); return nil }
