package algo

import (
	"context"
	"sync/atomic"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/internal/keystore"
)

type gcraState struct{ tat atomic.Int64 } // theoretical arrival time

// GCRALimiter is the Generic Cell Rate Algorithm: a token bucket expressed as
// one int64 per key, updated with a lock-free CAS loop. This is the fastest
// local algorithm and the best default when you do not need sliding windows.
type GCRALimiter struct {
	clk   rl.Clock
	limit int64
	burst int64
	t     int64 // emission interval: ns per unit
	tau   int64 // burst tolerance: burst * t
	store *keystore.Store[gcraState]
	stop  func()
}

func NewGCRA(spec rl.Spec, o Options) *GCRALimiter {
	g := &GCRALimiter{clk: o.clock(), limit: spec.Limit, burst: spec.EffectiveBurst(), t: int64(spec.Window) / spec.Limit}
	g.tau = g.burst * g.t
	g.store = keystore.New(o.maxKeys(), func() *gcraState { return &gcraState{} })
	g.stop = g.store.RunJanitor(o.sweep(spec.Window), func(s *gcraState) bool {
		return s.tat.Load() <= g.clk.Now()
	})
	return g
}

func (g *GCRALimiter) Allow(_ context.Context, key string, cost int64) (rl.Decision, error) {
	d := rl.Decision{Limit: g.burst}
	if cost > g.burst {
		d.RetryAfter = time.Duration(g.tau)
		return d, nil
	}
	now := g.clk.Now()
	st := g.store.Get(key)
	inc := cost * g.t
	for {
		tat := st.tat.Load()
		base := maxI64(tat, now)
		newTat := base + inc
		if allowAt := newTat - g.tau; now < allowAt {
			d.RetryAfter = time.Duration(allowAt - now)
			d.Remaining = maxI64(0, (g.tau-(base-now))/g.t)
			d.ResetAfter = time.Duration(base - now)
			return d, nil
		}
		if st.tat.CompareAndSwap(tat, newTat) {
			d.Allowed = true
			d.Remaining = maxI64(0, (g.tau-(newTat-now))/g.t)
			d.ResetAfter = time.Duration(newTat - now)
			return d, nil
		}
	}
}

func (g *GCRALimiter) Refund(key string, cost int64) {
	if st := g.store.Peek(key); st != nil {
		for {
			tat := st.tat.Load()
			if st.tat.CompareAndSwap(tat, maxI64(0, tat-cost*g.t)) {
				return
			}
		}
	}
}

func (g *GCRALimiter) Close() error { g.stop(); return nil }
