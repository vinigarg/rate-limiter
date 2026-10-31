package algo

import (
	"context"
	"sync"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/internal/keystore"
)

type lbState struct {
	mu  sync.Mutex
	nft int64 // time the next request slot becomes free
}

// LeakyBucketLimiter is the queue (shaping) variant: requests join a bucket
// that drains at Limit/Window; capacity is Burst slots. An admitted request
// carries Decision.Delay: how long until its slot, so the caller can release
// traffic downstream at a steady rate. Denied when the queue is full.
type LeakyBucketLimiter struct {
	clk      rl.Clock
	limit    int64
	capacity int64
	interval int64 // ns per slot
	store    *keystore.Store[lbState]
	stop     func()
}

func NewLeakyBucket(spec rl.Spec, o Options) *LeakyBucketLimiter {
	l := &LeakyBucketLimiter{
		clk: o.clock(), limit: spec.Limit, capacity: spec.EffectiveBurst(),
		interval: int64(spec.Window) / spec.Limit,
	}
	l.store = keystore.New(o.maxKeys(), func() *lbState { return &lbState{} })
	l.stop = l.store.RunJanitor(o.sweep(spec.Window), func(s *lbState) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.nft <= l.clk.Now()
	})
	return l
}

func (l *LeakyBucketLimiter) Allow(_ context.Context, key string, cost int64) (rl.Decision, error) {
	now := l.clk.Now()
	d := rl.Decision{Limit: l.capacity}
	if cost > l.capacity {
		d.RetryAfter = time.Duration(l.capacity * l.interval)
		return d, nil
	}
	st := l.store.Get(key)
	st.mu.Lock()
	base := maxI64(st.nft, now)
	queued := (base - now) / l.interval
	if queued+cost <= l.capacity {
		st.nft = base + cost*l.interval
		d.Allowed = true
		d.Delay = time.Duration(base - now)
		queued += cost
	} else {
		d.RetryAfter = time.Duration((queued + cost - l.capacity) * l.interval)
	}
	d.Remaining = l.capacity - queued
	d.ResetAfter = time.Duration(maxI64(st.nft-now, 0))
	st.mu.Unlock()
	return d, nil
}

func (l *LeakyBucketLimiter) Refund(key string, cost int64) {
	if st := l.store.Peek(key); st != nil {
		st.mu.Lock()
		st.nft -= cost * l.interval
		st.mu.Unlock()
	}
}

func (l *LeakyBucketLimiter) Close() error { l.stop(); return nil }
