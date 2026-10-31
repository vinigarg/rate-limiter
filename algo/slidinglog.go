package algo

import (
	"context"
	"sync"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/internal/keystore"
)

type swlState struct {
	mu   sync.Mutex
	ts   []int64 // admitted unit timestamps, oldest first from index head
	head int
}

func (s *swlState) live() int { return len(s.ts) - s.head }

func (s *swlState) trim(cutoff int64) {
	for s.head < len(s.ts) && s.ts[s.head] <= cutoff {
		s.head++
	}
	if s.head > 0 && s.head >= len(s.ts)/2 { // amortised compaction
		n := copy(s.ts, s.ts[s.head:])
		s.ts, s.head = s.ts[:n], 0
	}
}

// SlidingWindowLogLimiter stores one timestamp per admitted unit: exact, but
// O(limit) memory per key. Use for billing-critical limits with small limits.
type SlidingWindowLogLimiter struct {
	clk    rl.Clock
	limit  int64
	window int64
	store  *keystore.Store[swlState]
	stop   func()
}

func NewSlidingWindowLog(spec rl.Spec, o Options) *SlidingWindowLogLimiter {
	l := &SlidingWindowLogLimiter{clk: o.clock(), limit: spec.Limit, window: int64(spec.Window)}
	l.store = keystore.New(o.maxKeys(), func() *swlState { return &swlState{} })
	l.stop = l.store.RunJanitor(o.sweep(spec.Window), func(st *swlState) bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		st.trim(l.clk.Now() - l.window)
		return st.live() == 0
	})
	return l
}

func (l *SlidingWindowLogLimiter) Allow(_ context.Context, key string, cost int64) (rl.Decision, error) {
	now := l.clk.Now()
	d := rl.Decision{Limit: l.limit}
	if cost > l.limit {
		d.RetryAfter = time.Duration(l.window)
		return d, nil
	}
	st := l.store.Get(key)
	st.mu.Lock()
	st.trim(now - l.window)
	n := int64(st.live())
	if n+cost <= l.limit {
		for i := int64(0); i < cost; i++ {
			st.ts = append(st.ts, now)
		}
		n += cost
		d.Allowed = true
	} else {
		// wait until enough of the oldest entries age out of the window
		need := n + cost - l.limit
		d.RetryAfter = time.Duration(st.ts[st.head+int(need)-1] + l.window - now)
	}
	d.Remaining = l.limit - n
	if st.live() > 0 {
		d.ResetAfter = time.Duration(st.ts[len(st.ts)-1] + l.window - now)
	}
	st.mu.Unlock()
	return d, nil
}

func (l *SlidingWindowLogLimiter) Refund(key string, cost int64) {
	if st := l.store.Peek(key); st != nil {
		st.mu.Lock()
		n := int64(st.live())
		if cost > n {
			cost = n
		}
		st.ts = st.ts[:len(st.ts)-int(cost)]
		st.mu.Unlock()
	}
}

func (l *SlidingWindowLogLimiter) Close() error { l.stop(); return nil }
