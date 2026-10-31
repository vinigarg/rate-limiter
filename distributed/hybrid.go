package distributed

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/internal/keystore"
)

// HybridOptions tunes token leasing.
type HybridOptions struct {
	// LeaseSize is how many tokens a node withdraws from Redis at a time.
	// Worst-case over-admission is about Nodes x LeaseSize (independent of
	// request rate). Default: max(1, burst/10).
	LeaseSize int64
	// LeaseTTL: unspent leased tokens are discarded after this long so an idle
	// node does not hoard global budget. Default 1s.
	LeaseTTL time.Duration
	// LowWater triggers an asynchronous refill when local tokens drop below it.
	// Default LeaseSize/2.
	LowWater int64
	// RefillTimeout bounds each synchronous Redis call (the lease fast path
	// never touches the network). Default 50ms.
	RefillTimeout time.Duration
	Clock         rl.Clock
	MaxKeys       int
	Prefix        string
}

type leaseState struct {
	tokens    atomic.Int64
	expires   atomic.Int64 // clock ns; 0 = no lease
	denyUntil atomic.Int64 // central said "empty" until this time
	fetching  atomic.Bool
	fetchMu   sync.Mutex // serialises synchronous fetches per key
}

// Hybrid serves almost every request from a local lease (no network on the
// hot path) and talks to Redis only to refill: asynchronously when the lease
// runs low, synchronously only when it is empty. This is the "token leasing"
// point on the accuracy/latency spectrum.
type Hybrid struct {
	client Client
	spec   rl.Spec
	o      HybridOptions
	store  *keystore.Store[leaseState]
	rate   float64
	burst  int64
	stop   func()
}

// NewHybrid builds a leasing limiter on a Redis token bucket.
func NewHybrid(c Client, spec rl.Spec, o HybridOptions) (*Hybrid, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	h := &Hybrid{client: c, spec: spec, o: o, burst: spec.EffectiveBurst()}
	if h.o.LeaseSize <= 0 {
		h.o.LeaseSize = max(1, h.burst/10)
	}
	if h.o.LowWater <= 0 {
		h.o.LowWater = h.o.LeaseSize / 2
	}
	if h.o.LeaseTTL <= 0 {
		h.o.LeaseTTL = time.Second
	}
	if h.o.RefillTimeout <= 0 {
		h.o.RefillTimeout = 50 * time.Millisecond
	}
	if h.o.Clock == nil {
		h.o.Clock = rl.NewSystemClock()
	}
	if h.o.MaxKeys == 0 {
		h.o.MaxKeys = 1_000_000
	}
	h.rate = float64(spec.Limit) / (float64(spec.Window) / float64(time.Millisecond))
	h.store = keystore.New(h.o.MaxKeys, func() *leaseState { return &leaseState{} })
	h.stop = h.store.RunJanitor(5*time.Second, func(s *leaseState) bool {
		return s.tokens.Load() <= 0 && h.o.Clock.Now() > s.expires.Load() && !s.fetching.Load()
	})
	return h, nil
}

func (h *Hybrid) Allow(ctx context.Context, key string, cost int64) (rl.Decision, error) {
	d := rl.Decision{Limit: h.burst}
	if cost > h.burst {
		d.RetryAfter = h.spec.Window
		return d, nil
	}
	st := h.store.Get(key)
	now := h.o.Clock.Now()

	if exp := st.expires.Load(); exp != 0 && now > exp { // expired lease
		st.tokens.Store(0)
		st.expires.Store(0)
	}
	if left := st.tokens.Add(-cost); left >= 0 {
		if left < h.o.LowWater && st.fetching.CompareAndSwap(false, true) {
			go h.refill(key, st)
		}
		d.Allowed, d.Remaining = true, left
		return d, nil
	}
	st.tokens.Add(cost) // undo: not enough local tokens

	if du := st.denyUntil.Load(); now < du { // global budget known-empty
		d.RetryAfter = time.Duration(du - now)
		return d, nil
	}
	// Slow path: lease is empty, ask Redis synchronously (one caller per key).
	st.fetchMu.Lock()
	if left := st.tokens.Add(-cost); left >= 0 { // someone refilled while we waited
		st.fetchMu.Unlock()
		d.Allowed, d.Remaining = true, left
		return d, nil
	}
	st.tokens.Add(cost)
	fctx, cancel := context.WithTimeout(ctx, h.o.RefillTimeout) // budget only the network path
	err := h.fetchKey(fctx, key, st)
	cancel()
	st.fetchMu.Unlock()
	if err != nil {
		return rl.Decision{}, err
	}
	if left := st.tokens.Add(-cost); left >= 0 {
		d.Allowed, d.Remaining = true, left
		return d, nil
	}
	st.tokens.Add(cost)
	if du := st.denyUntil.Load(); du > now {
		d.RetryAfter = time.Duration(du - now)
	} else {
		d.RetryAfter = time.Millisecond
	}
	return d, nil
}

func (h *Hybrid) refill(key string, st *leaseState) {
	defer st.fetching.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), h.o.RefillTimeout)
	defer cancel()
	st.fetchMu.Lock()
	defer st.fetchMu.Unlock()
	_ = h.fetchKey(ctx, key, st) // async: errors surface via the sync path
}

// fetchKey withdraws a lease from Redis. Caller holds st.fetchMu.
func (h *Hybrid) fetchKey(ctx context.Context, key string, st *leaseState) error {
	res, err := h.client.Eval(ctx, leaseScript, []string{h.o.Prefix + "{" + key + "}"},
		h.rate, h.burst, h.o.LeaseSize)
	if err != nil {
		return err
	}
	now := h.o.Clock.Now()
	if res[0] > 0 {
		st.tokens.Add(res[0])
		st.expires.Store(now + int64(h.o.LeaseTTL))
		st.denyUntil.Store(0)
	} else {
		st.denyUntil.Store(now + res[1]*int64(time.Millisecond))
	}
	return nil
}

// Refund returns tokens to the local lease.
func (h *Hybrid) Refund(key string, cost int64) {
	if st := h.store.Peek(key); st != nil {
		st.tokens.Add(cost)
	}
}

func (h *Hybrid) Close() error { h.stop(); return nil }
