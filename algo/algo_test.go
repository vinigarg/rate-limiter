package algo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

var ctx = context.Background()

func mk(t testing.TB, strat string, spec rl.Spec, clk rl.Clock) Local {
	t.Helper()
	l, err := New(strat, spec, Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func count(l rl.Limiter, key string, n int) (allowed int) {
	for i := 0; i < n; i++ {
		if d, _ := l.Allow(ctx, key, 1); d.Allowed {
			allowed++
		}
	}
	return
}

// Every strategy must admit exactly Limit requests in a burst at t0, deny the
// next one with a positive RetryAfter, and admit again after the window.
func TestAllStrategiesBasics(t *testing.T) {
	for _, s := range Strategies {
		t.Run(s, func(t *testing.T) {
			clk := rl.NewManualClock()
			l := mk(t, s, rl.Spec{Limit: 10, Window: time.Second}, clk)
			if got := count(l, "k", 10); got != 10 {
				t.Fatalf("allowed %d of first 10, want 10", got)
			}
			d, _ := l.Allow(ctx, "k", 1)
			if d.Allowed || d.RetryAfter <= 0 {
				t.Fatalf("11th request: %+v, want denied with RetryAfter>0", d)
			}
			if count(l, "other", 10) != 10 {
				t.Fatal("keys must be independent")
			}
			clk.Advance(2 * time.Second)
			if got := count(l, "k", 10); got != 10 {
				t.Fatalf("after window allowed %d, want 10", got)
			}
		})
	}
}

// Honouring RetryAfter must lead to an allowed request.
func TestRetryAfterIsHonest(t *testing.T) {
	for _, s := range Strategies {
		t.Run(s, func(t *testing.T) {
			clk := rl.NewManualClock()
			l := mk(t, s, rl.Spec{Limit: 10, Window: time.Second}, clk)
			count(l, "k", 10)
			d, _ := l.Allow(ctx, "k", 1)
			clk.Advance(d.RetryAfter + time.Millisecond)
			if d2, _ := l.Allow(ctx, "k", 1); !d2.Allowed {
				t.Fatalf("still denied after RetryAfter=%v: %+v", d.RetryAfter, d2)
			}
		})
	}
}

func TestBoundaryBurst(t *testing.T) {
	// 10 requests at the end of window N, 10 at the start of N+1.
	run := func(s string) int {
		clk := rl.NewManualClock()
		l := mk(t, s, rl.Spec{Limit: 10, Window: time.Second}, clk)
		w := int64(time.Second)
		clk.Advance(time.Duration(w - clk.Now()%w - int64(10*time.Millisecond))) // 10ms before boundary
		a := count(l, "k", 10)
		clk.Advance(20 * time.Millisecond) // 10ms after boundary
		return a + count(l, "k", 10)
	}
	if got := run(FixedWindow); got != 20 {
		t.Fatalf("fixed window admitted %d, expected the 2x boundary burst (20)", got)
	}
	for _, s := range []string{SlidingWindowCounter, SlidingWindowLog} {
		if got := run(s); got > 11 {
			t.Fatalf("%s admitted %d across the boundary, want ~10", s, got)
		}
	}
}

func TestTokenBucketBurstThenRate(t *testing.T) {
	clk := rl.NewManualClock()
	l := mk(t, TokenBucketName, rl.Spec{Limit: 10, Window: time.Second, Burst: 50}, clk)
	if got := count(l, "k", 100); got != 50 {
		t.Fatalf("burst admitted %d, want 50", got)
	}
	clk.Advance(500 * time.Millisecond) // refill 5
	if got := count(l, "k", 100); got != 5 {
		t.Fatalf("refill admitted %d, want 5", got)
	}
}

func TestLeakyBucketDelay(t *testing.T) {
	clk := rl.NewManualClock()
	l := mk(t, LeakyBucketName, rl.Spec{Limit: 10, Window: time.Second, Burst: 5}, clk)
	var last time.Duration
	for i := 0; i < 5; i++ {
		d, _ := l.Allow(ctx, "k", 1)
		if !d.Allowed || d.Delay != time.Duration(i)*100*time.Millisecond {
			t.Fatalf("req %d: %+v, want delay %v", i, d, time.Duration(i)*100*time.Millisecond)
		}
		last = d.Delay
	}
	if d, _ := l.Allow(ctx, "k", 1); d.Allowed {
		t.Fatalf("queue full (last delay %v) but admitted", last)
	}
}

func TestCostAndOversize(t *testing.T) {
	for _, s := range Strategies {
		clk := rl.NewManualClock()
		l := mk(t, s, rl.Spec{Limit: 10, Window: time.Second}, clk)
		if d, _ := l.Allow(ctx, "k", 7); !d.Allowed || d.Remaining != 3 {
			t.Fatalf("%s cost 7: %+v", s, d)
		}
		if d, _ := l.Allow(ctx, "k", 4); d.Allowed {
			t.Fatalf("%s cost 4 over remaining 3 admitted", s)
		}
		if d, _ := l.Allow(ctx, "k", 11); d.Allowed {
			t.Fatalf("%s cost 11 > limit admitted", s)
		}
	}
}

// Concurrency/accuracy: with a frozen clock nobody refills, so the number of
// admitted requests must be exactly the limit - never more (no lost updates).
func TestConcurrentExactness(t *testing.T) {
	for _, s := range Strategies {
		t.Run(s, func(t *testing.T) {
			clk := rl.NewManualClock()
			l := mk(t, s, rl.Spec{Limit: 1000, Window: time.Minute}, clk)
			var allowed atomic.Int64
			var wg sync.WaitGroup
			for g := 0; g < 16; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 500; i++ {
						if d, _ := l.Allow(ctx, "hot", 1); d.Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			if allowed.Load() != 1000 {
				t.Fatalf("admitted %d, want exactly 1000", allowed.Load())
			}
		})
	}
}

func TestMultiRefundsOnDeny(t *testing.T) {
	clk := rl.NewManualClock()
	perSec := mk(t, TokenBucketName, rl.Spec{Limit: 5, Window: time.Second}, clk)
	perDay := mk(t, SlidingWindowCounter, rl.Spec{Limit: 8, Window: 24 * time.Hour}, clk)
	m := NewMulti(perSec, perDay)
	if got := count(m, "k", 20); got != 5 {
		t.Fatalf("admitted %d, want 5 (per-second binds)", got)
	}
	clk.Advance(time.Second)
	// the 15 rejected requests must not have consumed the daily budget
	if got := count(m, "k", 20); got != 3 {
		t.Fatalf("second burst admitted %d, want 3 (8 daily - 5 used)", got)
	}
	d, _ := m.Allow(ctx, "k", 1)
	if d.Allowed || d.RetryAfter < time.Hour {
		t.Fatalf("daily limit binding: %+v, want RetryAfter of hours", d)
	}
}

func TestKeyCapOverflowSharesBucket(t *testing.T) {
	clk := rl.NewManualClock()
	l, _ := New(TokenBucketName, rl.Spec{Limit: 5, Window: time.Hour}, Options{Clock: clk, MaxKeys: 64})
	defer l.Close()
	total := 0
	for i := 0; i < 10000; i++ { // rotating keys must not allocate unbounded state
		if d, _ := l.Allow(ctx, string(rune('a'+i%26))+time.Duration(i).String(), 1); d.Allowed {
			total++
		}
	}
	if tb := l.(*TokenBucketLimiter); tb.store.Len() > 128 {
		t.Fatalf("store grew to %d keys with cap 64", tb.store.Len())
	}
	if total > 64*5+5 {
		t.Fatalf("rotating keys admitted %d, overflow bucket not shared", total)
	}
}

func TestSweepEvictsIdle(t *testing.T) {
	clk := rl.NewManualClock()
	l := NewGCRA(rl.Spec{Limit: 10, Window: time.Second}, Options{Clock: clk, SweepInterval: time.Hour})
	defer l.Close()
	l.Allow(ctx, "a", 1)
	l.Allow(ctx, "b", 1)
	clk.Advance(time.Minute)
	if n := l.store.Sweep(func(s *gcraState) bool { return s.tat.Load() <= clk.Now() }); n != 2 {
		t.Fatalf("swept %d, want 2", n)
	}
}

func TestInvalidSpec(t *testing.T) {
	if _, err := New(GCRAName, rl.Spec{}, Options{}); err == nil {
		t.Fatal("want error")
	}
	if _, err := New("nope", rl.Spec{Limit: 1, Window: time.Second}, Options{}); err == nil {
		t.Fatal("want unknown strategy error")
	}
}
