package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/algo"
)

type flaky struct {
	fail  atomic.Bool
	calls atomic.Int64
	slow  time.Duration
}

func (f *flaky) Allow(ctx context.Context, _ string, _ int64) (rl.Decision, error) {
	f.calls.Add(1)
	if f.slow > 0 {
		select {
		case <-time.After(f.slow):
		case <-ctx.Done():
			return rl.Decision{}, ctx.Err()
		}
	}
	if f.fail.Load() {
		return rl.Decision{}, errors.New("redis down")
	}
	return rl.Decision{Allowed: true, Limit: 10, Remaining: 9}, nil
}

func TestPolicies(t *testing.T) {
	ctx := context.Background()
	clk := rl.NewManualClock()
	local, _ := algo.New(algo.TokenBucketName, rl.Spec{Limit: 2, Window: time.Hour}, algo.Options{Clock: clk})
	defer local.Close()

	cases := []struct {
		name   string
		policy Policy
		want   []bool // successive decisions while primary is down
	}{
		{"open", FailOpen, []bool{true, true, true}},
		{"closed", FailClosed, []bool{false, false, false}},
		{"local", FailLocal, []bool{true, true, false}}, // backstop allows 2 then denies
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &flaky{}
			f.fail.Store(true)
			lc, _ := algo.New(algo.TokenBucketName, rl.Spec{Limit: 2, Window: time.Hour}, algo.Options{Clock: clk})
			g := NewGuard(f, lc, Config{Policy: c.policy, Clock: clk, FailureThreshold: 100})
			for i, want := range c.want {
				d, err := g.Allow(ctx, "k", 1)
				if err != nil || d.Allowed != want || !d.Degraded {
					t.Fatalf("call %d: %+v err=%v, want allowed=%v degraded", i, d, err, want)
				}
			}
		})
	}
}

func TestBreakerOpensProbesAndCloses(t *testing.T) {
	ctx := context.Background()
	clk := rl.NewManualClock()
	f := &flaky{}
	f.fail.Store(true)
	g := NewGuard(f, nil, Config{Policy: FailOpen, Clock: clk, FailureThreshold: 3, Cooldown: time.Second})

	for i := 0; i < 3; i++ {
		g.Allow(ctx, "k", 1)
	}
	if !g.BreakerOpen() {
		t.Fatal("breaker should be open after 3 failures")
	}
	before := f.calls.Load()
	for i := 0; i < 50; i++ {
		g.Allow(ctx, "k", 1)
	}
	if f.calls.Load() != before {
		t.Fatal("open breaker must not call the primary")
	}
	clk.Advance(1100 * time.Millisecond)
	f.fail.Store(false)
	if d, _ := g.Allow(ctx, "k", 1); d.Degraded {
		t.Fatalf("probe should reach the recovered primary: %+v", d)
	}
	if g.BreakerOpen() {
		t.Fatal("breaker should close after a successful probe")
	}
}

func TestTimeoutTriggersFallback(t *testing.T) {
	f := &flaky{slow: 200 * time.Millisecond}
	g := NewGuard(f, nil, Config{Policy: FailClosed, Timeout: 5 * time.Millisecond})
	start := time.Now()
	d, _ := g.Allow(context.Background(), "k", 1)
	if d.Allowed || !d.Degraded {
		t.Fatalf("want fail-closed degraded deny, got %+v", d)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("timeout not enforced")
	}
}

func TestParsePolicy(t *testing.T) {
	if p, err := ParsePolicy("fail_local"); err != nil || p != FailLocal {
		t.Fatal(p, err)
	}
	if _, err := ParsePolicy("x"); err == nil {
		t.Fatal("want error")
	}
}
