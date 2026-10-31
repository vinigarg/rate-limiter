package distributed

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

var bg = context.Background()

// redisAddr returns a Redis to test against: $REDIS_ADDR, or a throwaway
// redis-server started from PATH. Tests skip when neither is available.
func redisAddr(t testing.TB) string {
	t.Helper()
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	path, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("no redis available (set REDIS_ADDR or install redis-server)")
	}
	addr := "127.0.0.1:16399"
	cmd := exec.Command(path, "--port", "16399", "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Skip("cannot start redis-server:", err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	c := NewRESPClient(addr)
	for i := 0; i < 50; i++ {
		if _, err := c.Eval(bg, NewScript("return {1}"), nil); err == nil {
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Skip("redis-server did not become ready")
	return ""
}

func TestCentralStrategies(t *testing.T) {
	c := NewRESPClient(redisAddr(t))
	for _, strat := range []string{TokenBucket, SlidingWindowCounter, FixedWindow} {
		t.Run(strat, func(t *testing.T) {
			l, err := NewCentral(c, strat, rl.Spec{Limit: 10, Window: time.Minute}, "t:"+strat+":")
			if err != nil {
				t.Fatal(err)
			}
			allowed := 0
			for i := 0; i < 25; i++ {
				d, err := l.Allow(bg, "k", 1)
				if err != nil {
					t.Fatal(err)
				}
				if d.Allowed {
					allowed++
				}
			}
			if allowed != 10 {
				t.Fatalf("allowed %d, want 10", allowed)
			}
			d, _ := l.Allow(bg, "k", 1)
			if d.Allowed || d.RetryAfter <= 0 {
				t.Fatalf("want denied with RetryAfter, got %+v", d)
			}
		})
	}
}

// Many goroutines, one key: Lua atomicity means exactly `limit` admissions.
func TestCentralAtomicUnderConcurrency(t *testing.T) {
	c := NewRESPClient(redisAddr(t))
	l, _ := NewCentral(c, SlidingWindowCounter, rl.Spec{Limit: 200, Window: time.Hour}, "t:conc:")
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if d, err := l.Allow(bg, "hot", 1); err == nil && d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 200 {
		t.Fatalf("admitted %d, want exactly 200", allowed.Load())
	}
}

// Three "nodes" share one global budget. Over-admission is bounded by
// nodes x LeaseSize regardless of how hard we push.
func TestHybridBoundedOverAdmission(t *testing.T) {
	c := NewRESPClient(redisAddr(t))
	const limit, lease, nodes = 300, 20, 3
	spec := rl.Spec{Limit: limit, Window: time.Hour} // ~no refill during the test
	var hs []*Hybrid
	for i := 0; i < nodes; i++ {
		h, err := NewHybrid(c, spec, HybridOptions{LeaseSize: lease, Prefix: "t:hyb:", LeaseTTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		hs = append(hs, h)
	}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for _, h := range hs {
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(h *Hybrid) {
				defer wg.Done()
				for i := 0; i < 400; i++ {
					if d, err := h.Allow(bg, "tenant-1", 1); err == nil && d.Allowed {
						allowed.Add(1)
					}
				}
			}(h)
		}
	}
	wg.Wait()
	got := allowed.Load()
	if got > limit {
		t.Fatalf("admitted %d > global limit %d (leases only draw from the global bucket)", got, limit)
	}
	if got < limit-nodes*lease { // unspent leases held by nodes
		t.Fatalf("admitted only %d, expected close to %d", got, limit)
	}
}

func TestHybridLocalHotPathNoNetwork(t *testing.T) {
	var calls atomic.Int64
	h, _ := NewHybrid(countingClient{&calls}, rl.Spec{Limit: 1000, Window: time.Second},
		HybridOptions{LeaseSize: 500, LowWater: 1, LeaseTTL: time.Minute})
	defer h.Close()
	for i := 0; i < 400; i++ {
		if d, err := h.Allow(bg, "k", 1); err != nil || !d.Allowed {
			t.Fatalf("req %d: %+v %v", i, d, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("redis calls = %d, want 1 (a single lease serves 400 requests)", calls.Load())
	}
}

type countingClient struct{ n *atomic.Int64 }

func (c countingClient) Eval(_ context.Context, s *Script, _ []string, args ...any) ([]int64, error) {
	c.n.Add(1)
	return []int64{500, 0}, nil // grant a full lease
}
