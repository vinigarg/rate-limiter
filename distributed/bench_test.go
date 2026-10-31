package distributed

import (
	"testing"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

// Compares central (1 RTT per decision) with hybrid (lease: ~0 RTT).
// Run against a Redis in the same AZ/host as production for meaningful numbers.
func BenchmarkCentralVsHybrid(b *testing.B) {
	c := NewRESPClient(redisAddr(b))
	spec := rl.Spec{Limit: 1_000_000_000, Window: time.Second}
	b.Run("central/sliding_window_counter", func(b *testing.B) {
		l, _ := NewCentral(c, SlidingWindowCounter, spec, "bench:c:")
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				l.Allow(bg, "k", 1)
			}
		})
	})
	b.Run("hybrid/lease=1000", func(b *testing.B) {
		h, _ := NewHybrid(c, spec, HybridOptions{LeaseSize: 1000, Prefix: "bench:h:"})
		defer h.Close()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				h.Allow(bg, "k", 1)
			}
		})
	})
}
