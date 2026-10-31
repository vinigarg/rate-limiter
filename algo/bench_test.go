package algo

import (
	"strconv"
	"testing"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

// Matrix: strategy x key-cardinality, run in parallel (b.RunParallel) so the
// numbers reflect lock/CAS contention, not just single-core arithmetic.
//
//	go test ./algo -bench=Allow -benchmem -cpu=1,4,8 -count=10 | tee new.txt
//	benchstat old.txt new.txt
func BenchmarkAllow(b *testing.B) {
	spec := rl.Spec{Limit: 1_000_000_000, Window: time.Second} // never denies: measure the hot path
	for _, s := range Strategies {
		if s == SlidingWindowLog {
			spec.Limit = 1000 // log is O(limit); keep memory sane
		} else {
			spec.Limit = 1_000_000_000
		}
		for _, keys := range []int{1, 1_000, 100_000} {
			b.Run(s+"/keys="+strconv.Itoa(keys), func(b *testing.B) {
				l, _ := New(s, spec, Options{})
				defer l.Close()
				ks := make([]string, keys)
				for i := range ks {
					ks[i] = "client-" + strconv.Itoa(i)
					l.Allow(ctx, ks[i], 1) // pre-create state: measure steady state
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						l.Allow(ctx, ks[i%keys], 1)
						i++
					}
				})
			})
		}
	}
}

func BenchmarkMultiTwoWindows(b *testing.B) {
	m := NewMulti(
		NewGCRA(rl.Spec{Limit: 1_000_000_000, Window: time.Second}, Options{}),
		NewSlidingWindowCounter(rl.Spec{Limit: 1_000_000_000, Window: time.Hour}, Options{}),
	)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Allow(ctx, "k", 1)
		}
	})
}
