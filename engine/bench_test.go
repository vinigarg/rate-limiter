package engine

import (
	"strconv"
	"testing"
)

// End-to-end local decision: match -> key build -> limiter -> metrics.
//
//	go test ./engine -bench=Check -benchmem -cpu=1,4,8 -count=10
func BenchmarkCheck(b *testing.B) {
	for _, strat := range []string{"gcra", "token_bucket", "sliding_window_counter"} {
		for _, sample := range []string{"1", "16"} {
			b.Run(strat+"/sampleEvery="+sample, func(b *testing.B) {
				js := `{"rules":[
				 {"id":"api","priority":5,"match":{"path_prefix":"/v1/","attrs":{"tier":"free"}},"key_by":["api_key"],
				  "strategy":"` + strat + `","limits":[{"limit":1000000000,"window":"1s"}]},
				 {"id":"ip","priority":1,"key_by":["ip"],"strategy":"gcra","limits":[{"limit":1000000000,"window":"1s"}]}]}`
				e, _ := newEngine(b, js, Options{})
				n, _ := strconv.Atoi(sample)
				e.mask = uint32(n - 1)
				reqs := make([]*Request, 1024)
				for i := range reqs {
					reqs[i] = &Request{APIKey: "key-" + strconv.Itoa(i), IP: "10.0.0." + strconv.Itoa(i%250), Path: "/v1/items", Tier: "free", Method: "GET"}
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						e.Check(bg, reqs[i&1023])
						i++
					}
				})
			})
		}
	}
}
