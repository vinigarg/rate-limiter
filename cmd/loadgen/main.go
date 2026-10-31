// loadgen drives the engine in-process with a realistic key distribution and
// reports throughput and latency percentiles.
//
// Closed loop (-rate 0): every worker issues the next call as soon as the
// previous returns. Great for peak throughput, but it HIDES queueing: when the
// system stalls, the workers stall too and stop measuring (coordinated omission).
//
// Open loop (-rate N): calls are scheduled at fixed intended times and latency
// is measured from the INTENDED start, so a stall shows up as the latency every
// delayed call would have seen. Use this for SLO numbers (p99, p99.9).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"context"

	"github.com/yourorg/ratelimiter/distributed"
	"github.com/yourorg/ratelimiter/engine"
)

// log-linear histogram: 8 sub-buckets per power of two (~12% resolution), ns.
type hist struct{ b [8 * 48]uint64 }

func idx(v uint64) int {
	if v < 8 {
		return int(v)
	}
	e := bits.Len64(v) - 1
	return e*8 + int((v>>(uint(e)-3))&7)
}
func lower(i int) uint64 {
	if i < 8 {
		return uint64(i)
	}
	e, s := i/8, uint64(i%8)
	return (8 + s) << uint(e-3)
}
func (h *hist) add(ns int64) {
	if ns < 0 {
		ns = 0
	}
	h.b[idx(uint64(ns))]++
}
func (h *hist) merge(o *hist) {
	for i := range h.b {
		h.b[i] += o.b[i]
	}
}
func (h *hist) quantile(q float64) time.Duration {
	var total uint64
	for _, c := range h.b {
		total += c
	}
	target := uint64(float64(total) * q)
	var cum uint64
	for i, c := range h.b {
		cum += c
		if cum > target {
			return time.Duration(lower(i))
		}
	}
	return 0
}
func (h *hist) max() time.Duration {
	for i := len(h.b) - 1; i >= 0; i-- {
		if h.b[i] > 0 {
			return time.Duration(lower(i))
		}
	}
	return 0
}

func main() {
	var (
		dur      = flag.Duration("duration", 10*time.Second, "measurement duration")
		warm     = flag.Duration("warmup", 2*time.Second, "warmup (not measured)")
		workers  = flag.Int("workers", runtime.GOMAXPROCS(0), "concurrent workers")
		keys     = flag.Int("keys", 100_000, "distinct api keys")
		zipfS    = flag.Float64("zipf", 1.1, "zipf skew s>1 (hot keys); 0 = uniform")
		rate     = flag.Float64("rate", 0, "total target ops/s (open loop); 0 = closed loop")
		strategy = flag.String("strategy", "gcra", "strategy")
		limit    = flag.Int64("limit", 1_000_000_000, "limit per window (huge = measure allow path; small = deny path)")
		window   = flag.String("window", "1s", "window")
		scope    = flag.String("scope", "local", "local|hybrid|global")
		redis    = flag.String("redis", "127.0.0.1:6379", "redis addr for hybrid/global")
		onError  = flag.String("on-error", "fail_local", "fail_open|fail_closed|fail_local")
		asJSON   = flag.Bool("json", false, "machine-readable output")
	)
	flag.Parse()

	rule := fmt.Sprintf(`{"rules":[{"id":"bench","match":{"path_prefix":"/v1/"},"key_by":["api_key"],
	  "strategy":%q,"scope":%q,"on_error":%q,"limits":[{"limit":%d,"window":%q}]}]}`,
		*strategy, *scope, *onError, *limit, *window)
	cfg, err := engine.ParseConfig([]byte(rule))
	must(err)
	opts := engine.Options{}
	if *scope != "local" {
		opts.Redis = distributed.NewRESPClient(*redis)
	}
	e := engine.New(opts)
	must(e.Reload(cfg))
	defer e.Close()

	pool := make([]*engine.Request, *keys)
	for i := range pool {
		pool[i] = &engine.Request{APIKey: "key-" + strconv.Itoa(i), Path: "/v1/items", Method: "GET"}
	}

	var stopFlag, measuring atomic.Bool
	hists := make([]hist, *workers)
	var ops, allowed [64]atomic.Uint64 // coarse stripes by worker
	perWorkerInterval := time.Duration(0)
	if *rate > 0 {
		perWorkerInterval = time.Duration(float64(time.Second) * float64(*workers) / *rate)
	}

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w)+1, 42))
			var zipf *rand.Zipf
			if *zipfS > 1 {
				zipf = rand.NewZipf(rng, *zipfS, 1, uint64(*keys-1))
			}
			ctx := context.Background()
			next := time.Now()
			var lops, lallowed uint64
			for !stopFlag.Load() {
				var k int
				if zipf != nil {
					k = int(zipf.Uint64())
				} else {
					k = rng.IntN(*keys)
				}
				start := time.Now()
				if perWorkerInterval > 0 {
					next = next.Add(perWorkerInterval)
					for d := time.Until(next); d > 0; d = time.Until(next) {
						if d > 100*time.Microsecond {
							time.Sleep(d - 50*time.Microsecond)
						} else {
							runtime.Gosched() // never hog a core while waiting
						}
					}
					start = next // measure from the INTENDED start
				}
				res := e.Check(ctx, pool[k])
				if measuring.Load() {
					hists[w].add(int64(time.Since(start)))
					lops++
					if res.Allowed {
						lallowed++
					}
				}
			}
			ops[w%64].Add(lops)
			allowed[w%64].Add(lallowed)
		}(w)
	}

	time.Sleep(*warm)
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	t0 := time.Now()
	measuring.Store(true)
	time.Sleep(*dur)
	measuring.Store(false)
	elapsed := time.Since(t0)
	runtime.ReadMemStats(&m1)
	stopFlag.Store(true)
	wg.Wait()

	var total hist
	for i := range hists {
		total.merge(&hists[i])
	}
	var nOps, nAllowed uint64
	for i := range ops {
		nOps += ops[i].Load()
		nAllowed += allowed[i].Load()
	}
	out := map[string]any{
		"mode": map[bool]string{true: "open-loop", false: "closed-loop"}[*rate > 0], "strategy": *strategy, "scope": *scope,
		"workers": *workers, "keys": *keys, "zipf": *zipfS, "gomaxprocs": runtime.GOMAXPROCS(0),
		"ops": nOps, "ops_per_sec": float64(nOps) / elapsed.Seconds(),
		"allowed_pct": 100 * float64(nAllowed) / float64(max(nOps, 1)),
		"p50_ns":      total.quantile(0.50).Nanoseconds(), "p90_ns": total.quantile(0.90).Nanoseconds(),
		"p99_ns": total.quantile(0.99).Nanoseconds(), "p999_ns": total.quantile(0.999).Nanoseconds(), "max_ns": total.max().Nanoseconds(),
		"allocs_per_op": float64(m1.Mallocs-m0.Mallocs) / float64(max(nOps, 1)),
		"gc_cycles":     m1.NumGC - m0.NumGC,
		"gc_pause_ms":   float64(m1.PauseTotalNs-m0.PauseTotalNs) / 1e6,
	}
	if *asJSON {
		json.NewEncoder(os.Stdout).Encode(out)
		return
	}
	keysSorted := make([]string, 0, len(out))
	for k := range out {
		keysSorted = append(keysSorted, k)
	}
	sort.Strings(keysSorted)
	fmt.Printf("%-14s %v\n", "mode", out["mode"])
	fmt.Printf("%-14s %s / %s  workers=%d keys=%d zipf=%.2f GOMAXPROCS=%d\n", "config", *strategy, *scope, *workers, *keys, *zipfS, runtime.GOMAXPROCS(0))
	fmt.Printf("%-14s %.0f ops/s (%d ops, %.1f%% allowed)\n", "throughput", out["ops_per_sec"], nOps, out["allowed_pct"])
	fmt.Printf("%-14s p50=%v p90=%v p99=%v p99.9=%v max=%v\n", "latency",
		total.quantile(0.5), total.quantile(0.9), total.quantile(0.99), total.quantile(0.999), total.max())
	fmt.Printf("%-14s %.2f allocs/op, %d GC cycles, %.2fms total pause\n", "runtime", out["allocs_per_op"], m1.NumGC-m0.NumGC, out["gc_pause_ms"])
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
