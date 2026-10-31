# Benchmarking design

Goal: prove the decision path stays in the **hundreds of nanoseconds locally** and that the distributed tiers behave as designed, with numbers you can trust and regress-test.

## Four layers

| Layer | Question | Tool | Command |
|---|---|---|---|
| 1. Micro | Cost of one algorithm decision, allocs, contention | `go test -bench` (`b.RunParallel`) | `make bench` |
| 2. Component | Engine: match + key + limiter + metrics; telemetry overhead | `engine/bench_test.go`, `telemetry` | `make bench` |
| 3. End to end | Throughput and tail latency under skewed keys; GC impact | `cmd/loadgen` closed + open loop | `make loadgen` |
| 4. Distributed | Central vs hybrid cost; correctness under outage | `distributed/bench_test.go`, loadgen `-scope`, chaos below | `REDIS_ADDR=... make bench` |

## Method

* **Parallel, not single-threaded.** `RunParallel` with `-cpu 1,4,8` exposes mutex/CAS contention; a single-core number flatters locks.
* **Matrix.** strategy × key cardinality (1 = hot key worst case for contention; 1k; 100k = cache-miss/map-lookup cost). State is pre-created so you measure steady state, not first-touch.
* **Skewed keys.** loadgen draws keys from a Zipf distribution (`-zipf 1.1`): real traffic has hot keys.
* **Never-deny limits** (`limit=1e9`) measure the allow path; small limits measure the deny path. Run both.
* **Statistics.** `-count=10` and `benchstat old.txt new.txt`; only trust deltas with low variance. Pin CPUs (`taskset`), disable turbo/frequency scaling, quiet the box.
* **Coordinated omission.** Closed loop (`-rate 0`) gives peak throughput but hides stalls. For SLO numbers use open loop (`-rate N`): latency is measured from the *intended* send time.
* **Allocations & GC.** `-benchmem` and loadgen's `allocs/op`, GC cycles and pause total. The hot path target is ≤1 alloc/op (the key string).
* **Profiles.** `make profile` → `go tool pprof -top cpu.out`; look for mutex wait, `maphash`, `time.Now`.

## Accuracy benchmarks (correctness is a performance dimension)

* Frozen-clock concurrency tests assert *exactly* `limit` admissions (no lost updates).
* Boundary-burst test shows fixed window admitting 2× across a boundary while sliding counter/log stay ≈ limit.
* Hybrid test bounds over-admission by `nodes × lease_size`.
* To measure *over-admission vs lease size* empirically: run N loadgens (`-scope hybrid`) against one Redis for a fixed window and compare total allowed with the limit.

## Chaos / failure benchmarks

Run loadgen at a steady `-rate` and, mid-run:

1. `redis-cli DEBUG SLEEP 5` or `docker pause redis`: expect `degraded` count to rise, p99 to stay near the Redis timeout budget (5 ms default) for the first calls, then drop to local latency once the breaker opens (no more Redis calls).
2. Unpause: breaker probes, closes; `degraded` stops growing.
3. Repeat per policy (`-on-error fail_open|fail_closed|fail_local`) and compare allowed%.

Pass criteria: no panics, no latency cliff beyond `RedisTimeout` + breaker open time, `fail_closed` denies, `fail_open` allows, `fail_local` approximates the configured fallback rate.

## Targets (initial SLOs to adopt, then ratchet with CI)

| Path | Target |
|---|---|
| Local allow (algo), per decision | < 250 ns at 100k keys |
| Engine.Check, 2 matching rules | < 500 ns, ≤ 2 allocs |
| Hybrid fast path | < 1 µs p99 (no network) |
| Central (Redis, same AZ) | ≈ 1 RTT; budget 0.5–1 ms p99 |
| Telemetry observe | < 20 ns, 0 allocs |

## Reference numbers (sandbox: 1 vCPU, Go 1.22, shared host, local Redis)

These show shape, not production capacity. Re-run on your hardware.

| Benchmark | Result |
|---|---|
| algo Allow, 1 key | 68–109 ns/op, 0 allocs |
| algo Allow, 100k keys | 129–255 ns/op, 0 allocs |
| Engine.Check (2 rules, 100k keys) | ≈300–370 ns/op sampled latency; 2 allocs (one key string per matched rule) |
| Hybrid via loadgen, 1k keys | 2.06 M ops/s, p50 256 ns, p99 640 ns |
| Central via Redis (local loopback) | ≈38 µs/op, 25 allocs (client allocations dominate) |
| Open loop 100k ops/s (local) | p50 512 ns, p99 ≈ 27 µs, p99.9 ≈ 2 ms (single shared core: scheduler noise) |
