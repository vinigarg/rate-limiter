# Architecture

Maps the reference design (System Design School, *Distributed Rate Limiter*, §5 HLD and §6 deep dives) onto this repo.

```
 client ─► gateway/app ─► httpmw / limiterd ─► engine.Check ──► rule snapshot (atomic.Pointer, lock-free reads)
                                                   │ match → key_by → limiter
                          ┌────────────────────────┼─────────────────────────────┐
                          ▼ scope=local            ▼ scope=hybrid                ▼ scope=global
                   algo.* (in-process,       distributed.Hybrid             distributed.Central
                   sharded, capped)          local lease, async refill      1 atomic Lua script / decision
                                                   │                              │
                                                   └──── resilience.Guard ────────┘
                                                   timeout + circuit breaker + policy
                                                   fail_open | fail_closed | fail_local ──► algo.* backstop
                          telemetry.RuleMetrics (striped atomics) ─► Flusher ─► OTLP/HTTP ─► OTel Collector
                                                                  └─► /metrics (Prometheus)
 config file ─► engine.WatchFile ─► validate+compile off to the side ─► atomic swap (bad config = no-op)
```

| Reference design | Here |
|---|---|
| Counter in the app server (§5.1) | `scope: local` + `algo/` |
| Shared store (§5.2), atomic script (§5.3) | `distributed/scripts.go`, one Lua script per decision, TIME from Redis |
| Replica/failover (§5.4) | Redis concern; the client is a one-method interface (`distributed.Client`) |
| Config store refreshed off the hot path | `engine.WatchFile` + `Reload` |
| Telemetry async, aggregated | `telemetry/` striped counters, flush every N seconds |
| Multiple windows per key (§6.1) | `limits: [...]`, `algo.Multi` (with refunds) |
| Hybrid / token leasing (§6.2) | `distributed.Hybrid`, over-admission ≤ nodes × lease_size |
| Two-tier fail-open with in-process backstop (§6.3) | `resilience.Guard` + `on_error: fail_local` |
| Jittered Retry-After (§6.3) | `httpmw` |
| Shadow mode (§6.4) | `shadow: true` |
| Hot key | hybrid leases keep hot keys off Redis; sub-counters not implemented (see below) |
| Limiter returns 200 + verdict, gateway returns 429 (§3) | `cmd/limiterd` |

## Choosing algorithm / scope per rule

| Need | strategy | scope | on_error |
|---|---|---|---|
| Default API limit, bursts OK | `gcra` or `token_bucket` | `local` or `hybrid` | `fail_local` |
| Smooth, no boundary burst, ~exact | `sliding_window_counter` | `local` / `global` | `fail_local` |
| Billing-critical, exact | `sliding_window_log` (local) / `sliding_window_counter` (global) | `global` | `fail_closed` |
| Shape traffic to a fixed-rate downstream | `leaky_bucket` (honour `Decision.Delay`) | `local` | n/a |
| Cheapest coarse cap | `fixed_window` | any | `fail_open` |

## Failure behaviour

| Situation | fail_open | fail_closed | fail_local |
|---|---|---|---|
| Redis error / timeout | allow | deny, `Retry-After` = cooldown | decide with in-process limiter (`fallback_scale` x limit) |
| Breaker open | allow, Redis not called | deny, Redis not called | local, Redis not called |
| Recovery | one probe after cooldown; success closes the breaker | same | same |

Every degraded decision has `Degraded=true` and increments `ratelimiter_degraded_decisions_total`, so a Redis outage is visible even though traffic keeps flowing. `fail_local` with `fallback_scale = 1/nodes` approximates the global limit during an outage; with `1.0` each node enforces the full limit (over-admits by up to N×).

## Accuracy guarantees (all verified by tests)

* Local algorithms are exact under concurrency (`TestConcurrentExactness`: 16 goroutines, exactly `limit` admissions).
* Redis scripts are atomic (`TestCentralAtomicUnderConcurrency`).
* Hybrid never exceeds the *global* bucket (leases are withdrawn from it); it can strand up to `nodes × lease_size` tokens in idle leases (`LeaseTTL` bounds that).
* Multi-limit rules refund sibling limits on deny. Central (Redis) limits are not refunded: a request denied by a sibling still consumed Redis quota. Keep multi-window global rules to cases where that is acceptable, or use a single combined script.

## Known limits / next steps

* Key string allocation: the engine builds one key string per matched rule (≈1 alloc, 20 B). Avoiding it needs an unsafe string view or a byte-keyed map.
* Redis central path allocates ≈25 objects per call in the minimal RESP client. Hybrid (the intended hot path) avoids Redis per request; if you run `global` hot, swap in go-redis or optimise the client's buffer reuse.
* Hot-key sub-counters (`X#0..X#9` across shards) are not implemented; hybrid leasing is the mitigation shipped.
* Redis Cluster/sharding: keys use `{key}` hash tags so each limit is single-slot; `RESPClient` talks to one endpoint, so use a cluster-aware client behind `distributed.Client` for multi-shard deployments.
* Local fixed-window alignment is relative to process start (monotonic clock), not wall-clock boundaries.
* Sliding-window-log is O(limit) memory per key; keep limits small.
