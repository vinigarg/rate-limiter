# ratelimiter

Low-latency, configurable rate limiting in Go. Zero third-party dependencies.

* **Six algorithms, in-process:** fixed window, sliding window counter, sliding window log, token bucket, leaky bucket (shaping), GCRA.
* **Distributed:** exact Redis Lua tier (token bucket / sliding counter / fixed window) and a **hybrid token-lease** tier that keeps Redis off the request path.
* **Configurable per client:** rules match on path/method/attributes, key on any dimensions (`api_key`, `tenant`, `ip`, `header:X`…), pick a strategy, and stack multiple windows (100/s *and* 10k/day). Hot-reloaded, validated, last-good-config on error.
* **Failure policies:** `fail_open`, `fail_closed`, `fail_local` (in-process backstop) behind a timeout + circuit breaker.
* **Telemetry:** async, striped counters + sampled latency histogram → **OTLP/HTTP** push (OpenTelemetry Collector) and Prometheus `/metrics`.
* **Shadow mode** for safe rollout; jittered `Retry-After`; `RateLimit-*` headers.

Design notes: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) · Benchmarking: [docs/BENCHMARKING.md](docs/BENCHMARKING.md)

## Quick start

```bash
make race                     # vet + tests with the race detector (Redis tests auto-start redis-server if present)
make build && ./bin/limiterd -config configs/rules.json
curl -s localhost:8080/v1/limits:check -d '{"ip":"1.2.3.4","path":"/login","method":"POST"}'
curl -s localhost:8080/metrics

# full stack: Redis + OTel Collector + Prometheus + limiterd
docker compose -f deploy/docker-compose.yml up --build
```

Rename the module once: `make rename MODULE=github.com/acme/ratelimiter`.

## Use as a library

```go
reg := telemetry.NewRegistry()
e := engine.New(engine.Options{
    Redis:   distributed.NewRESPClient("redis:6379"), // only for global/hybrid rules
    Metrics: reg,
})
cfg, _ := engine.LoadConfig("rules.json")
if err := e.Reload(cfg); err != nil { log.Fatal(err) }
go e.WatchFile(ctx, "rules.json", 5*time.Second)

// HTTP
mux := httpmw.New(e, httpmw.Options{Extract: myExtractFromAuthenticatedPrincipal})(handler)

// metrics → OpenTelemetry Collector
go (&telemetry.Flusher{Registry: reg, Interval: 10 * time.Second,
    Exporter: telemetry.NewOTLPHTTP(telemetry.OTLPOptions{
        Endpoint: "http://otel-collector:4318/v1/metrics", ServiceName: "my-api"})}).Run(ctx)

// or use one algorithm directly, no engine
l, _ := algo.New(algo.GCRAName, ratelimiter.Spec{Limit: 100, Window: time.Second, Burst: 20}, algo.Options{})
d, _ := l.Allow(ctx, "client-42", 1)
```

## Rule example

```json
{ "id": "public-api", "priority": 50,
  "match": { "path_prefix": "/v1/", "attrs": { "tier": "free" } },
  "key_by": ["api_key"],
  "strategy": "token_bucket",
  "scope": "hybrid", "on_error": "fail_local", "fallback_scale": 0.1,
  "lease_size": 100, "lease_ttl": "1s",
  "limits": [ { "limit": 10000, "window": "1m", "burst": 1000 } ],
  "shadow": false }
```

Unknown JSON fields are rejected so a typo cannot silently disable a limit.

## Telemetry emitted

| OTLP metric | Prometheus | Attributes |
|---|---|---|
| `ratelimiter.decisions` (sum) | `ratelimiter_decisions_total` | `rule`, `outcome` = allowed / denied / shadow_denied |
| `ratelimiter.degraded_decisions` | `ratelimiter_degraded_decisions_total` | `rule` |
| `ratelimiter.backend_errors` | `ratelimiter_backend_errors_total` | `rule` |
| `ratelimiter.decision.duration` (histogram, s) | `ratelimiter_decision_duration_seconds` | `rule` (sampled 1 in 16) |

Alert ideas: denied ratio per rule spike (abuse vs misconfig: check `shadow_denied` first), any `degraded` > 0 (Redis trouble), p99 duration.

## Using go-redis instead of the built-in client

```go
type goRedis struct{ c *redis.Client }
func (g goRedis) Eval(ctx context.Context, s *distributed.Script, keys []string, args ...any) ([]int64, error) {
    return redis.NewScript(s.Src).Run(ctx, g.c, keys, args...).Int64Slice()
}
```

## Layout

```
limiter.go            core types (Decision, Limiter, Spec, Clock)
algo/                 6 algorithms + Multi (multi-window with refunds) + tests + benchmarks
internal/keystore/    sharded, capped, janitor-swept state map
distributed/          RESP client, Lua scripts, Central, Hybrid (leases) + Redis integration tests
resilience/           Guard: timeout, circuit breaker, fail_open/closed/local
engine/               rules, matching, key building, hot reload, shadow mode
telemetry/            metrics registry, OTLP/HTTP exporter, Prometheus handler, flusher
httpmw/               net/http middleware (429, RateLimit-*, jittered Retry-After)
cmd/limiterd          limiter-as-a-service (POST /v1/limits:check, /metrics, /healthz)
cmd/loadgen           closed/open-loop load generator with Zipf keys and percentiles
configs/ deploy/      sample rules, docker-compose, collector + Prometheus config
```
