// limiterd serves the limiter as a service, matching the reference design:
// the limiter always answers 200 with a verdict and the calling gateway turns a
// deny into a 429. It also exposes /metrics (Prometheus), pushes OTLP metrics
// to a collector, and hot-reloads the rule file.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yourorg/ratelimiter/distributed"
	"github.com/yourorg/ratelimiter/engine"
	"github.com/yourorg/ratelimiter/telemetry"
)

type checkReq struct {
	ClientID string            `json:"client_id"`
	APIKey   string            `json:"api_key"`
	IP       string            `json:"ip"`
	Path     string            `json:"path"`
	Method   string            `json:"method"`
	Tenant   string            `json:"tenant"`
	Tier     string            `json:"tier"`
	Headers  map[string]string `json:"headers"`
	Cost     int64             `json:"cost"`
}

type checkResp struct {
	Allowed           bool   `json:"allowed"`
	Rule              string `json:"rule,omitempty"`
	Limit             int64  `json:"limit"`
	Remaining         int64  `json:"remaining"`
	RetryAfterSeconds int64  `json:"retry_after_seconds"`
	ResetSeconds      int64  `json:"reset_seconds"`
	DelayMillis       int64  `json:"delay_ms,omitempty"`
	Degraded          bool   `json:"degraded,omitempty"`
	Shadow            bool   `json:"shadow,omitempty"`
}

func main() {
	var (
		addr       = flag.String("addr", ":8080", "listen address")
		cfgPath    = flag.String("config", "configs/rules.json", "rules file (hot-reloaded)")
		reload     = flag.Duration("reload-interval", 5*time.Second, "config poll interval")
		redisAddr  = flag.String("redis", "", "redis host:port (enables global/hybrid scopes)")
		redisPass  = flag.String("redis-password", os.Getenv("REDIS_PASSWORD"), "redis password")
		redisTO    = flag.Duration("redis-timeout", 5*time.Millisecond, "per-decision redis budget before the failure policy applies")
		otlp       = flag.String("otlp-endpoint", "", "OTLP/HTTP metrics endpoint, e.g. http://otel-collector:4318/v1/metrics")
		otlpEvery  = flag.Duration("otlp-interval", 10*time.Second, "OTLP export interval")
		service    = flag.String("service-name", "limiterd", "service.name resource attribute")
		sampleEach = flag.Uint("latency-sample-every", 16, "measure decision latency for 1 in N checks")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	opts := engine.Options{Logger: log, RedisTimeout: *redisTO, LatencySampleEvery: uint32(*sampleEach), KeyPrefix: "rl:"}
	if *redisAddr != "" {
		opts.Redis = distributed.NewRESPClient(*redisAddr, distributed.WithPassword(*redisPass))
	}
	e := engine.New(opts)
	defer e.Close()

	cfg, err := engine.LoadConfig(*cfgPath)
	if err == nil {
		err = e.Reload(cfg)
	}
	if err != nil {
		log.Error("initial config invalid", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go e.WatchFile(ctx, *cfgPath, *reload)

	if *otlp != "" {
		host, _ := os.Hostname()
		fl := &telemetry.Flusher{Registry: e.Metrics(), Interval: *otlpEvery, Logger: log,
			Exporter: telemetry.NewOTLPHTTP(telemetry.OTLPOptions{Endpoint: *otlp, ServiceName: *service,
				Resource: map[string]string{"host.name": host}})}
		go fl.Run(ctx)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/limits:check", func(w http.ResponseWriter, r *http.Request) {
		var q checkReq
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&q); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res := e.Check(r.Context(), &engine.Request{ClientID: q.ClientID, APIKey: q.APIKey, IP: q.IP, Path: q.Path,
			Method: q.Method, Tenant: q.Tenant, Tier: q.Tier, Headers: q.Headers, Cost: q.Cost})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(checkResp{ // always 200: the gateway owns the 429
			Allowed: res.Allowed, Rule: res.RuleID, Limit: res.Limit, Remaining: max(res.Remaining, 0),
			RetryAfterSeconds: int64((res.RetryAfter + time.Second - 1) / time.Second),
			ResetSeconds:      int64((res.ResetAfter + time.Second - 1) / time.Second),
			DelayMillis:       res.Delay.Milliseconds(), Degraded: res.Degraded, Shadow: res.Shadow})
	})
	mux.Handle("GET /metrics", telemetry.PromHandler(e.Metrics()))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		<-ctx.Done()
		sc, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		srv.Shutdown(sc)
	}()
	log.Info("limiterd listening", "addr", *addr, "redis", *redisAddr != "", "otlp", *otlp != "")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server", "err", err)
		os.Exit(1)
	}
}
