package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// OTLPOptions configures the OTLP/HTTP (JSON) metrics exporter.
type OTLPOptions struct {
	Endpoint    string            // e.g. http://otel-collector:4318/v1/metrics
	ServiceName string            // resource attribute service.name
	Headers     map[string]string // e.g. auth for a hosted backend
	Client      *http.Client      // default: 5s timeout
	MaxRetries  int               // default 3 (429/5xx/network errors, exp. backoff + jitter)
	Resource    map[string]string // extra resource attributes (env, region, host...)
}

// OTLPHTTP exports metrics using the OpenTelemetry Protocol over HTTP with the
// JSON encoding, so it needs no SDK or protobuf dependency and works with any
// OTLP receiver (OpenTelemetry Collector, Grafana Alloy, Datadog, Honeycomb...).
type OTLPHTTP struct{ o OTLPOptions }

func NewOTLPHTTP(o OTLPOptions) *OTLPHTTP {
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 3
	}
	if o.ServiceName == "" {
		o.ServiceName = "ratelimiter"
	}
	return &OTLPHTTP{o: o}
}

type kv struct {
	Key   string `json:"key"`
	Value struct {
		S string `json:"stringValue"`
	} `json:"value"`
}

func attr(k, v string) kv {
	a := kv{Key: k}
	a.Value.S = v
	return a
}

// OTLP JSON encodes 64-bit integers as decimal strings.
func u64(v uint64) string { return strconv.FormatUint(v, 10) }

func (e *OTLPHTTP) payload(b Batch) ([]byte, error) {
	start, now := u64(uint64(b.Start.UnixNano())), u64(uint64(b.Now.UnixNano()))
	type sumDP struct {
		Attributes []kv   `json:"attributes"`
		Start      string `json:"startTimeUnixNano"`
		Time       string `json:"timeUnixNano"`
		AsInt      string `json:"asInt"`
	}
	type histDP struct {
		Attributes []kv      `json:"attributes"`
		Start      string    `json:"startTimeUnixNano"`
		Time       string    `json:"timeUnixNano"`
		Count      string    `json:"count"`
		Sum        float64   `json:"sum"`
		Buckets    []string  `json:"bucketCounts"`
		Bounds     []float64 `json:"explicitBounds"`
	}
	var decisions, degraded, errs []sumDP
	var hists []histDP
	for _, r := range b.Rules {
		rule := attr("rule", r.Rule)
		for _, o := range []struct {
			name string
			v    uint64
		}{{"allowed", r.Allowed}, {"denied", r.Denied}, {"shadow_denied", r.ShadowDenied}} {
			decisions = append(decisions, sumDP{[]kv{rule, attr("outcome", o.name)}, start, now, u64(o.v)})
		}
		degraded = append(degraded, sumDP{[]kv{rule}, start, now, u64(r.Degraded)})
		errs = append(errs, sumDP{[]kv{rule}, start, now, u64(r.Errors)})
		bc := make([]string, len(r.LatencyHist))
		for i, c := range r.LatencyHist {
			bc[i] = u64(c)
		}
		hists = append(hists, histDP{[]kv{rule}, start, now, u64(r.LatencyCount), r.LatencySum, bc, BoundsSeconds})
	}
	sum := func(name, desc, unit string, dps []sumDP) map[string]any {
		return map[string]any{"name": name, "description": desc, "unit": unit,
			"sum": map[string]any{"dataPoints": dps, "aggregationTemporality": 2, "isMonotonic": true}}
	}
	res := []kv{attr("service.name", e.o.ServiceName)}
	for k, v := range e.o.Resource {
		res = append(res, attr(k, v))
	}
	doc := map[string]any{"resourceMetrics": []any{map[string]any{
		"resource": map[string]any{"attributes": res},
		"scopeMetrics": []any{map[string]any{
			"scope": map[string]any{"name": "github.com/yourorg/ratelimiter"},
			"metrics": []any{
				sum("ratelimiter.decisions", "Rate limit decisions by outcome", "{decision}", decisions),
				sum("ratelimiter.degraded_decisions", "Decisions produced by a failure policy instead of the primary limiter", "{decision}", degraded),
				sum("ratelimiter.backend_errors", "Primary limiter (Redis) errors that triggered a fallback", "{error}", errs),
				map[string]any{"name": "ratelimiter.decision.duration", "description": "Sampled limiter decision latency", "unit": "s",
					"histogram": map[string]any{"dataPoints": hists, "aggregationTemporality": 2}},
			},
		}},
	}}}
	return json.Marshal(doc)
}

// Export POSTs the batch, retrying transient failures with exponential
// backoff and full jitter.
func (e *OTLPHTTP) Export(ctx context.Context, b Batch) error {
	body, err := e.payload(b)
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt <= e.o.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(rand.Int64N(int64(100*time.Millisecond) << attempt))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.o.Endpoint, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range e.o.Headers {
			req.Header.Set(k, v)
		}
		resp, err := e.o.Client.Do(req)
		if err != nil {
			last = err
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			last = fmt.Errorf("otlp: transient status %d", resp.StatusCode)
		default:
			return fmt.Errorf("otlp: status %d (not retrying)", resp.StatusCode)
		}
	}
	return last
}
