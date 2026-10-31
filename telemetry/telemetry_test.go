package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

func TestStripedCountersAreExactUnderContention(t *testing.T) {
	m := NewRegistry().Rule("r")
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				m.Observe(rl.Decision{Allowed: true})
			}
		}()
	}
	wg.Wait()
	if got := m.snapshot().Allowed; got != 160000 {
		t.Fatalf("allowed=%d want 160000", got)
	}
}

func TestOutcomeClassification(t *testing.T) {
	m := NewRegistry().Rule("r")
	m.Observe(rl.Decision{Allowed: true})
	m.Observe(rl.Decision{})
	m.Observe(rl.Decision{Allowed: true, Shadow: true})
	m.Observe(rl.Decision{Allowed: true, Degraded: true})
	s := m.snapshot()
	if s.Allowed != 2 || s.Denied != 1 || s.ShadowDenied != 1 || s.Degraded != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestOTLPExportRoundTripAndRetry(t *testing.T) {
	reg := NewRegistry()
	m := reg.Rule("free-tier")
	m.Observe(rl.Decision{Allowed: true})
	m.Observe(rl.Decision{})
	m.ObserveLatency(3 * time.Microsecond)
	m.ObserveError()

	var hits atomic.Int32
	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 { // first attempt: transient failure
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Token") != "s3cret" {
			t.Errorf("bad headers: %v", r.Header)
		}
		got, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()

	e := NewOTLPHTTP(OTLPOptions{Endpoint: srv.URL, ServiceName: "limiterd", Headers: map[string]string{"X-Token": "s3cret"},
		Resource: map[string]string{"deployment.environment": "test"}})
	now := time.Now()
	if err := e.Export(context.Background(), Batch{Start: now.Add(-time.Minute), Now: now, Rules: reg.Snapshot()}); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 2 {
		t.Fatalf("hits=%d, want retry after 503", hits.Load())
	}
	var doc map[string]any
	if err := json.Unmarshal(got, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	s := string(got)
	for _, want := range []string{`"service.name"`, `"limiterd"`, `"ratelimiter.decisions"`, `"outcome"`, `"asInt":"1"`,
		`"ratelimiter.decision.duration"`, `"explicitBounds"`, `"aggregationTemporality":2`, `"deployment.environment"`} {
		if !strings.Contains(s, want) {
			t.Errorf("payload missing %s", want)
		}
	}
}

func TestOTLPNoRetryOn4xx(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	e := NewOTLPHTTP(OTLPOptions{Endpoint: srv.URL})
	if err := e.Export(context.Background(), Batch{Start: time.Now(), Now: time.Now()}); err == nil || hits.Load() != 1 {
		t.Fatalf("err=%v hits=%d", err, hits.Load())
	}
}

func TestPrometheusOutput(t *testing.T) {
	reg := NewRegistry()
	m := reg.Rule("r1")
	m.Observe(rl.Decision{Allowed: true})
	m.ObserveLatency(2 * time.Microsecond)
	rec := httptest.NewRecorder()
	PromHandler(reg).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	out := rec.Body.String()
	for _, want := range []string{`ratelimiter_decisions_total{rule="r1",outcome="allowed"} 1`,
		`ratelimiter_decision_duration_seconds_bucket{rule="r1",le="+Inf"} 1`, `_count{rule="r1"} 1`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

type captureExporter struct{ n atomic.Int32 }

func (c *captureExporter) Export(context.Context, Batch) error { c.n.Add(1); return nil }

func TestFlusherFlushesPeriodicallyAndOnShutdown(t *testing.T) {
	ce := &captureExporter{}
	f := &Flusher{Registry: NewRegistry(), Exporter: ce, Interval: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	time.Sleep(70 * time.Millisecond)
	cancel()
	<-done
	if ce.n.Load() < 3 {
		t.Fatalf("flushes=%d, want >=3 (ticks + final)", ce.n.Load())
	}
}

func BenchmarkObserveParallel(b *testing.B) {
	m := NewRegistry().Rule("r")
	d := rl.Decision{Allowed: true}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Observe(d)
		}
	})
}
