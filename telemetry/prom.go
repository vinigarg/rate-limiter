package telemetry

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// PromHandler serves the registry in Prometheus text exposition format
// (pull model). Use it next to, or instead of, OTLP push.
func PromHandler(r *Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var b strings.Builder
		snaps := r.Snapshot()
		b.WriteString("# HELP ratelimiter_decisions_total Rate limit decisions by outcome.\n# TYPE ratelimiter_decisions_total counter\n")
		for _, s := range snaps {
			for _, o := range []struct {
				n string
				v uint64
			}{{"allowed", s.Allowed}, {"denied", s.Denied}, {"shadow_denied", s.ShadowDenied}} {
				fmt.Fprintf(&b, "ratelimiter_decisions_total{rule=%q,outcome=%q} %d\n", s.Rule, o.n, o.v)
			}
		}
		b.WriteString("# HELP ratelimiter_degraded_decisions_total Decisions made by a failure policy.\n# TYPE ratelimiter_degraded_decisions_total counter\n")
		for _, s := range snaps {
			fmt.Fprintf(&b, "ratelimiter_degraded_decisions_total{rule=%q} %d\n", s.Rule, s.Degraded)
		}
		b.WriteString("# HELP ratelimiter_backend_errors_total Primary limiter errors.\n# TYPE ratelimiter_backend_errors_total counter\n")
		for _, s := range snaps {
			fmt.Fprintf(&b, "ratelimiter_backend_errors_total{rule=%q} %d\n", s.Rule, s.Errors)
		}
		b.WriteString("# HELP ratelimiter_decision_duration_seconds Sampled decision latency.\n# TYPE ratelimiter_decision_duration_seconds histogram\n")
		for _, s := range snaps {
			var cum uint64
			for i, c := range s.LatencyHist {
				cum += c
				le := "+Inf"
				if i < len(BoundsSeconds) {
					le = strconv.FormatFloat(BoundsSeconds[i], 'g', -1, 64)
				}
				fmt.Fprintf(&b, "ratelimiter_decision_duration_seconds_bucket{rule=%q,le=%q} %d\n", s.Rule, le, cum)
			}
			fmt.Fprintf(&b, "ratelimiter_decision_duration_seconds_sum{rule=%q} %g\n", s.Rule, s.LatencySum)
			fmt.Fprintf(&b, "ratelimiter_decision_duration_seconds_count{rule=%q} %d\n", s.Rule, s.LatencyCount)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write([]byte(b.String()))
	})
}
