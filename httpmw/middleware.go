// Package httpmw adapts the engine to net/http: it extracts a Request, asks
// the engine, sets RateLimit-* headers, and turns a deny into 429 with a
// jittered Retry-After (a fixed delay would re-synchronise clients into the
// next spike).
package httpmw

import (
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yourorg/ratelimiter/engine"
)

// Options configures the middleware.
type Options struct {
	// Extract maps an HTTP request to the engine's view of it. Default:
	// DefaultExtract. Production setups should derive Tier/Tenant from the
	// authenticated principal, never from client-supplied headers.
	Extract func(*http.Request) *engine.Request
	// JitterFraction adds up to this fraction (default 0.2) of random extra
	// delay to Retry-After.
	JitterFraction float64
	// ApplyDelay sleeps for Decision.Delay (leaky-bucket shaping) before
	// calling the next handler.
	ApplyDelay bool
}

// DefaultExtract reads X-API-Key, X-Tenant, X-Tier and the socket peer IP.
func DefaultExtract(r *http.Request) *engine.Request {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	h := make(map[string]string, len(r.Header))
	for k := range r.Header {
		h[k] = r.Header.Get(k)
	}
	return &engine.Request{
		APIKey: r.Header.Get("X-API-Key"), ClientID: r.Header.Get("X-Client-ID"),
		Tenant: r.Header.Get("X-Tenant"), Tier: r.Header.Get("X-Tier"),
		IP: ip, Path: r.URL.Path, Method: strings.ToUpper(r.Method), Headers: h,
	}
}

// New returns the middleware.
func New(e *engine.Engine, o Options) func(http.Handler) http.Handler {
	if o.Extract == nil {
		o.Extract = DefaultExtract
	}
	if o.JitterFraction <= 0 {
		o.JitterFraction = 0.2
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res := e.Check(r.Context(), o.Extract(r))
			if res.RuleID != "" {
				h := w.Header()
				h.Set("RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
				h.Set("RateLimit-Remaining", strconv.FormatInt(max(res.Remaining, 0), 10))
				h.Set("RateLimit-Reset", strconv.Itoa(int(math.Ceil(res.ResetAfter.Seconds()))))
			}
			if !res.Allowed {
				wait := res.RetryAfter.Seconds()
				wait += wait * o.JitterFraction * rand.Float64()
				w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(wait)))))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":"rate_limited","rule":"` + res.RuleID + `"}`))
				return
			}
			if o.ApplyDelay && res.Delay > 0 {
				t := time.NewTimer(res.Delay)
				select {
				case <-t.C:
				case <-r.Context().Done():
					t.Stop()
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
