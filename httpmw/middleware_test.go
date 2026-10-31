package httpmw

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/yourorg/ratelimiter/engine"
)

func TestMiddleware429WithHeaders(t *testing.T) {
	e := engine.New(engine.Options{})
	defer e.Close()
	c, _ := engine.ParseConfig([]byte(`{"rules":[{"id":"r","key_by":["api_key"],"limits":[{"limit":2,"window":"1m"}]}]}`))
	if err := e.Reload(c); err != nil {
		t.Fatal(err)
	}
	h := New(e, Options{})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	var last *httptest.ResponseRecorder
	codes := []int{}
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/x", nil)
		req.Header.Set("X-API-Key", "k")
		last = httptest.NewRecorder()
		h.ServeHTTP(last, req)
		codes = append(codes, last.Code)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("codes=%v", codes)
	}
	ra, err := strconv.Atoi(last.Header().Get("Retry-After"))
	if err != nil || ra < 1 {
		t.Fatalf("Retry-After=%q", last.Header().Get("Retry-After"))
	}
	if last.Header().Get("RateLimit-Limit") != "2" || last.Header().Get("RateLimit-Remaining") != "0" {
		t.Fatalf("headers: %v", last.Header())
	}
}
