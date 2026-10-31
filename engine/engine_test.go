package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rl "github.com/yourorg/ratelimiter"
	"github.com/yourorg/ratelimiter/distributed"
)

var bg = context.Background()

func cfg(t testing.TB, js string) Config {
	t.Helper()
	c, err := ParseConfig([]byte(js))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newEngine(t testing.TB, js string, o Options) (*Engine, *rl.ManualClock) {
	t.Helper()
	clk := rl.NewManualClock()
	o.Clock = clk
	e := New(o)
	if err := e.Reload(cfg(t, js)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e, clk
}

const basic = `{"rules":[
 {"id":"free","match":{"path_prefix":"/v1/","attrs":{"tier":"free"}},"key_by":["api_key"],
  "strategy":"token_bucket","limits":[{"limit":3,"window":"1m"}]},
 {"id":"pro","match":{"path_prefix":"/v1/","attrs":{"tier":"pro"}},"key_by":["api_key"],
  "strategy":"gcra","limits":[{"limit":100,"window":"1m"}]}
]}`

func TestPerClientThresholdsAndKeyIsolation(t *testing.T) {
	e, _ := newEngine(t, basic, Options{})
	free := &Request{APIKey: "a", Path: "/v1/x", Method: "GET", Tier: "free"}
	n := 0
	for i := 0; i < 10; i++ {
		if e.Check(bg, free).Allowed {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("free tier admitted %d, want 3", n)
	}
	if !e.Check(bg, &Request{APIKey: "b", Path: "/v1/x", Tier: "free"}).Allowed {
		t.Fatal("other api key must have its own bucket")
	}
	pro := &Request{APIKey: "a", Path: "/v1/x", Tier: "pro"}
	for i := 0; i < 50; i++ {
		if !e.Check(bg, pro).Allowed {
			t.Fatalf("pro tier denied at %d", i)
		}
	}
	if r := e.Check(bg, &Request{Path: "/other", Tier: "free"}); !r.Allowed || r.RuleID != "" {
		t.Fatalf("unmatched request must pass untouched: %+v", r)
	}
}

func TestMultiDimensionKeyAndHeaderDim(t *testing.T) {
	e, _ := newEngine(t, `{"rules":[{"id":"r","key_by":["tenant","header:X-Region"],
	  "limits":[{"limit":1,"window":"1h"}]}]}`, Options{})
	a := &Request{Tenant: "t1", Headers: map[string]string{"X-Region": "eu"}}
	b := &Request{Tenant: "t1", Headers: map[string]string{"X-Region": "us"}}
	if !e.Check(bg, a).Allowed || !e.Check(bg, b).Allowed {
		t.Fatal("different dimension values must not share a bucket")
	}
	if e.Check(bg, a).Allowed {
		t.Fatal("same key must be limited")
	}
}

func TestShadowModeRecordsButNeverBlocks(t *testing.T) {
	e, _ := newEngine(t, `{"rules":[{"id":"s","shadow":true,"key_by":["ip"],"limits":[{"limit":1,"window":"1h"}]}]}`, Options{})
	req := &Request{IP: "1.1.1.1"}
	var shadows int
	for i := 0; i < 5; i++ {
		r := e.Check(bg, req)
		if !r.Allowed {
			t.Fatal("shadow rule blocked traffic")
		}
		if r.Shadow {
			shadows++
		}
	}
	if shadows != 4 {
		t.Fatalf("shadow denials=%d want 4", shadows)
	}
	if s := e.Metrics().Snapshot()[0]; s.ShadowDenied != 4 || s.Allowed != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestStrictestRuleWinsAndMultiWindow(t *testing.T) {
	e, clk := newEngine(t, `{"rules":[
	 {"id":"perip","priority":1,"key_by":["ip"],"limits":[{"limit":100,"window":"1s"}]},
	 {"id":"login","priority":9,"match":{"path_prefix":"/login","methods":["post"]},"key_by":["ip"],
	  "limits":[{"limit":2,"window":"1s"},{"limit":3,"window":"1h"}]}]}`, Options{})
	req := &Request{IP: "9.9.9.9", Path: "/login", Method: "POST"}
	var denied Result
	n := 0
	for i := 0; i < 6; i++ {
		if r := e.Check(bg, req); r.Allowed {
			n++
		} else {
			denied = r
		}
	}
	if n != 2 || denied.RuleID != "login" {
		t.Fatalf("admitted %d, denier=%q", n, denied.RuleID)
	}
	clk.Advance(2 * time.Second)
	if !e.Check(bg, req).Allowed || e.Check(bg, req).Allowed { // 3rd hits the hourly cap
		t.Fatal("hourly window should bind after per-second recovers")
	}
}

func TestReloadKeepsCountersForUnchangedRulesAndResetsChanged(t *testing.T) {
	js := func(limit string) string {
		return `{"rules":[{"id":"stable","key_by":["ip"],"limits":[{"limit":2,"window":"1h"}]},
		{"id":"tuned","match":{"path_prefix":"/t"},"key_by":["ip"],"limits":[{"limit":` + limit + `,"window":"1h"}]}]}`
	}
	e, _ := newEngine(t, js("1"), Options{})
	req := &Request{IP: "x", Path: "/t"}
	e.Check(bg, req)
	if e.Check(bg, req).Allowed {
		t.Fatal("setup: tuned(1) should deny second")
	}
	if err := e.Reload(cfg(t, js("5"))); err != nil { // raise tuned, keep stable
		t.Fatal(err)
	}
	// stable already counted 2 checks above -> still exhausted (counters kept)
	if r := e.Check(bg, req); r.Allowed || r.RuleID != "stable" {
		t.Fatalf("stable counter should be preserved: %+v", r)
	}
	if r := e.Check(bg, &Request{IP: "x", Path: "/other"}); r.RuleID != "stable" || r.Allowed {
		t.Fatalf("%+v", r)
	}
}

func TestBadReloadKeepsLastGoodConfig(t *testing.T) {
	e, _ := newEngine(t, basic, Options{})
	bad := cfg(t, `{"rules":[{"id":"x","key_by":["nope"],"limits":[{"limit":1,"window":"1s"}]},
	   {"id":"y","key_by":["ip"],"strategy":"bogus","limits":[{"limit":1,"window":"1s"}]},
	   {"id":"z","key_by":["ip"],"scope":"global","limits":[{"limit":1,"window":"1s"}]}]}`)
	err := e.Reload(bad)
	if err == nil {
		t.Fatal("want validation errors")
	}
	for _, want := range []string{`rule "x"`, `rule "y"`, `rule "z"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should report %s: %v", want, err)
		}
	}
	if r := e.Check(bg, &Request{APIKey: "k", Path: "/v1/", Tier: "free"}); r.RuleID != "free" {
		t.Fatal("last good config must keep serving")
	}
}

func TestStrictParsingRejectsTypos(t *testing.T) {
	if _, err := ParseConfig([]byte(`{"rules":[{"id":"a","limitz":[]}]}`)); err == nil {
		t.Fatal("unknown field must be an error")
	}
	if _, err := ParseConfig([]byte(`{"rules":[{"id":"a","limits":[{"limit":1,"window":60}]}]}`)); err == nil {
		t.Fatal("numeric window must be an error")
	}
}

// ---- failure policies -------------------------------------------------------

type downRedis struct{ calls atomic.Int64 }

func (d *downRedis) Eval(context.Context, *distributed.Script, []string, ...any) ([]int64, error) {
	d.calls.Add(1)
	return nil, errors.New("connection refused")
}

func ruleWith(onError, scope, strategy string) string {
	return `{"rules":[{"id":"g","key_by":["ip"],"scope":"` + scope + `","strategy":"` + strategy + `","on_error":"` + onError + `",
	 "fallback_scale":1,"limits":[{"limit":2,"window":"1h"}]}]}`
}

func TestRedisOutagePolicies(t *testing.T) {
	cases := []struct {
		onError, scope, strategy string
		want                     []bool
	}{
		{"fail_open", "global", "sliding_window_counter", []bool{true, true, true, true}},
		{"fail_closed", "global", "sliding_window_counter", []bool{false, false, false, false}},
		{"fail_local", "global", "sliding_window_counter", []bool{true, true, false, false}},
		{"fail_local", "hybrid", "token_bucket", []bool{true, true, false, false}},
		{"fail_closed", "hybrid", "token_bucket", []bool{false, false, false, false}},
	}
	for _, c := range cases {
		t.Run(c.onError+"/"+c.scope, func(t *testing.T) {
			e, _ := newEngine(t, ruleWith(c.onError, c.scope, c.strategy), Options{Redis: &downRedis{}})
			for i, want := range c.want {
				r := e.Check(bg, &Request{IP: "1.2.3.4"})
				if r.Allowed != want || !r.Degraded {
					t.Fatalf("call %d: %+v want allowed=%v degraded", i, r.Decision, want)
				}
			}
			s := e.Metrics().Snapshot()[0]
			if s.Degraded != 4 || s.Errors == 0 {
				t.Fatalf("metrics should show degraded decisions and backend errors: %+v", s)
			}
		})
	}
}

func TestHybridRuleUsesLeasesAgainstFakeRedis(t *testing.T) {
	var calls atomic.Int64
	fake := evalFunc(func(args []any) []int64 { calls.Add(1); return []int64{100, 0} })
	e, _ := newEngine(t, `{"rules":[{"id":"h","key_by":["ip"],"scope":"hybrid","strategy":"token_bucket",
	  "lease_size":100,"limits":[{"limit":1000,"window":"1s"}]}]}`, Options{Redis: fake})
	for i := 0; i < 90; i++ {
		if !e.Check(bg, &Request{IP: "z"}).Allowed {
			t.Fatal("denied within lease")
		}
	}
	if calls.Load() > 2 {
		t.Fatalf("redis calls=%d: a lease should serve many decisions", calls.Load())
	}
}

type evalFunc func(args []any) []int64

func (f evalFunc) Eval(_ context.Context, _ *distributed.Script, _ []string, args ...any) ([]int64, error) {
	return f(args), nil
}

// The shipped sample configs must always load.
func TestSampleConfigsLoad(t *testing.T) {
	for _, f := range []string{"../configs/rules.json", "../configs/rules.distributed.json"} {
		c, err := LoadConfig(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		e := New(Options{Redis: &downRedis{}})
		if err := e.Reload(c); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		e.Close()
	}
}
