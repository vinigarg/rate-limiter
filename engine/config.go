package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Duration is a JSON string like "1s", "5m", "24h".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"1m\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// Config is the full rule set. It is data: clients/tenants change thresholds
// by editing it, and the engine hot-reloads it without a restart.
type Config struct {
	Rules []Rule `json:"rules"`
}

// Rule is one client-configurable limit definition.
type Rule struct {
	ID       string `json:"id"`
	Priority int    `json:"priority"` // evaluation order, higher first
	Match    Match  `json:"match"`
	// KeyBy are the dimensions forming the bucket key: client_id, api_key, ip,
	// path, method, tenant, tier, header:<Name>. Missing values become "-".
	KeyBy    []string `json:"key_by"`
	Strategy string   `json:"strategy"` // fixed_window|sliding_window_counter|sliding_window_log|token_bucket|leaky_bucket|gcra
	// Limits are ANDed (e.g. 100/s and 10000/day). Most rules have one.
	Limits []Limit `json:"limits"`
	// Scope: local (in-process only) | global (Redis per decision) | hybrid (Redis token leases).
	Scope string `json:"scope"`
	// OnError: fail_open | fail_closed | fail_local, applied when Redis is
	// unavailable (global/hybrid scopes). Default fail_open.
	OnError string `json:"on_error"`
	// FallbackScale scales the local fallback limit used by fail_local
	// (e.g. 0.1 when 10 nodes share the global limit). Default 1.
	FallbackScale float64 `json:"fallback_scale"`
	// LeaseSize/LeaseTTL tune hybrid mode (see distributed.HybridOptions).
	LeaseSize int64    `json:"lease_size"`
	LeaseTTL  Duration `json:"lease_ttl"`
	// Shadow: evaluate and record, but never deny. Roll out new limits safely.
	Shadow bool `json:"shadow"`
}

// Limit is one threshold.
type Limit struct {
	Limit  int64    `json:"limit"`
	Window Duration `json:"window"`
	Burst  int64    `json:"burst"`
}

// Match selects requests. All populated fields must match.
type Match struct {
	PathPrefix string            `json:"path_prefix"`
	Methods    []string          `json:"methods"`
	Attrs      map[string]string `json:"attrs"` // dimension -> exact value, e.g. {"tier":"free"}
}

// ParseConfig decodes JSON strictly (unknown fields are errors, so a typo in a
// threshold name cannot silently disable a limit).
func ParseConfig(b []byte) (Config, error) {
	var c Config
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, err
	}
	return c, nil
}

// LoadConfig reads and parses a config file.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return ParseConfig(b)
}

func (r Rule) validate() error {
	var errs []error
	if r.ID == "" {
		errs = append(errs, errors.New("id is required"))
	}
	if len(r.KeyBy) == 0 {
		errs = append(errs, errors.New("key_by needs at least one dimension"))
	}
	if len(r.Limits) == 0 {
		errs = append(errs, errors.New("limits needs at least one entry"))
	}
	if len(r.Limits) > 4 {
		errs = append(errs, errors.New("at most 4 limits per rule"))
	}
	return errors.Join(errs...)
}
