package distributed

import (
	"context"
	"fmt"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

// Central strategies available as Redis scripts.
const (
	TokenBucket          = "token_bucket"
	SlidingWindowCounter = "sliding_window_counter"
	FixedWindow          = "fixed_window"
)

// Central is the exact, authoritative limiter: one atomic script per
// decision, one network round trip. Wrap it in resilience.Guard.
type Central struct {
	client Client
	script *Script
	spec   rl.Spec
	prefix string
	args   [2]any // first two script args; cost is appended
	limit  int64
}

// NewCentral builds a Redis-backed limiter. prefix namespaces keys
// (typically "rl:<ruleID>:<limitIndex>:").
func NewCentral(c Client, strategy string, spec rl.Spec, prefix string) (*Central, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	ce := &Central{client: c, spec: spec, prefix: prefix}
	winMs := float64(spec.Window) / float64(time.Millisecond)
	switch strategy {
	case TokenBucket:
		ce.script = tokenBucketScript
		ce.args = [2]any{float64(spec.Limit) / winMs, spec.EffectiveBurst()}
		ce.limit = spec.EffectiveBurst()
	case SlidingWindowCounter:
		ce.script = slidingCounterScript
		ce.args = [2]any{spec.Limit, int64(winMs)}
		ce.limit = spec.Limit
	case FixedWindow:
		ce.script = fixedWindowScript
		ce.args = [2]any{spec.Limit, int64(winMs)}
		ce.limit = spec.Limit
	default:
		return nil, fmt.Errorf("distributed: strategy %q not available in redis (want %s|%s|%s)",
			strategy, TokenBucket, SlidingWindowCounter, FixedWindow)
	}
	return ce, nil
}

func (c *Central) Allow(ctx context.Context, key string, cost int64) (rl.Decision, error) {
	if cost > c.limit {
		return rl.Decision{Limit: c.limit, RetryAfter: c.spec.Window}, nil
	}
	// {key} hash tag: one limit == one slot, so scripts are cluster-safe.
	res, err := c.client.Eval(ctx, c.script, []string{c.prefix + "{" + key + "}"}, c.args[0], c.args[1], cost)
	if err != nil {
		return rl.Decision{}, err
	}
	if len(res) < 3 {
		return rl.Decision{}, fmt.Errorf("distributed: short script reply %v", res)
	}
	return rl.Decision{
		Allowed:    res[0] == 1,
		Limit:      c.limit,
		Remaining:  res[1],
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
	}, nil
}
