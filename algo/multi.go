package algo

import (
	"context"
	"time"

	rl "github.com/yourorg/ratelimiter"
)

// Multi enforces several limits on one key at once (e.g. 100/s AND 10k/day).
// A request passes only if every limit admits it. If any limit denies, units
// already spent on sibling limits are refunded (when they implement
// Refunder), so a rejected request never burns quota.
//
// The reported decision is the binding one: on allow, the limit with the least
// remaining quota; on deny, the violated limit that frees up last (a client
// must wait for ALL violated limits to recover).
type Multi struct {
	ls        []rl.Limiter
	refunders []rl.Refunder // parallel to ls; nil where unsupported
}

// NewMulti returns the limiter itself when there is only one.
func NewMulti(ls ...rl.Limiter) rl.Limiter {
	if len(ls) == 1 {
		return ls[0]
	}
	m := &Multi{ls: ls, refunders: make([]rl.Refunder, len(ls))}
	for i, l := range ls {
		m.refunders[i], _ = l.(rl.Refunder)
	}
	return m
}

func (m *Multi) Allow(ctx context.Context, key string, cost int64) (rl.Decision, error) {
	var buf [4]rl.Decision
	ds := buf[:0]
	denied := false
	for i, l := range m.ls {
		d, err := l.Allow(ctx, key, cost)
		if err != nil {
			m.refund(ds, key, cost)
			return rl.Decision{}, err
		}
		ds = append(ds, d)
		denied = denied || !d.Allowed
		_ = i
	}
	if !denied {
		best := ds[0]
		for _, d := range ds[1:] {
			if d.Remaining < best.Remaining {
				best = d
			}
		}
		for _, d := range ds {
			best.Degraded = best.Degraded || d.Degraded
			if d.Delay > best.Delay {
				best.Delay = d.Delay
			}
		}
		return best, nil
	}
	m.refund(ds, key, cost)
	var worst rl.Decision
	var wait time.Duration = -1
	for _, d := range ds {
		if !d.Allowed && d.RetryAfter > wait {
			worst, wait = d, d.RetryAfter
		}
	}
	return worst, nil
}

func (m *Multi) refund(ds []rl.Decision, key string, cost int64) {
	for i, d := range ds {
		if d.Allowed && m.refunders[i] != nil {
			m.refunders[i].Refund(key, cost)
		}
	}
}

// Close closes every member that is closable.
func (m *Multi) Close() error {
	for _, l := range m.ls {
		if c, ok := l.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	return nil
}
