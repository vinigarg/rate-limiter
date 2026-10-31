// Package keystore is a sharded map of per-key limiter state with a hard cap
// on key cardinality and a background janitor.
package keystore

import (
	"hash/maphash"
	"sync"
	"time"
)

const numShards = 64 // power of two

type shard[S any] struct {
	mu sync.RWMutex
	m  map[string]*S
	_  [40]byte // reduce false sharing between shard locks
}

// Store maps keys to *S. When a shard is full, new keys share one overflow
// state instead of growing memory: a key-rotation attack degrades to a single
// shared bucket rather than OOM.
type Store[S any] struct {
	shards      [numShards]shard[S]
	seed        maphash.Seed
	newFn       func() *S
	perShardMax int
	overflow    *S
}

// New creates a Store. maxKeys<=0 disables the cap.
func New[S any](maxKeys int, newFn func() *S) *Store[S] {
	s := &Store[S]{seed: maphash.MakeSeed(), newFn: newFn, overflow: newFn()}
	if maxKeys > 0 {
		s.perShardMax = (maxKeys + numShards - 1) / numShards
	}
	for i := range s.shards {
		s.shards[i].m = make(map[string]*S)
	}
	return s
}

// Get returns the state for key, creating it if needed.
func (s *Store[S]) Get(key string) *S {
	sh := &s.shards[maphash.String(s.seed, key)&(numShards-1)]
	sh.mu.RLock()
	v := sh.m[key]
	sh.mu.RUnlock()
	if v != nil {
		return v
	}
	sh.mu.Lock()
	if v = sh.m[key]; v == nil {
		if s.perShardMax > 0 && len(sh.m) >= s.perShardMax {
			sh.mu.Unlock()
			return s.overflow
		}
		v = s.newFn()
		sh.m[key] = v
	}
	sh.mu.Unlock()
	return v
}

// Peek returns existing state without creating it.
func (s *Store[S]) Peek(key string) *S {
	sh := &s.shards[maphash.String(s.seed, key)&(numShards-1)]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.m[key]
}

// Len returns the number of tracked keys.
func (s *Store[S]) Len() int {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.RLock()
		n += len(s.shards[i].m)
		s.shards[i].mu.RUnlock()
	}
	return n
}

// Sweep deletes entries for which evict returns true. Evictable state must be
// equivalent to fresh state (idle and fully replenished), so a request racing
// with eviction loses nothing.
func (s *Store[S]) Sweep(evict func(*S) bool) int {
	removed := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, v := range sh.m {
			if evict(v) {
				delete(sh.m, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// RunJanitor sweeps every interval until the returned stop func is called.
func (s *Store[S]) RunJanitor(interval time.Duration, evict func(*S) bool) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.Sweep(evict)
			case <-done:
				return
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}
