// Package mitigation provides a way to fast-path allows for keys that are not actively hitting their rate limit.
//
// While a key is not mitigated, `Allow()` and `Wait()` will only do a `sync.Map` lookup and then return immediately.
//
// While a key is mitigated, a single goroutine will be responsible for checking the allowFn and distributing the the allowed requests to all callers evenly.
//
// Keys are currently stored globally
package mitigation

import (
	"context"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/vitaminmoo/go-slidingwindow/internal/ringbalancer"
)

var (
	// This cache is in a package global as keys are global - like the external redis server
	mitigationCache = sync.Map{}
	ttlMultiplier   = time.Duration(3)
)

// mitigation exists to allow caching of a mitigated state, and cooperative
// sharing of requests as the mitigation expires and is refreshed
type mitigation struct {
	period  time.Duration              // the period we should retry the allow function at
	allowFn func(context.Context) bool // the function we should call to see if a request is allowed
	ttl     time.Time                  // when the mitigation will be deleted entirely, shutting down goroutines
	until   time.Time                  // when the mitigation will be re-evaluated
	rb      *ringbalancer.Balancer     // the ring balancer for the mitigation
	ctx     context.Context            // context for cancellation of the mitigation garbage collector goroutine
	mu      sync.Mutex                 // mutex for the mitigation
}

// Trigger creates a new mitigation or refreshes an existing one's ttl
//
// ctx is specifically for cancellation of the mitigation's garbage collector goroutine, which happens automatically if the mitigation expires.
//
// key is the key to mitigate. Note that all keys are global
//
// period is how often to retry the allowFn, which will be the maximum rate requests are allowed
//
// allowFn is a function to call that will be the final gatekeeper for whether requests are allowed. The mitigation cache is specifically designed to call this as little as possible, as the allowFn is expected to be expensive. allowFn must be thread safe.
func Trigger(ctx context.Context, key string, period time.Duration, allowFn func(context.Context) bool) {
	mit, ok := mitigationCache.Load(key)
	if ok {
		m := mit.(*mitigation)
		m.mu.Lock()
		m.ttl = time.Now().Add(ttlMultiplier * period)
		m.mu.Unlock()
		return
	}
	m := &mitigation{
		period:  period,
		allowFn: allowFn,
		ttl:     time.Now().Add(ttlMultiplier * period),
		until:   time.Now().Add(period),
		rb:      ringbalancer.New(),
		ctx:     ctx,
	}
	mitigationCache.Store(key, m)

	go pprof.Do(ctx, pprof.Labels("key", key), func(ctx context.Context) {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.mu.Lock()
				now := time.Now()
				if now.After(m.ttl) {
					if m.rb.Subscribers() == 0 {
						// the mitigation is expired, nuke it
						if mit, ok := mitigationCache.LoadAndDelete(key); ok {
							mit.(*mitigation).rb.Close()
						}
						m.mu.Unlock()
						return
					}
					// there are still subscribers, extend the mitigation
					// this is kinda incompatible with moving from Wait() to something
					// more stateful
					m.ttl = time.Now().Add(ttlMultiplier * m.period)
				}
				if now.After(m.until) {
					// the mitigation is ready to be re-evaluated
					if m.allowFn(ctx) {
						m.rb.Tick()
					} else {
						m.ttl = now.Add(ttlMultiplier * m.period)
						m.until = now.Add(m.period)
					}
				}
				m.mu.Unlock()
			}
		}
	})
}

// Wait blocks until the mitigation fires, is cancelled via context, or is done.
//
// Note that currently, Wait() unsubscribe/resubscribes to the ringbuffer every time it's called which is pretty non-ideal.
func Wait(ctx context.Context, key string) error {
	value, ok := mitigationCache.Load(key)
	if !ok {
		// we're not actually mitigated
		return nil
	}
	m := value.(*mitigation)
	entry := m.rb.Subscribe()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-entry.C:
		entry.Close()
		return nil
	}
}

// Allow reports whether a request is allowed for the given key.
func Allow(ctx context.Context, key string) bool {
	mit, ok := mitigationCache.Load(key)
	if !ok {
		// we're not actually mitigated, allow immediately
		return true
	}
	m := mit.(*mitigation)
	now := time.Now()
	allowed := false
	if now.After(m.until) {
		allowed = m.allowFn(ctx)
	}
	if allowed {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// bump the ttl as if we're only using Allow, the per-mitigation gc/ticker goroutine will not
	m.ttl = now.Add(3 * m.period)
	return false
}
