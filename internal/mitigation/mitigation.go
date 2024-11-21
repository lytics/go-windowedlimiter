package mitigation

import (
	"context"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/vitaminmoo/go-slidingwindow/internal/ringbalancer"
)

var (
	mitigationCache = sync.Map{}
	ttlMultiplier   = time.Duration(3)
)

// a mitigation exists to allow caching of a mitigated state, and cooperative
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

// Trigger creates a new mitigation
// args:
//
// ctx: context specifically for cancellation of the mitigation garbage collector goroutine
// key: the key to store the mitigation under
// period: the time between attempts to do a request
// allowFn: a function to call that will be the gatekeeper for requests, after mitigate does its own adjustment on how frequently to try
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
				if time.Now().After(m.ttl) {
					if mit, ok := mitigationCache.LoadAndDelete(key); ok {
						mit.(*mitigation).rb.Close()
					}
					m.mu.Unlock()
					return
				}
				if time.Now().After(m.until) {
					if m.allowFn(ctx) {
						m.rb.Tick()
					} else {
						m.ttl = time.Now().Add(ttlMultiplier * m.period)
						m.until = time.Now().Add(m.period)
					}
				}
				m.mu.Unlock()
			}
		}
	})
}

// Wait blocks until the mitigation fires, is cancelled via context, or is done
// Note that currently, Wait() unsubscribe/resubscribes to the ringbuffer every
// time it's called which is pretty non-ideal
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
		if m.rb.Subscribers() > 0 {
			// there are still subscribers, extend the mitigation
			// this is kinda incompatible with moving from Wait() to something
			// more stateful
			m.mu.Lock()
			m.ttl = time.Now().Add(3 * m.period)
			m.mu.Unlock()
		}
		return nil
	}
}

// Allow returns true if one request is allowed
// Note that Allow will not even try to do requests while a mitigation is
// active, so it will likely never succeed if goroutines are calling Wait() with
// the same key
func Allow(ctx context.Context, key string) bool {
	mit, ok := mitigationCache.Load(key)
	if !ok {
		return true
	}
	m := mit.(*mitigation)
	if m.allowFn(ctx) {
		return time.Now().After(m.until)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ttl = time.Now().Add(3 * m.period)
	return false
}
