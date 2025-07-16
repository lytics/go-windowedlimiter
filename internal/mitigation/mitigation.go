// Package mitigation provides a way to fast-path allows for keys that are not actively hitting their rate limit.
//
// While a key is not mitigated, `Allow()` and `Wait()` will only do a `xsync.Map` lookup and then return immediately.
//
// While a key is mitigated, a single goroutine will be responsible for checking the allowFn and distributing the the allowed requests to all callers evenly.
package mitigation

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync"
	"github.com/vitaminmoo/go-slidingwindow/internal/fifo"
)

var ttlMultiplier = 3

type tick struct {
	C    chan struct{}
	Done chan struct{}
}

// Mitigation exists to allow caching of a mitigated state, and cooperative sharing of requests as the Mitigation expires and is refreshed
type Mitigation struct {
	period   time.Duration      // the period we should retry the allow function at
	ttl      time.Time          // when the mitigation will be deleted entirely, shutting down goroutines
	until    time.Time          // when the mitigation will be re-evaluated
	q        *fifo.Queue[*tick] // the queue for the mitigation
	ctx      context.Context    // context for cancellation of the mitigation garbage collector goroutine
	mu       sync.Mutex         // mutex for the mitigation
	allowOne bool               // if false, first allowed request toggles this for the next Allow() call to consume
}

func New(allowFn func(context.Context, string) bool) *MitigationCache {
	mc := &MitigationCache{
		cache:   xsync.NewMapOf[*Mitigation](),
		allowFn: allowFn,
	}
	return mc
}

type MitigationCache struct {
	cache   *xsync.MapOf[string, *Mitigation]
	allowFn func(ctx context.Context, key string) bool
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
func (mc *MitigationCache) Trigger(ctx context.Context, key string, period time.Duration) {
	m, ok := mc.cache.Load(key)
	if ok {
		m.mu.Lock()
		m.ttl = time.Now().Add(time.Duration(ttlMultiplier) * period)
		m.mu.Unlock()
		return
	}
	m = &Mitigation{
		period: period,
		ttl:    time.Now().Add(time.Duration(ttlMultiplier) * period),
		until:  time.Now().Add(period),
		q:      fifo.New[*tick](),
		ctx:    ctx,
	}
	mc.cache.Store(key, m)

	go pprof.Do(ctx, pprof.Labels("key", key), func(ctx context.Context) {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
	TICK:
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.mu.Lock()
				now := time.Now()
				if now.After(m.ttl) {
					if m.q.Peek() == nil {
						// the mitigation is expired, nuke it
						mc.cache.Delete(key)
						m.mu.Unlock()
						return
					}
					// there are still subscribers, extend the mitigation
					// this is kinda incompatible with moving from Wait() to something
					// more stateful
					m.ttl = time.Now().Add(time.Duration(ttlMultiplier) * m.period)
				}
				if now.After(m.until) {
					// the mitigation is ready to be re-evaluated
					if mc.allowFn(ctx, key) {
					NEXT:
						if !m.allowOne {
							// feed first allowed request to any realtime Allow() that happens
							m.allowOne = true
							m.mu.Unlock()
							continue TICK
						}
						next := m.q.Shift()
						if next == nil {
							// empty ring, can't send to anyone
							m.mu.Unlock()
							continue TICK
						}
						select {
						case next.Value.C <- struct{}{}:
						default:
							// no listener on the blocking channel, remove it from the ring
							err := next.Remove()
							if err != nil {
								panic(err)
							}
							goto NEXT
						}
					} else {
						m.ttl = now.Add(time.Duration(ttlMultiplier) * m.period)
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
func (mc *MitigationCache) Wait(ctx context.Context, key string) error {
	m, ok := mc.cache.Load(key)
	if !ok {
		// we're not actually mitigated
		return nil
	}
	t := &tick{
		C:    make(chan struct{}),
		Done: make(chan struct{}),
	}
	cleanup := func(t *tick) error {
		select {
		case t.Done <- struct{}{}:
		default: // already done
		}
		return nil
	}
	entry := m.q.Push(t, cleanup)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.Done:
		return errors.New("done")
	case <-t.C:
		err := entry.Remove()
		if err != nil {
			return fmt.Errorf("removing entry from fifo: %w", err)
		}
		return nil
	}
}

// Allow reports whether a request is allowed for the given key.
func (mc *MitigationCache) Allow(ctx context.Context, key string) bool {
	m, ok := mc.cache.Load(key)
	if !ok {
		// we're not actually mitigated, allow immediately
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.allowOne {
		m.allowOne = false
		return true
	}
	// bump the ttl as if we're only using Allow, the per-mitigation gc/ticker goroutine will not
	m.ttl = time.Now().Add(3 * m.period)
	return false
}
