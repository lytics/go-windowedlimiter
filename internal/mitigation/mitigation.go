package mitigation

import (
	"context"
	"sync"
	"time"

	"github.com/vitaminmoo/go-slidingwindow/internal/ringbalancer"
)

var mitigationCache = sync.Map{}

// a mitigation exists to allow caching of a mitigated state, and cooperative
// sharing of requests as the mitigation expires and is refreshed
type mitigation struct {
	period  time.Duration              // the period we should retry the allow function at
	allowFn func(context.Context) bool // the function we should call to see if a request is allowed
	until   time.Time                  // when the mitigation will be deleted entirely, shutting down goroutines
	rb      *ringbalancer.Balancer     // the ring balancer for the mitigation
	ctx     context.Context            // context for cancellation of the mitigation garbage collector goroutine
	mu      sync.Mutex                 // mutex for the mitigation
}

// Mitigate creates a new mitigation
// args:
//
// ctx: context specifically for cancellation of the mitigation garbage collector goroutine
// key: the key to store the mitigation under
// period: the time between attempts to do a request
// allowFn: a function to call that will be the gatekeeper for requests, after mitigate does its own adjustment on how frequently to try
func Mitigate(ctx context.Context, key string, period time.Duration, allowFn func(context.Context) bool) {
	mit, ok := mitigationCache.Load(key)
	var m *mitigation
	if ok {
		// update the current mitigation, kill the ticker and restart
		m = mit.(*mitigation)
		m.period = period
		m.allowFn = allowFn
		m.until = time.Now().Add(3 * period)
		m.ctx.Done()
		m.ctx = ctx
	} else {
		m = &mitigation{
			period:  period,
			allowFn: allowFn,
			until:   time.Now().Add(3 * period),
			rb:      ringbalancer.New(),
			ctx:     ctx,
		}
		mitigationCache.Store(key, m)
	}

	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Now().After(m.until) {
					if mit, ok := mitigationCache.LoadAndDelete(key); ok {
						mit.(*mitigation).rb.Close()
					}
					return
				}
				if m.allowFn(ctx) {
					m.rb.Tick()
				} else {
					m.until = time.Now().Add(3 * m.period)
				}
			}
		}
	}()
}

// Wait blocks until the mitigation fires, is cancelled via context, or is done
// Note that it churns the ring balancer's subscribers
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

// Allow returns true if one request is allowed
func Allow(ctx context.Context, key string) bool {
	value, ok := mitigationCache.Load(key)
	if !ok {
		// we're not actually mitigated
		return true
	}
	m := value.(*mitigation)
	return m.allowFn(ctx)
}
