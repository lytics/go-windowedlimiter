package slidingwindow

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync"
	"github.com/redis/go-redis/v9"
	"github.com/vitaminmoo/go-slidingwindow/internal/mitigation"
	"github.com/yuseferi/zax/v2"
	"go.uber.org/zap"
)

// KeyConf is a configuration for a key. It's set lazily per-key by calling Options.KeyConfFn
type KeyConf struct {
	Rate     int64
	Interval time.Duration
}

// Options is a set of options for creating a new Limiter
type Options struct {
	Logger *zap.Logger // The zap logger to use for logging. Will default to not logging anything
}

// New creates a new Limiter with the provided Redis client and options
//
// Note that rdb needs to be carefully configured for timeouts if you expect redis outages to not have severe impacts on latency.
//
// KeyConfFn returns a KeyConf for a given key. This will be called lazily, once per key, until `Limiter.Refresh()` or `Limiter.RefreshKey(key string)` is called.
func New(ctx context.Context, rdb redis.Cmdable, keyConfFn func(ctx context.Context, key string) *KeyConf, options ...Options) *Limiter {
	l := &Limiter{
		logger: zap.NewNop(),
		rdb:    rdb,
		// TODO: expire keyConfCache entries
		keyConfCache: xsync.NewMapOf[*KeyConf](),
		keyConfFn:    keyConfFn,
		incrChan:     make(chan string, 1000),
		doneChan:     make(chan struct{}),
	}
	if len(options) > 0 {
		l.logger = options[0].Logger
	}
	allow := func(ctx context.Context, key string) bool {
		allowed, err := l.checkRedis(ctx, key)
		if err != nil {
			l.logger.Debug("failed to check redis for mitigation status", zap.Error(err))
			return true // if we can't check redis, we assume the key is allowed
		}
		return allowed
	}
	mit := mitigation.New(allow)
	l.mitigationCache = mit
	l.wg.Add(1)
	go pprof.Do(ctx, pprof.Labels("name", "slidingwindow_incrementer"), func(ctx context.Context) { l.incrementer(ctx) })
	return l
}

type Limiter struct {
	mu              sync.RWMutex
	logger          *zap.Logger
	rdb             redis.Cmdable
	mitigationCache *mitigation.MitigationCache
	keyConfCache    *xsync.MapOf[string, *KeyConf]
	keyConfFn       func(ctx context.Context, key string) *KeyConf
	incrChan        chan string
	doneChan        chan struct{}
	wg              sync.WaitGroup
}

// SetRDB sets the redis client on the limiter. This is thread-safe.
func (l *Limiter) SetRDB(rdb redis.Cmdable) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rdb = rdb
}

// Close stops all goroutines, flushes logging, and waits for completion.
func (l *Limiter) Close() {
	l.mitigationCache.Close()
	select {
	case l.doneChan <- struct{}{}:
		// Successfully sent done signal
		l.wg.Wait()
	default:
		// Channel is full or closed, log a warning
		l.logger.Warn("unable to send done signal to incrementer channel - channel might be full or closed")
	}
	_ = l.logger.Sync()
}

// Refresh causes the KeyConfFn to be called again for all keys (lazily).
//
// If your rate limit is changing globally, you should call this once the keyConfFn is returning the new result
func (l *Limiter) Refresh(ctx context.Context) {
	l.keyConfCache.Range(func(key string, _ *KeyConf) bool {
		l.keyConfCache.Delete(key)
		return true
	})
}

// RefreshKey causes the KeyConfFn to be called again for the specified key (lazily).
//
// If your rate limit is changing for a specific key, you should call this once the keyConfFn is returning the new result
func (l *Limiter) RefreshKey(ctx context.Context, key string) {
	l.keyConfCache.Delete(key)
}

// keyConf returns the KeyConf for a given key, calling the KeyConfFn if it hasn't been called for this key yet
func (l *Limiter) keyConf(ctx context.Context, key string) *KeyConf {
	keyConf, ok := l.keyConfCache.Load(key)
	if !ok {
		keyConf = l.keyConfFn(ctx, key)
		l.keyConfCache.Store(key, keyConf)
	}
	return keyConf
}

func (l *Limiter) incrementer(ctx context.Context) {
	logger := l.logger.With(zax.Get(ctx)...)
	l.wg.Done()

	keysToIncr := make(map[string]int64)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-l.doneChan:
			logger.Debug("incrementer stopping via Close(), processing final batch")
			l.processIncrBatch(ctx, keysToIncr)
			return
		case <-ctx.Done():
			logger.Debug("incrementer stopping via context cancellation, processing final batch")
			l.processIncrBatch(ctx, keysToIncr)
			return
		case key := <-l.incrChan:
			keysToIncr[key]++
			// if the channel is getting full, process the batch to avoid blocking callers
			if len(l.incrChan) > cap(l.incrChan)/2 {
				l.processIncrBatch(ctx, keysToIncr)
				keysToIncr = make(map[string]int64)
			}
		case <-ticker.C:
			if len(keysToIncr) > 0 {
				l.processIncrBatch(ctx, keysToIncr)
				keysToIncr = make(map[string]int64)
			}
		}
	}
}

func (l *Limiter) processIncrBatch(ctx context.Context, batch map[string]int64) {
	if len(batch) == 0 {
		return
	}
	logger := l.logger.With(zax.Get(ctx)...)

	l.mu.RLock()
	rdb := l.rdb
	l.mu.RUnlock()

	if rdb == nil {
		logger.Error("redis client is nil, skipping batch processing")
		return
	}

	pipe := rdb.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(batch))

	now := time.Now()
	for key, count := range batch {
		keyConf := l.keyConf(ctx, key)
		curIntStart := now.Truncate(keyConf.Interval)
		curKey := fmt.Sprintf("{%s}.%d", key, curIntStart.UnixNano())
		cmds[key] = pipe.IncrBy(ctx, curKey, count)
		pipe.Expire(ctx, curKey, max(3*keyConf.Interval, time.Second))
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.ErrClosed) {
		logger.Error("error executing increment pipeline", zap.Error(err))
	}

	for key, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil && !errors.Is(err, redis.ErrClosed) {
			logger.Error("error incrementing key in pipeline", zap.String("key", key), zap.Error(err))
			continue
		}
		keyConf := l.keyConf(ctx, key)
		if res >= keyConf.Rate {
			l.mitigate(ctx, key)
		}
	}
}

func (l *Limiter) checkRedis(ctx context.Context, key string) (bool, error) {
	logger := l.logger.With(zax.Get(ctx)...)

	l.mu.RLock()
	rdb := l.rdb
	l.mu.RUnlock()

	now := time.Now()
	keyConf := l.keyConf(ctx, key)
	curIntStart := now.Truncate(keyConf.Interval)
	prevIntStart := curIntStart.Add(-keyConf.Interval)
	intervals, err := rdb.MGet(ctx,
		fmt.Sprintf("{%s}.%d", key, curIntStart.UnixNano()),
		fmt.Sprintf("{%s}.%d", key, prevIntStart.UnixNano()),
	).Result()
	if err != nil {
		// we failed talking to redis here so we do /not/ try to do any other commands
		// to it, especially not trying to write to a node that can't even do a read
		logger.Error("getting intervals", zap.Error(err))
		return false, err
	}

	var curr, prev int64
	var errCurr, errPrev error
	if intervals[0] != nil {
		curr, errCurr = strconv.ParseInt(intervals[0].(string), 10, 64)
	}
	if intervals[1] != nil {
		prev, errPrev = strconv.ParseInt(intervals[1].(string), 10, 64)
	}
	if errCurr != nil || errPrev != nil {
		return false, fmt.Errorf("could not parse interval values, cur: %w, prev: %w", errCurr, errPrev)
	}

	curIntAgo := now.Sub(curIntStart)
	portionOfLastInt := keyConf.Interval - curIntAgo
	percentOfLastInt := float64(portionOfLastInt.Nanoseconds()) / float64(keyConf.Interval.Nanoseconds())
	rawOfLastInt := int64(float64(prev) * percentOfLastInt)
	rateEstimate := rawOfLastInt + curr
	if rateEstimate >= keyConf.Rate {
		l.mitigate(ctx, key)
		return false, nil
	}
	return true, nil
}

func (l *Limiter) mitigate(ctx context.Context, key string) {
	logger := l.logger.With(zax.Get(ctx)...)
	keyConf := l.keyConf(ctx, key)
	period := time.Duration(keyConf.Interval.Nanoseconds() / keyConf.Rate)
	logger.Debug("mitigating", zap.Duration("period", period))
	l.mitigationCache.Trigger(ctx, key, period)
}

func (l *Limiter) Allow(ctx context.Context, key string) (allowed bool) {
	ctx = zax.Set(ctx, []zap.Field{zap.String("key", key)})
	pprof.Do(ctx, pprof.Labels("key", key), func(ctx context.Context) {
		allowed = l.allow(ctx, key)
	})
	return
}

func (l *Limiter) allow(ctx context.Context, key string) (allowed bool) {
	logger := l.logger.With(zax.Get(ctx)...)
	var err error
	defer func() {
		if err != nil {
			logger.Error("checking redis", zap.Error(err))
		} else if allowed {
			// only increment if we didn't fail talking to redis
			l.incrChan <- key
		}
	}()

	if !l.mitigationCache.Allow(ctx, key) {
		logger.Debug("blocked by mitigation")
		return false
	}

	allowed, err = l.checkRedis(ctx, key)
	logger.Debug("checked by redis", zap.Bool("allowed", allowed), zap.Error(err))
	if err != nil {
		logger.Debug("failed to check redis for rate", zap.Error(err))
		return true // if we can't check redis, we assume the key is allowed
	}
	return allowed
}

func (l *Limiter) Wait(ctx context.Context, key string) {
	ctx = zax.Set(ctx, []zap.Field{zap.String("key", key)})
	pprof.Do(ctx, pprof.Labels("key", key), func(ctx context.Context) {
		l.wait(ctx, key)
	})
}

func (l *Limiter) wait(ctx context.Context, key string) {
	logger := l.logger.With(zax.Get(ctx)...)
	for retries := 0; ; retries++ { // TODO: exponential backoff, and maybe a retry limit?
		err := l.mitigationCache.Wait(ctx, key)
		if err == nil {
			if retries > 0 {
				logger.Warn("waiter retried", zap.Int("count", retries))
			}
			l.incrChan <- key
			return
		}

		logger.Error("waiting", zap.Error(err))

		// mitigation.Wait should return context errors, but just in case it wraps them, we check the context as well.
		if ctx.Err() != nil {
			logger.Warn("context cancelled during wait")
			return
		}
	}
}
