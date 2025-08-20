package windowedlimiter

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/lytics/go-windowedlimiter/internal/mitigation"
	"github.com/puzpuzpuz/xsync"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Key interface {
	comparable
	fmt.Stringer
}

type Option[K Key] func(*Limiter[K])

func OptionWithLogger[K Key](l *zap.Logger) Option[K] {
	return func(limiter *Limiter[K]) {
		limiter.logger = l
	}
}

// OptionWithBatchDuration sets how long to wait maximum before flushing all
// waiting increments.
//
// This is proportional to both how much over the limit a key can go before
// being mitigated, and inversely proportional to how many writes to the remote
// cache are made.
//
// It is important that this is set to a value that is smaller thant the
// smallest KeyConf interval, or a mitigation will never be triggered.
// Ideally at least 4x smaller, if you want smooth rates.
//
// The default is set with the assumption that per second limits are going to happen.
func OptionWithBatchDuration[K Key](t time.Duration) Option[K] {
	return func(limiter *Limiter[K]) {
		limiter.batchDuration = t
	}
}

// KeyConf is a configuration for a key. It's set lazily per-key by calling
// Options.KeyConfFn
//
// Rate is the number of allowed calls per interval.
//
// Interval is the duration of the sliding window for the rate limit. Larger
// intervals will decrease the requests to redis.
type KeyConf struct {
	Rate     int64
	Interval time.Duration
}

// KeyConfFn is a function that returns a KeyConf for a given key. It will be called lazily and cached forever, unless explicitly evicted.
type KeyConfFn[K Key] func(ctx context.Context, key K) *KeyConf

// New creates a new Limiter with the provided Redis client and options
//
// Note that rdb needs to be carefully configured for timeouts if you expect redis outages to not have severe impacts on latency.
//
// KeyConfFn returns a KeyConf for a given key. This will be called lazily, once per key, until `Limiter.Refresh()` or `Limiter.RefreshKey(key string)` is called.
func New[K Key](ctx context.Context, rdb redis.Cmdable, keyConfFn KeyConfFn[K], options ...Option[K]) *Limiter[K] {
	l := &Limiter[K]{
		logger: zap.NewNop(),
		rdb:    rdb,
		// TODO: expire keyConfCache entries
		keyConfCache: xsync.NewTypedMapOf[K, *KeyConf](func(k K) uint64 {
			return xsync.StrHash64(k.String())
		}),
		keyConfFn:     keyConfFn,
		incrChan:      make(chan K),
		doneChan:      make(chan struct{}),
		batchDuration: 1 * time.Second,
	}
	for _, o := range options {
		o(l)
	}
	allow := func(ctx context.Context, key K) bool {
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

type Limiter[K Key] struct {
	mu              sync.RWMutex
	logger          *zap.Logger
	rdb             redis.Cmdable
	mitigationCache *mitigation.MitigationCache[K]
	keyConfCache    *xsync.MapOf[K, *KeyConf]
	keyConfFn       KeyConfFn[K]
	incrChan        chan K
	doneChan        chan struct{}
	wg              sync.WaitGroup
	batchDuration   time.Duration
}

// Close stops all goroutines, flushes logging, and waits for completion.
func (l *Limiter[K]) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mitigationCache.CloseAll()
	select {
	case l.doneChan <- struct{}{}:
		// Successfully sent done signal
		l.wg.Wait()
	default:
	}
	_ = l.logger.Sync()
}

// SetKeyConfFn sets the KeyConfFn on the limiter and clears all mitigations
func (l *Limiter[K]) SetKeyConfFn(ctx context.Context, keyConfFn KeyConfFn[K]) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Info("setting new key configuration function")
	l.keyConfFn = keyConfFn
	l.Refresh(ctx)
}

// Refresh causes the KeyConfFn to be called again for all keys (lazily).
//
// If your rate limit is changing globally, you should call this once the keyConfFn is returning the new result
func (l *Limiter[K]) Refresh(ctx context.Context) {
	l.logger.Info("refreshing all key configurations")
	l.keyConfCache.Range(func(key K, _ *KeyConf) bool {
		l.keyConfCache.Delete(key)
		return true
	})
	l.mitigationCache.CloseAll()
}

// RefreshKey causes the KeyConfFn to be called again for the specified key (lazily).
//
// If your rate limit is changing for a specific key, you should call this once the keyConfFn is returning the new result
func (l *Limiter[K]) RefreshKey(ctx context.Context, key K) {
	logger := l.logger.With(keyField(key))
	logger.Info("refreshing key configuration")
	l.keyConfCache.Delete(key)
	// for now we just naively nuke any mitigation currently in place and assume it will come back at the new limits if it needs to
	l.mitigationCache.Close(key)
}

// keyConf returns the KeyConf for a given key, calling the KeyConfFn if it hasn't been called for this key yet
func (l *Limiter[K]) keyConf(ctx context.Context, key K) *KeyConf {
	keyConf, ok := l.keyConfCache.Load(key)
	if !ok {
		keyConf = l.keyConfFn(ctx, key)
		if keyConf != nil {
			l.keyConfCache.Store(key, keyConf)
		}
	}
	return keyConf
}

func (l *Limiter[K]) incrementer(ctx context.Context) {
	defer l.wg.Done()

	keysToIncr := make(map[K]int64)
	ticker := time.NewTicker(l.batchDuration)
	defer ticker.Stop()

	for {
		select {
		case <-l.doneChan:
			l.logger.Debug("incrementer stopping via Close(), processing final batch")
			l.processIncrBatch(ctx, keysToIncr)
			return
		case <-ctx.Done():
			l.logger.Debug("incrementer stopping via context cancellation, processing final batch")
			l.processIncrBatch(ctx, keysToIncr)
			return
		case key := <-l.incrChan:
			keysToIncr[key]++
			keyConf := l.keyConf(ctx, key)
			if keyConf == nil {
				continue
			}
			if keysToIncr[key]*3 > keyConf.Rate {
				// short-circuit if we already know a key is headed for mitigation
				l.logger.Debug("key increments approaching limit, flushing immediately", zap.String("key", key.String()), zap.Int64("count", keysToIncr[key]))
				l.processIncrBatch(ctx, keysToIncr)
				keysToIncr = make(map[K]int64) // reset the batch after processing
			}
		case <-ticker.C:
			if len(keysToIncr) == 0 {
				continue
			}
			l.processIncrBatch(ctx, keysToIncr)
			keysToIncr = make(map[K]int64) // reset the batch after processing
		}
	}
}

func (l *Limiter[K]) processIncrBatch(ctx context.Context, batch map[K]int64) {
	if len(batch) == 0 {
		return
	}

	l.mu.RLock()
	rdb := l.rdb
	l.mu.RUnlock()

	if rdb == nil {
		l.logger.Error("redis client is nil, skipping batch processing")
		return
	}

	pipe := rdb.Pipeline()
	cmds := make(map[K]*redis.IntCmd, len(batch))

	now := time.Now()
	for key, count := range batch {
		keyConf := l.keyConf(ctx, key)
		if keyConf == nil {
			continue
		}
		curIntStart := now.Truncate(keyConf.Interval)
		curKey := fmt.Sprintf("{%s}.%d", key, curIntStart.UnixNano())
		l.logger.Debug("sending increments", keyField(key), zap.Int64("count", count))
		cmds[key] = pipe.IncrBy(ctx, curKey, count)
		pipe.Expire(ctx, curKey, max(3*keyConf.Interval, time.Second))
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.ErrClosed) && !errors.Is(err, context.Canceled) {
		l.logger.Error("error executing increment pipeline", zap.Error(err))
	}

	for key, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil && !errors.Is(err, redis.ErrClosed) {
			l.logger.Error("error incrementing key in pipeline", keyField(key), zap.Error(err))
			continue
		}
		keyConf := l.keyConf(ctx, key)
		if keyConf == nil {
			continue
		}
		if res >= keyConf.Rate {
			l.mitigate(ctx, key)
		}
	}
}

func (l *Limiter[K]) checkRedis(ctx context.Context, key K) (bool, error) {
	logger := l.logger.With(keyField(key))

	l.mu.RLock()
	rdb := l.rdb
	l.mu.RUnlock()

	now := time.Now()
	keyConf := l.keyConf(ctx, key)
	if keyConf == nil {
		return true, fmt.Errorf("no key configuration found for key %s", key)
	}
	curIntStart := now.Truncate(keyConf.Interval)
	prevIntStart := curIntStart.Add(-keyConf.Interval)

	pipe := rdb.Pipeline()
	curKey := fmt.Sprintf("{%s}.%d", key, curIntStart.UnixNano())
	prevKey := fmt.Sprintf("{%s}.%d", key, prevIntStart.UnixNano())
	curCmd := pipe.Get(ctx, curKey)
	prevCmd := pipe.Get(ctx, prevKey)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// we failed talking to redis here so we do /not/ try to do any other commands
		// to it, especially not trying to write to a node that can't even do a read
		logger.Error("getting intervals", zap.Error(err))
		return false, err
	}

	var curr, prev int64
	var errCurr, errPrev error

	curVal, err := curCmd.Result()
	if err != nil && err != redis.Nil {
		errCurr = err
	}
	if curVal != "" {
		curr, errCurr = strconv.ParseInt(curVal, 10, 64)
	}

	prevVal, err := prevCmd.Result()
	if err != nil && err != redis.Nil {
		errPrev = err
	}
	if prevVal != "" {
		prev, errPrev = strconv.ParseInt(prevVal, 10, 64)
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

func (l *Limiter[K]) mitigate(ctx context.Context, key K) {
	logger := l.logger.With(keyField(key))
	keyConf := l.keyConf(ctx, key)
	if keyConf == nil {
		return
	}
	period := time.Duration(keyConf.Interval.Nanoseconds() / keyConf.Rate)
	if l.mitigationCache.Trigger(ctx, key, period) {
		logger.Debug("new mitigation")
	}
}

func (l *Limiter[K]) Allow(ctx context.Context, key K) (allowed bool) {
	pprof.Do(ctx, pprof.Labels("key", key.String()), func(ctx context.Context) {
		allowed = l.allow(ctx, key)
	})
	return
}

func (l *Limiter[K]) allow(ctx context.Context, key K) (allowed bool) {
	logger := l.logger.With(keyField(key))
	var err error
	defer func() {
		if err != nil {
			logger.Error("checking redis", zap.Error(err))
		} else if allowed {
			// only increment if we didn't fail talking to redis
			l.incrChan <- key
		}
	}()

	if l.mitigationCache.Allow(ctx, key) {
		logger.Debug("allowed by mitigation")
		return true
	}

	/*
		allowed, err = l.checkRedis(ctx, key)
		logger.Debug("checked by redis", zap.Bool("allowed", allowed), zap.Error(err))
		if err != nil {
			logger.Debug("failed to check redis for rate", zap.Error(err))
			return true // if we can't check redis, we assume the key is allowed
		}
	*/
	allowed = false
	return allowed
}

func (l *Limiter[K]) Wait(ctx context.Context, key K) {
	pprof.Do(ctx, pprof.Labels("key", key.String()), func(ctx context.Context) {
		l.wait(ctx, key)
	})
}

func (l *Limiter[K]) wait(ctx context.Context, key K) {
	logger := l.logger.With(keyField(key))
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

func keyField[K Key](key K) zap.Field {
	return zap.String("slidingwindow/key", key.String())
}
