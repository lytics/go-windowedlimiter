package slidingwindow

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vitaminmoo/go-slidingwindow/internal/mitigation"
	"github.com/yuseferi/zax/v2"
	"go.uber.org/zap"
)

type Options struct {
	Logger     *zap.Logger                                    // The zap logger to use for logging. Will default to not logging anything
	KeyConfFn  func(ctx context.Context, key string) *KeyConf // A function that returns the KeyConf for a given key. This will be called lazily, once per key, until `Limiter.Refresh()` or `Limiter.RefreshKey(key string)` is called
	FailClosed bool                                           // Whether or not to deny requests/block if the limiter is unable to determine if a request should be allowed. Note that due to the mitigation cache, this currently only has any effect when a key is already in the mitigation cache
}

type KeyConf struct {
	rate      int64
	interval  time.Duration
	firstUsed time.Time
	lastUsed  time.Time
}

// New creates a new Limiter with the provided redis client and options
func New(ctx context.Context, rdb redis.Cmdable, options ...*Options) *Limiter {
	l := &Limiter{
		logger: zap.NewNop(),
		rdb:    rdb,
		// TODO: expire keyConfCache entries
		keyConfCache: sync.Map{},
		keyConfFn:    func(ctx context.Context, key string) *KeyConf { return &KeyConf{rate: 100, interval: time.Second} },
		incrChan:     make(chan string),
		doneChan:     make(chan struct{}),
		failClosed:   false,
	}
	if len(options) > 0 {
		l.logger = options[0].Logger
		l.keyConfFn = options[0].KeyConfFn
		l.failClosed = options[0].FailClosed
	}
	l.wg.Add(1)
	go pprof.Do(ctx, pprof.Labels("name", "slidingwindow_incrementer"), func(ctx context.Context) { l.incrementer(ctx) })
	return l
}

type Limiter struct {
	logger       *zap.Logger
	rdb          redis.Cmdable
	keyConfCache sync.Map
	keyConfFn    func(ctx context.Context, key string) *KeyConf
	incrChan     chan string
	doneChan     chan struct{}
	wg           sync.WaitGroup
	failClosed   bool
}

// Close stops all goroutines, flushes logging, and waits for completion
func (l *Limiter) Close() {
	l.doneChan <- struct{}{}
	l.logger.Sync()
	l.wg.Wait()
}

// Refresh causes the KeyConfFn to be called again for all keys (lazily)
// If your rate limit is changing globally, you should call this once the keyConfFn is returning the new result
func (l *Limiter) Refresh(ctx context.Context) {
	l.keyConfCache.Clear()
}

// RefreshKey causes the KeyConfFn to be called again for the specified key (lazily)
// If your rate limit is changing for a specific key, you should call this once the keyConfFn is returning the new result
func (l *Limiter) RefreshKey(ctx context.Context, key string) {
	l.keyConfCache.Delete(key)
}

func (l *Limiter) keyConf(ctx context.Context, key string) *KeyConf {
	keyConf, ok := l.keyConfCache.Load(key)
	if !ok {
		keyConf = l.keyConfFn(ctx, key)
		l.keyConfCache.Store(key, keyConf)
	}
	return keyConf.(*KeyConf)
}

func (l *Limiter) increment(ctx context.Context, key string) {
	ctx = zax.Set(ctx, []zap.Field{zap.String("key", key)})
	logger := l.logger.With(zax.Get(ctx)...)
	keyConf := l.keyConf(ctx, key)
	curIntStart := time.Now().Truncate(keyConf.interval)
	curKey := fmt.Sprintf("{%s}.%d", key, curIntStart.UnixNano())
	res, err := l.rdb.Incr(context.Background(), curKey).Result()
	if err != nil && !errors.Is(err, redis.ErrClosed) {
		logger.Error("error incrementing key", zap.String("key", curKey), zap.Error(err))
	}
	if err == nil && res == 1 {
		l.rdb.ExpireNX(context.Background(), key, max(3*keyConf.interval, time.Second))
	}
	if res >= keyConf.rate {
		l.mitigate(ctx, key)
	}
}

func (l *Limiter) incrementer(ctx context.Context) {
	logger := l.logger.With(zax.Get(ctx)...)
	defer l.wg.Done()
	for {
		select {
		case <-l.doneChan:
			logger.Debug("incrementer stopping via Close()")
			return
		case <-ctx.Done():
			logger.Debug("incrementer stopping via context cancellation")
			return
		case key := <-l.incrChan:
			l.increment(ctx, key)
		}
	}
}

func (l *Limiter) checkRedis(ctx context.Context, key string) (bool, error) {
	logger := l.logger.With(zax.Get(ctx)...)

	now := time.Now()
	keyConf := l.keyConf(ctx, key)
	curIntStart := now.Truncate(keyConf.interval)
	prevIntStart := curIntStart.Add(-keyConf.interval)
	intervals, err := l.rdb.MGet(ctx,
		fmt.Sprintf("{%s}.%d", key, curIntStart.UnixNano()),
		fmt.Sprintf("{%s}.%d", key, prevIntStart.UnixNano()),
	).Result()
	if err != nil {
		// we failed talking to redis here so we do /not/ try to do any other commands
		// to it, especially not trying to write to a node that can't even do a read
		logger.Error("getting intervals", zap.Error(err))
		return false, err
	}

	if intervals[0] != nil {
		curr, err := strconv.ParseInt(intervals[0].(string), 10, 64)
		if err != nil {
			return false, err
		}
		var prev int64
		if intervals[1] == nil {
			prev = 0
		} else {
			prev, err = strconv.ParseInt(intervals[1].(string), 10, 64)
			if err != nil {
				return false, err
			}
		}
		curIntAgo := now.Sub(curIntStart)
		portionOfLastInt := keyConf.interval - curIntAgo
		percentOfLastInt := float64(portionOfLastInt.Nanoseconds()) / float64(keyConf.interval.Nanoseconds())
		rawOfLastInt := int64(float64(prev) * percentOfLastInt)
		rateEstimate := int64(rawOfLastInt) + curr
		if rateEstimate >= keyConf.rate {
			return false, nil
		}
	}
	return true, nil
}

func (l *Limiter) mitigate(ctx context.Context, key string) {
	logger := l.logger.With(zax.Get(ctx)...)
	keyConf := l.keyConf(ctx, key)
	period := time.Duration(keyConf.interval.Nanoseconds() / keyConf.rate)
	allow := func(ctx context.Context) bool {
		allowed, err := l.checkRedis(ctx, key)
		if err != nil {
			logger.Debug("failed to check redis for mitigation status", zap.Error(err))
			return !l.failClosed
		}
		return allowed
	}
	logger.Debug("mitigating", zap.Duration("period", period))
	mitigation.Trigger(ctx, key, period, allow)
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
		} else {
			if allowed {
				// only increment if we didn't fail talking to redis
				l.incrChan <- key
			}
		}
	}()
	if mitigation.Allow(ctx, key) {
		logger.Debug("allowed by mitigation")
		return true
	}
	allowed, err = l.checkRedis(ctx, key)
	logger.Debug("checked by redis", zap.Bool("allowed", allowed), zap.Error(err))
	if err != nil {
		logger.Debug("failed to check redis for rate", zap.Error(err))
		return !l.failClosed
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
	// now := time.Now()
	retries := 0
RETRY: // TODO: exponential backoff?
	err := mitigation.Wait(ctx, key)
	if err != nil {
		logger.Error("waiting", zap.Error(err))
		retries++
		goto RETRY
	}
	l.incrChan <- key
	if retries > 0 {
		logger.Warn("waiter retried", zap.Int("count", retries))
	}
}
