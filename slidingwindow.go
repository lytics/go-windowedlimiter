package slidingwindow

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vitaminmoo/go-slidingwindow/internal/mitigation"
	"go.uber.org/zap"
)

var zl zap.Logger

var incrChan = make(chan *RateLimiter, 1)

func incrementer(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			zl.Debug("incrementer stopping")
			return
		case rl := <-incrChan:
			curIntStart := time.Now().Truncate(rl.interval)
			curKey := fmt.Sprintf("{%s}.%d", rl.key, curIntStart.UnixNano())
			res, err := rl.rdb.Incr(context.Background(), curKey).Result()
			if err != nil {
				zl.Error("error incrementing key", zap.String("key", curKey), zap.Error(err))
			}
			if err == nil && res == 1 {
				rl.rdb.ExpireNX(context.Background(), rl.key, max(3*rl.interval, time.Second))
			}
			if res >= rl.rate {
				zl.Debug("mitigating due to current window being full in incrementer", zap.String("key", rl.key), zap.Int64("rate", rl.rate), zap.Duration("interval", rl.interval), zap.Int64("res", res))
				rl.mitigate(ctx)
			}
		}
	}
}

func Init(ctx context.Context, z zap.Logger) func() {
	ctx, cancel := context.WithCancel(ctx)
	zl = z
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go pprof.Do(ctx, pprof.Labels("name", "incrementer"), func(ctx context.Context) { incrementer(wg, ctx) })
	return func() {
		cancel()
		wg.Wait()
	}
}

type Options struct {
	FailOpen bool
}

type RateLimiter struct {
	rdb      redis.Cmdable
	key      string
	rate     int64
	interval time.Duration
	options  Options
}

func NewRateLimiter(rdb redis.Cmdable, key string, rate int64, interval time.Duration, opt ...Options) *RateLimiter {
	var options Options
	if len(opt) > 0 {
		options = opt[0]
	}
	return &RateLimiter{
		rdb:      rdb,
		key:      key,
		rate:     rate,
		interval: interval,
		options:  options,
	}
}

func (rl *RateLimiter) checkRedis(ctx context.Context) (bool, error) {
	now := time.Now()
	curIntStart := now.Truncate(rl.interval)
	prevIntStart := curIntStart.Add(-rl.interval)
	intervals, redisErr := rl.rdb.MGet(ctx,
		fmt.Sprintf("{%s}.%d", rl.key, curIntStart.UnixNano()),
		fmt.Sprintf("{%s}.%d", rl.key, prevIntStart.UnixNano()),
	).Result()
	if redisErr != nil {
		// we failed talking to redis here so we do /not/ try to do any other commands
		// to it, especially not trying to write to a node that can't even do a read
		zl.Error("getting intervals", zap.Error(redisErr))
		return false, redisErr
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
		portionOfLastInt := rl.interval - curIntAgo
		percentOfLastInt := float64(portionOfLastInt.Nanoseconds()) / float64(rl.interval.Nanoseconds())
		rawOfLastInt := int64(float64(prev) * percentOfLastInt)
		rateEstimate := int64(rawOfLastInt) + curr
		if rateEstimate >= rl.rate {
			return false, nil
		}
	}
	return true, nil
}

func (rl *RateLimiter) mitigate(ctx context.Context) {
	period := time.Duration(rl.interval.Nanoseconds() / rl.rate)
	allow := func(ctx context.Context) bool {
		allowed, err := rl.checkRedis(ctx)
		if err != nil {
			allowed = allowed || !rl.options.FailOpen
			zl.Debug("mitigation check (err)", zap.String("key", rl.key), zap.Bool("allowed", allowed))
			return allowed
		}
		zl.Debug("mitigation check", zap.String("key", rl.key), zap.Bool("allowed", allowed))
		return allowed
	}
	zl.Debug("mitigating", zap.String("key", rl.key), zap.Duration("period", period))
	mitigation.Trigger(ctx, rl.key, period, allow)
}

func (rl *RateLimiter) Allow(ctx context.Context) (allowed bool) {
	pprof.Do(ctx, pprof.Labels("key", rl.key), func(ctx context.Context) {
		allowed = rl.allow(ctx)
	})
	return
}

func (rl *RateLimiter) allow(ctx context.Context) (allowed bool) {
	zl := zl.With(zap.String("key", rl.key))
	var err error
	defer func() {
		if err != nil {
			zl.Error("checking redis", zap.Error(err))
		} else {
			if allowed {
				// only increment if we didn't fail talking to redis
				incrChan <- rl
			}
		}
	}()
	if mitigation.Allow(ctx, rl.key) {
		zl.Debug("allowed by mitigation")
		return true
	}
	allowed, err = rl.checkRedis(ctx)
	zl.Debug("checked by redis", zap.Bool("allowed", allowed), zap.Error(err))
	if err != nil {
		return allowed || !rl.options.FailOpen
	}
	return allowed
}

func (rl *RateLimiter) Wait(ctx context.Context) {
	pprof.Do(ctx, pprof.Labels("key", rl.key), func(ctx context.Context) {
		rl.wait(ctx)
	})
}

func (rl *RateLimiter) wait(ctx context.Context) {
	now := time.Now()
RETRY: // TODO: exponential backoff?
	err := mitigation.Wait(ctx, rl.key)
	if err != nil {
		zl.Error("waiting", zap.Error(err))
		goto RETRY
	}
	incrChan <- rl
	zl.Debug("wait woke up", zap.String("key", rl.key), zap.Duration("wait", time.Since(now)))
}
