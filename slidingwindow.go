package slidingwindow

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	rdb             redis.Cmdable
	zl              zap.Logger
	mitigationCache = sync.Map{}
	// these could use prom/otel but I'm avoiding the dependency for now
	allows               atomic.Uint64
	denies               atomic.Uint64
	mitigatedCacheMisses atomic.Uint64
	mitigatedCacheHits   atomic.Uint64
	mitigatedCacheWrites atomic.Uint64
	redisMGets           atomic.Uint64
	redisGets            atomic.Uint64
	redisSets            atomic.Uint64
	redisIncrs           atomic.Uint64
	redisExpireNXs       atomic.Uint64
	redisErrors          atomic.Uint64
)

type incrOp struct {
	key      string
	interval time.Duration
}

var (
	incrChan            = make(chan incrOp, 1)
	cleanerStopChan     = make(chan struct{})
	incrementerStopChan = make(chan struct{})
)

func mitigationCacheCleaner() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-cleanerStopChan:
			return
		case <-ticker.C:
			mitigationCache.Range(func(key, u any) bool {
				if time.Now().After(u.(time.Time)) {
					mitigationCache.Delete(key)
				}
				return false
			})
		}
	}
}

func incrementer() {
	for {
		select {
		case <-incrementerStopChan:
			return
		case op := <-incrChan:
			res, err := rdb.Incr(context.Background(), op.key).Result()
			if err == nil && res == 1 {
				rdb.ExpireNX(context.Background(), op.key, max(3*op.interval, time.Second))
				redisExpireNXs.Add(1)
			}
			redisIncrs.Add(1)
			if err != nil {
				redisErrors.Add(1)
			}
		}
	}
}

func Init(r redis.Cmdable, z zap.Logger) func() {
	rdb = r
	zl = z
	go mitigationCacheCleaner()
	go incrementer()
	return func() {
		close(cleanerStopChan)
		close(incrementerStopChan)
	}
}

type Options struct {
	FailClosed bool
}

type RateLimiter struct {
	key      string
	rate     int64
	interval time.Duration
	options  Options
}

func NewRateLimiter(key string, rate int64, interval time.Duration, opt ...Options) *RateLimiter {
	var options Options
	if len(opt) > 0 {
		options = opt[0]
	}
	return &RateLimiter{
		key:      key,
		rate:     rate,
		interval: interval,
		options:  options,
	}
}

func (rl *RateLimiter) failMaybe() time.Time {
	if rl.options.FailClosed {
		return time.Now().Add(rl.interval)
	}
	return time.Time{}
}

func (rl *RateLimiter) AllowWithUntil(ctx context.Context) (notUntil time.Time) {
	notUntil = time.Time{}
	defer func() {
		if time.Now().Before(notUntil) {
			allows.Add(1)
		} else {
			denies.Add(1)
		}
	}()

	until := rl.mitigatedUntil(ctx)
	if time.Now().Before(until) {
		return until
	}
	now := time.Now()
	curIntStart := now.Truncate(rl.interval)
	prevIntStart := curIntStart.Add(-rl.interval)
	intervals, err := rdb.MGet(ctx,
		fmt.Sprintf("{%s}.%d", rl.key, curIntStart.UnixNano()),
		fmt.Sprintf("{%s}.%d", rl.key, prevIntStart.UnixNano()),
	).Result()
	redisMGets.Add(1)
	if err != nil {
		// we failed talking to redis here so we do /not/ try to do any other commands
		// to it, especially not trying to write to a node that can't even do a read
		zl.Error("getting intervals", zap.Error(err))
		redisErrors.Add(1)
		return rl.failMaybe()
	}

	// after this point we do want to try to increment the current interval if we allow the request
	defer func() {
		if time.Now().After(notUntil) {
			curKey := fmt.Sprintf("{%s}.%d", rl.key, curIntStart.UnixNano())
			incrChan <- incrOp{key: curKey, interval: rl.interval}
		}
	}()
	if intervals[0] != nil {
		curr, err := strconv.ParseInt(intervals[0].(string), 10, 64)
		if err != nil {
			return rl.failMaybe()
		}
		// zl.Debug("current count", zap.Any("count", curr))

		var prev int64
		if intervals[1] == nil {
			prev = 0
		} else {
			prev, err = strconv.ParseInt(intervals[1].(string), 10, 64)
			if err != nil {
				return rl.failMaybe()
			}
		}
		curIntAgo := now.Sub(curIntStart)
		portionOfLastInt := rl.interval - curIntAgo
		percentOfLastInt := float64(portionOfLastInt.Nanoseconds()) / float64(rl.interval.Nanoseconds())
		rawOfLastInt := int64(float64(prev) * percentOfLastInt)
		rateEstimate := int64(rawOfLastInt) + curr
		if rateEstimate >= rl.rate {
			// right now this is mitigating based on the portion of the window that has
			// to pass for it to be possible to get an allow (unless my math is wrong).
			// This causes a ridiculous number of hits to redis though
			notUntil = time.Now().Add(time.Duration(rl.interval.Nanoseconds() / rl.rate))
			rl.mitigate(ctx, notUntil)
		} else {
			notUntil = time.Time{}
		}
		zl.Debug("rate check",
			zap.Int64("prev", prev),
			zap.Int64("curr", curr),
			zap.Duration("curIntAgo", curIntAgo),
			zap.Float64("percentOfLastInt", percentOfLastInt*100),
			zap.Int64("rawOfLastInt", rawOfLastInt),
			zap.Int64("rateEstimate", rateEstimate),
			zap.Int64("rl.rate", rl.rate),
		)
		return
	}
	// zl.Debug("no intervals, allowing", zap.String("key", rl.key))
	return time.Time{}
}

func (rl *RateLimiter) Allow(ctx context.Context) (ret bool) {
	return time.Now().After(rl.AllowWithUntil(ctx))
}

func (rl *RateLimiter) Wait(ctx context.Context) {
WAIT:
	if rl.Allow(ctx) {
		return
	}
	until := rl.mitigatedUntil(ctx)
	if until.IsZero() {
		return
	}

	minDuration := rl.interval / time.Duration(rl.rate)
	maxDuration := time.Until(until)
	waitRange := max(maxDuration-minDuration, maxDuration)
	randomOffset := time.Duration(float64(waitRange) * float64(rand.Float64()))
	scaledDuration := minDuration + randomOffset

	zl.Debug("waiting",
		zap.String("key", rl.key),
		zap.Duration("for", scaledDuration),
		zap.Duration("minDuration", minDuration),
		zap.Duration("maxDuration", maxDuration),
		zap.Duration("waitRange", waitRange),
	)
	time.Sleep(scaledDuration)
	goto WAIT
}

func (rl *RateLimiter) mitigate(ctx context.Context, until time.Time) {
	zl.Debug("mitigating",
		zap.String("key", rl.key),
		zap.Duration("for", time.Until(until)),
	)
	mitigationCache.Store(rl.key, until)
	mitigatedCacheWrites.Add(1)
	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		mKey := fmt.Sprintf("{%s}.m", rl.key)
		pipe.Set(ctx, mKey, until.UnixNano(), max(time.Until(until), time.Millisecond))
		return nil
	})
	redisSets.Add(1)
	if err != nil {
		redisErrors.Add(1)
	}
}

func (rl *RateLimiter) isMitigated(ctx context.Context) bool {
	return time.Now().Before(rl.mitigatedUntil(ctx))
}

func (rl *RateLimiter) mitigatedUntil(ctx context.Context) (until time.Time) {
	/*
		defer func() {
			if !until.IsZero() && time.Now().Before(until) {
				zl.Debug("mitigated",
					zap.String("key", rl.key),
					zap.Duration("for", time.Until(until)))
			}
		}()
	*/
	if u, ok := mitigationCache.Load(rl.key); ok {
		mitigatedCacheHits.Add(1)
		return u.(time.Time)
	}
	mitigatedCacheMisses.Add(1)
	v, err := rdb.Get(ctx, fmt.Sprintf("{%s}.m", rl.key)).Result()
	redisGets.Add(1)
	if err != nil {
		if err == redis.Nil {
			return time.Time{}
		}
		if rl.options.FailClosed {
			// it's kinda arbitrary to return the next interval here
			redisErrors.Add(1)
			return time.Now().Add(rl.interval)
		}
		return time.Time{}
	}
	unixNanos, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		if rl.options.FailClosed {
			// it's kinda arbitrary to return the next interval here. What if an interval
			// is an hour?
			redisErrors.Add(1)
			return time.Now().Add(rl.interval)
		}
		return time.Time{}
	}
	mitigatedUntil := time.Unix(0, unixNanos)
	if time.Until(mitigatedUntil) > 0 {
		mitigationCache.Store(rl.key, mitigatedUntil)
		mitigatedCacheWrites.Add(1)
		return mitigatedUntil
	}
	return time.Time{}
}
