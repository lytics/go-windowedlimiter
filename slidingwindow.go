package slidingwindow

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	mitigationCache      = sync.Map{}
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

func mitigationCacheCleaner() {
	time.Sleep(time.Minute)
	mitigationCache.Range(func(key, u any) bool {
		if time.Now().After(u.(time.Time)) {
			mitigationCache.Delete(key)
		}
		return false
	})
}

type incrOp struct {
	rdb      redis.Cmdable
	key      string
	interval time.Duration
}

var incrChan = make(chan incrOp, 1)

func incrementer() {
	for op := range incrChan {
		res, err := op.rdb.Incr(context.Background(), op.key).Result()
		if err == nil && res == 1 {
			op.rdb.ExpireNX(context.Background(), op.key, max(3*op.interval, time.Second))
			redisExpireNXs.Add(1)
		}
		redisIncrs.Add(1)
		if err != nil {
			redisErrors.Add(1)
		}
	}
}

func init() {
	go mitigationCacheCleaner()
	go incrementer()
}

type Options struct {
	FailClosed bool
	ZapLogger  *zap.Logger
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
	if options.ZapLogger == nil {

		zl, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}

		zl = zap.NewNop()
		options.ZapLogger = zl
	}
	return &RateLimiter{
		rdb:      rdb,
		key:      key,
		rate:     rate,
		interval: interval,
		options:  options,
	}
}

func (rl *RateLimiter) Allow(ctx context.Context) (ret bool) {
	defer func() {
		if ret {
			allows.Add(1)
		} else {
			denies.Add(1)
		}
	}()
	if rl.isMitigated(ctx) {
		return false
	}
	now := time.Now()
	curIntStart := now.Truncate(rl.interval)
	prevIntStart := curIntStart.Add(-rl.interval)
	intervals, err := rl.rdb.MGet(ctx,
		fmt.Sprintf("{%s}.%d", rl.key, curIntStart.UnixNano()),
		fmt.Sprintf("{%s}.%d", rl.key, prevIntStart.UnixNano()),
	).Result()
	redisMGets.Add(1)
	if err != nil {
		// we failed talking to redis here so we do /not/ try to do any other commands
		// to it, especially not trying to write to a node that can't even do a read
		rl.options.ZapLogger.Error("getting intervals", zap.Error(err))
		redisErrors.Add(1)
		return !rl.options.FailClosed
	}

	// after this point we do want to try to increment the current interval if we allow the request
	defer func() {
		if ret {
			curKey := fmt.Sprintf("{%s}.%d", rl.key, curIntStart.UnixNano())
			incrChan <- incrOp{rdb: rl.rdb, key: curKey, interval: rl.interval}
		} else {
			// right now this is mitigating based on the portion of the window that has
			// to pass for it to be possible to get an allow (unless my math is wrong).
			// This causes a ridiculous number of hits to redis though
			rl.mitigate(ctx, curIntStart.Add(time.Duration(rl.interval.Nanoseconds()/rl.rate)))
		}
	}()
	if intervals[0] != nil {
		curr, err := strconv.ParseInt(intervals[0].(string), 10, 64)
		if err != nil {
			return !rl.options.FailClosed
		}
		// rl.options.ZapLogger.Debug("current count", zap.Any("count", curr))

		var prev int64
		if intervals[1] == nil {
			prev = 0
		} else {
			prev, err = strconv.ParseInt(intervals[1].(string), 10, 64)
			if err != nil {
				return !rl.options.FailClosed
			}
		}
		curIntAgo := now.Sub(curIntStart)
		portionOfLastInt := rl.interval - curIntAgo
		percentOfLastInt := float64(portionOfLastInt.Nanoseconds()) / float64(rl.interval.Nanoseconds())
		rawOfLastInt := int64(float64(prev) * percentOfLastInt)
		rateEstimate := int64(rawOfLastInt) + curr
		if rateEstimate >= rl.rate {
			ret = false
		} else {
			ret = true
		}
		rl.options.ZapLogger.Debug("rate ok",
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
	// rl.options.ZapLogger.Debug("no intervals, allowing", zap.String("key", rl.key))
	return true
}

func (rl *RateLimiter) Wait(ctx context.Context) {
WAIT:
	if rl.Allow(ctx) {
		return
	}
	until := rl.mitigatedUntil(ctx)

	// Calculate logarithmic wait duration between current until and full interval
	fullDuration := rl.interval
	currentDuration := time.Until(until)
	if currentDuration <= 0 {
		currentDuration = time.Millisecond
	}

	// an LLM wrote this and I have no idea if it's actually a good distribution to use
	logScale := math.Log(float64(fullDuration)) / math.Log(float64(currentDuration))
	scaledDuration := time.Duration(float64(currentDuration) * logScale)

	if scaledDuration > fullDuration {
		scaledDuration = fullDuration
	}

	rl.options.ZapLogger.Debug("waiting",
		zap.String("key", rl.key),
		zap.Duration("for", scaledDuration),
	)
	time.Sleep(scaledDuration)
	goto WAIT
}

func (rl *RateLimiter) mitigate(ctx context.Context, until time.Time) {
	rl.options.ZapLogger.Debug("mitigating",
		zap.String("key", rl.key),
		zap.Duration("for", time.Until(until)),
	)
	mitigationCache.Store(rl.key, until)
	mitigatedCacheWrites.Add(1)
	_, err := rl.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
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
				rl.options.ZapLogger.Debug("mitigated",
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
	v, err := rl.rdb.Get(ctx, fmt.Sprintf("{%s}.m", rl.key)).Result()
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
	if time.Now().Before(mitigatedUntil) {
		mitigationCache.Store(rl.key, mitigatedUntil)
		mitigatedCacheWrites.Add(1)
		return mitigatedUntil
	}
	return time.Time{}
}
