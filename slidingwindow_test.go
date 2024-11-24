package slidingwindow

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/fgrosse/zaptest"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitaminmoo/go-slidingwindow/internal/mitigation"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRateLimiterBasic(t *testing.T) {
	ctx := context.Background()
	rate := int64(10)
	interval := 100 * time.Millisecond
	l, key := setup(t, ctx, rate, interval)

	allowed := 0
	for i := 0; i < 15; i++ {
		if l.Allow(ctx, key) {
			time.Sleep(2 * time.Millisecond) // due to async incrementer
			allowed++
		}
	}
	assert.Equal(t, 10, allowed)

	now := time.Now()
	l.Wait(ctx, key)
	assert.WithinDuration(t, time.Now(), now, interval)

	for i := 0; i < 10; i++ {
		now := time.Now()
		l.Wait(ctx, key)
		assert.WithinDuration(t, time.Now(), now, 11*time.Millisecond)
	}
}

func TestRateLimiterConcurrent(t *testing.T) {
	ctx := context.Background()
	rate := int64(100)
	interval := 500 * time.Millisecond
	l, key := setup(t, ctx, rate, interval)
	granularity := time.Duration(interval.Nanoseconds() / 4)

	var wg sync.WaitGroup
	var durations []time.Duration
	timesChan := make(chan time.Time, 10000)
	durationsChan := make(chan time.Duration, 10000)
	intervals := make(map[time.Time]int64)
	done := make(chan struct{})
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go pprof.Do(ctx, pprof.Labels("name", "collectorWg"), func(context.Context) {
		defer collectorWg.Done()
		for {
			select {
			case ts := <-timesChan:
				intervals[ts.Truncate(granularity)]++
			case d := <-durationsChan:
				durations = append(durations, d)
			case <-done:
				// Drain any remaining items from channels
				for {
					select {
					case ts := <-timesChan:
						intervals[ts.Truncate(granularity)]++
					case d := <-durationsChan:
						durations = append(durations, d)
					default:
						return
					}
				}
			}
		}
	})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		labels := pprof.Labels("name", "testWaiter")
		go pprof.Do(ctx, labels, func(context.Context) {
			defer wg.Done()
			for j := 0; int64(j) < rate; j++ {
				now := time.Now()
				l.Wait(ctx, key)
				timesChan <- time.Now()
				durationsChan <- time.Since(now)
				time.Sleep(1 * time.Millisecond)
			}
		})
	}

	wg.Wait()
	done <- struct{}{}
	collectorWg.Wait()

	// require.False(t, mitigation.Allow(ctx, key), "should still be mitigated")
	time.Sleep(interval * 3)
	require.True(t, mitigation.Allow(ctx, key), "should not be mitigated")

	analyzeIntervals(t, l.logger, interval, granularity, rate, intervals)

	total := len(durations)
	assert.NotZero(t, total, "didn't record any requests")

	l.logger.Sugar().Infof("total sent: %d", len(durations))
}

func setup(t *testing.T, ctx context.Context, rate int64, interval time.Duration) (*Limiter, string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	config := zaptest.Config()
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	zaptest.Config = func() zapcore.EncoderConfig { return config }
	logger := zaptest.Logger(t)

	key := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()%1000)
	keyConfFn := func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{rate: rate, interval: interval}
	}
	l := New(ctx, rdb, &Options{
		Logger:    logger,
		KeyConfFn: keyConfFn,
	})
	t.Cleanup(func() {
		l.Close()
		rdb.Close()
	})
	// avoid testing over the first interval wrap, which can cause more requests to
	// be allowed
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))
	return l, key
}

func analyzeIntervals(t *testing.T, logger *zap.Logger, _ time.Duration, granularity time.Duration, rate int64, intervals map[time.Time]int64) {
	t.Helper()
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	var minInterval time.Time
	var maxInterval time.Time
	for s := range intervals {
		if minInterval.IsZero() || s.Before(minInterval) {
			minInterval = s
		}
		if maxInterval.IsZero() || s.After(maxInterval) {
			maxInterval = s
		}
	}
	for i := minInterval; i.Before(maxInterval); i = i.Add(granularity) {
		logger.Sugar().Infof("requests in interval %v: %d", i.Format("05.000"), intervals[i])
	}
	assert.GreaterOrEqual(t, intervals[minInterval.Add(granularity*0)], rate, "first interval should have at least rate requests")
	assert.Equal(t, intervals[minInterval.Add(granularity*1)], int64(0), "second interval should have zero requests")
}
