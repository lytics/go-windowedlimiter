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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestBasic(t *testing.T) {
	ctx := context.Background()
	rate := int64(10)
	interval := 100 * time.Millisecond
	l, key := setup(t, ctx, rate, interval)

	allowed := 0
	for range 15 {
		if l.Allow(ctx, key) {
			allowed++
		}
	}
	assert.GreaterOrEqual(t, allowed, int(rate))

	// wait for batch processing and mitigation
	time.Sleep(interval)
	require.False(t, l.Allow(ctx, key), "should be blocked after burst")

	now := time.Now()
	l.Wait(ctx, key)
	// The first wait after mitigation might take longer, but subsequent ones should be paced.
	assert.WithinDuration(t, time.Now(), now, interval, "first wait should be within an interval")

	for i := range 10 {
		now := time.Now()
		l.Wait(ctx, key)
		assert.WithinDuration(t, time.Now(), now, 12*time.Millisecond, "try #%d wasn't within required duration", i)
	}
}

func TestConcurrent(t *testing.T) {
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

	for range 10 {
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
	require.True(t, l.mitigationCache.Allow(ctx, key), "should not be mitigated")

	analyzeIntervals(t, l.logger, interval, granularity, rate, intervals)

	total := len(durations)
	assert.NotZero(t, total, "didn't record any requests")

	l.logger.Sugar().Infof("total sent: %d", len(durations))
}

func TestRefreshKey(t *testing.T) {
	ctx := context.Background()
	rate := int64(5)
	interval := 100 * time.Millisecond
	l, key := setup(t, ctx, rate, interval)

	for range 5 {
		require.True(t, l.Allow(ctx, key), "should allow initial requests")
	}
	time.Sleep(50 * time.Millisecond) // let incrementer run

	require.False(t, l.Allow(ctx, key), "should not allow after rate limit is hit")

	// Refresh key to a higher rate limit
	l.keyConfFn = func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{Rate: 15, Interval: interval}
	}
	l.RefreshKey(ctx, key)

	for i := range 10 {
		require.True(t, l.Allow(ctx, key), "should allow requests after refreshing key conf, attempt %d", i+1)
	}
	time.Sleep(50 * time.Millisecond) // let incrementer run
	allowed, err := l.checkRedis(ctx, key)
	require.NoError(t, err)
	require.True(t, allowed, "checkRedis should allow after refreshing key conf")
}

func TestRefresh(t *testing.T) {
	ctx := context.Background()
	rate := int64(5)
	interval := 100 * time.Millisecond
	l, key1 := setup(t, ctx, rate, interval)
	key2 := key1 + "-key2"

	for range 5 {
		require.True(t, l.Allow(ctx, key1), "should allow initial requests for key 1")
		require.True(t, l.Allow(ctx, key2), "should allow initial requests for key 2")
	}
	time.Sleep(50 * time.Millisecond) // let incrementer run

	require.False(t, l.Allow(ctx, key1), "should not allow for key 1 after rate limit is hit")
	require.False(t, l.Allow(ctx, key2), "should not allow for key 2 after rate limit is hit")

	// Refresh all keys to a higher rate limit
	l.keyConfFn = func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{Rate: 15, Interval: interval}
	}
	l.Refresh(ctx)

	for range 10 {
		require.True(t, l.Allow(ctx, key1), "should allow requests for key 1 after refreshing")
		require.True(t, l.Allow(ctx, key2), "should allow requests for key 2 after refreshing")
	}
}

func TestWait_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	rate := int64(1)
	interval := 100 * time.Millisecond
	l, key := setup(t, ctx, rate, interval)

	// Exceed the rate limit to trigger mitigation.
	l.Allow(ctx, key)
	time.Sleep(50 * time.Millisecond) // let incrementer run
	l.Allow(ctx, key)
	time.Sleep(50 * time.Millisecond) // let incrementer run
	require.False(t, l.mitigationCache.Allow(ctx, key), "should be mitigated")

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	l.Wait(waitCtx, key) // This should return quickly due to context cancellation
	duration := time.Since(start)

	require.ErrorIs(t, waitCtx.Err(), context.DeadlineExceeded, "context should be cancelled")
	require.Less(t, duration, 50*time.Millisecond, "Wait should return promptly after context is canceled")
}

func TestClose(t *testing.T) {
	ctx := context.Background()
	l, _ := setup(t, ctx, 10, time.Second)
	// close manually
	l.Close()
	// Close is called automatically via t.Cleanup inside setup() and should not error or block even with us already having closed
}

func TestConcurrent_AllowAndRefreshKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, key := setup(t, ctx, 100, 500*time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(10)

	// Goroutine to refresh the key repeatedly.
	go func() {
		for i := range 50 {
			rate := int64(100 + i)
			l.keyConfFn = func(ctx context.Context, key string) *KeyConf {
				return &KeyConf{Rate: rate, Interval: 500 * time.Millisecond}
			}
			l.RefreshKey(ctx, key)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Multiple goroutines calling Allow.
	for range 10 {
		go func() {
			defer wg.Done()
			for j := range 100 {
				l.Allow(ctx, key)
				time.Sleep(time.Duration(j%5) * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	// The test passes if it completes without the race detector firing.
}

func TestAllow_AsyncIncrementRace(t *testing.T) {
	ctx := context.Background()
	rate := int64(10)
	interval := 500 * time.Millisecond
	l, key := setup(t, ctx, rate, interval)

	// Fire a burst of requests. More than the rate limit may be allowed initially
	// because of the async incrementer.
	allowedCount := 0
	for range 15 {
		if l.Allow(ctx, key) {
			allowedCount++
		}
	}

	t.Logf("Allowed %d requests initially in a burst", allowedCount)
	assert.GreaterOrEqual(t, allowedCount, int(rate), "at least 'rate' should be allowed")

	// Wait for incrementer to catch up and mitigation to trigger.
	time.Sleep(100 * time.Millisecond)

	// Now, subsequent requests should be denied.
	assert.False(t, l.Allow(ctx, key), "should not be allowed after incrementer catches up")
	assert.False(t, l.mitigationCache.Allow(ctx, key), "should be mitigated")
}

func BenchmarkAllow(b *testing.B) {
	ctx := context.Background()
	rate := int64(b.N + 1) // Ensure we don't hit the rate limit
	interval := 1 * time.Minute
	l, key := setupBench(b, ctx, rate, interval)

	for b.Loop() {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_Contended(b *testing.B) {
	ctx := context.Background()
	rate := int64(10)
	interval := 1 * time.Minute
	l, key := setupBench(b, ctx, rate, interval)

	// Pre-fill to hit the rate limit
	for range 10 {
		l.Allow(ctx, key)
	}
	time.Sleep(50 * time.Millisecond) // let incrementer run

	for b.Loop() {
		l.Allow(ctx, key)
	}
}

func BenchmarkWait(b *testing.B) {
	ctx := context.Background()
	rate := int64(1)
	interval := 1 * time.Minute
	l, key := setupBench(b, ctx, rate, interval)
	l.Allow(ctx, key)
	time.Sleep(50 * time.Millisecond) // let incrementer run
	l.Allow(ctx, key)                 // trigger mitigation
	time.Sleep(50 * time.Millisecond)

	for b.Loop() {
		l.Wait(ctx, key)
	}
}

func BenchmarkAllow_RedisDown(b *testing.B) {
	ctx := context.Background()
	rate := int64(b.N + 1)
	interval := 1 * time.Minute
	l, key := setupBench(b, ctx, rate, interval)

	// Kill redis
	l.rdb = redis.NewClient(&redis.Options{
		DialTimeout: 100 * time.Millisecond,
		Addr:        "127.0.0.1:0", // Invalid port, should fail fast
	})

	for b.Loop() {
		l.Allow(ctx, key)
	}
}

// setupBench is a simplified version of setup for benchmarks
func setupBench(b *testing.B, ctx context.Context, rate int64, interval time.Duration) (*Limiter, string) {
	b.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	keyConfFn := func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{Rate: rate, Interval: interval}
	}
	// Benchmarks shouldn't log to avoid skewing results.
	l := New(ctx, rdb, keyConfFn)
	key := fmt.Sprintf("%s-%d", b.Name(), time.Now().UnixNano()%1000)
	b.Cleanup(func() {
		l.Close()
		_ = rdb.Close()
	})
	return l, key
}

func setup(t *testing.T, ctx context.Context, rate int64, interval time.Duration) (*Limiter, string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		DialTimeout: 100 * time.Millisecond,
		Addr:        "localhost:6379",
	})

	zaptest.Level = zapcore.InfoLevel
	config := zaptest.Config()
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	zaptest.Config = func() zapcore.EncoderConfig { return config }
	logger := zaptest.Logger(t)

	key := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()%1000)
	keyConfFn := func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{Rate: rate, Interval: interval}
	}
	l := New(ctx, rdb, keyConfFn, Options{Logger: logger})
	t.Cleanup(func() {
		l.Close()
		_ = rdb.Close()
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
	assert.Equal(t, int64(0), intervals[minInterval.Add(granularity*1)], "second interval should have zero requests")
}
