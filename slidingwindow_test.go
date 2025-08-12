package slidingwindow

import (
	"context"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestBasic(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rate := int64(10)
	interval := 100 * time.Millisecond
	_, l, key := setup(t, ctx, rate, interval, 49*time.Millisecond)

	allowed := 0
	denied := 0
	for range rate {
		if l.Allow(ctx, key) {
			allowed++
		} else {
			denied++
		}
	}
	l.logger.Info("counts", zap.Int("allowed", allowed), zap.Int("denied", denied))
	assert.Equal(t, int(rate), allowed)
	assert.Equal(t, 0, denied)

	time.Sleep(l.batchDuration) // let incrementer run

	allowed = 0
	denied = 0
	for range rate {
		if l.Allow(ctx, key) {
			allowed++
		} else {
			denied++
		}
	}
	l.logger.Info("counts", zap.Int("allowed", allowed), zap.Int("denied", denied))
	assert.Equal(t, 0, allowed)
	assert.Equal(t, int(rate), denied)

	now := time.Now()
	l.Wait(ctx, key)
	// The first wait after mitigation might take longer, but subsequent ones should be paced.
	assert.WithinDuration(t, time.Now(), now, interval, "first wait should be within an interval")

	for i := range 10 {
		now := time.Now()
		l.Wait(ctx, key)
		// This is super time sensitive, I'm hoping go 1.25's time testing stuff will help here.
		assert.WithinDuration(t, time.Now(), now, 30*time.Millisecond, "try #%d wasn't within required duration", i)
	}
}

func TestConcurrent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rate := int64(80)
	interval := 1000 * time.Millisecond
	_, l, key := setup(t, ctx, rate, interval, 250*time.Millisecond)
	granularity := 4 // how many chunks to divide each interval into for analysis
	granularityDuration := time.Duration(interval.Nanoseconds() / int64(granularity))

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
				intervals[ts.Truncate(granularityDuration)]++
			case d := <-durationsChan:
				durations = append(durations, d)
			case <-done:
				// Drain any remaining items from channels
				for {
					select {
					case ts := <-timesChan:
						intervals[ts.Truncate(granularityDuration)]++
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
			}
		})
	}

	wg.Wait()
	done <- struct{}{}
	collectorWg.Wait()

	// require.False(t, mitigation.Allow(ctx, key), "should still be mitigated")
	time.Sleep(interval * 3)
	require.True(t, l.mitigationCache.Allow(ctx, key), "should not be mitigated")

	analyzeIntervals(t, l.logger, interval, granularity, granularityDuration, rate, intervals)

	total := len(durations)
	assert.NotZero(t, total, "didn't record any requests")

	l.logger.Info("total sent", zap.Int("count", len(durations)))
}

func TestRefreshKey(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rate := int64(5)
	interval := 1000 * time.Millisecond
	_, l, key := setup(t, ctx, rate, interval, 1*time.Microsecond)

	for range 5 {
		require.True(t, l.Allow(ctx, key), "should allow initial requests")
	}
	time.Sleep(5 * time.Millisecond)

	require.False(t, l.Allow(ctx, key), "should not allow after rate limit is hit")

	// Refresh key to a higher rate limit
	l.keyConfFn = func(ctx context.Context, key string) *KeyConf {
		return &KeyConf{Rate: 15, Interval: interval}
	}
	l.RefreshKey(ctx, key)

	time.Sleep(interval)

	for i := range 10 {
		require.True(t, l.Allow(ctx, key), "should allow requests after refreshing key conf, attempt %d", i+1)
	}
	time.Sleep(l.batchDuration) // let incrementer run
	allowed, err := l.checkRedis(ctx, key)
	require.NoError(t, err)
	require.True(t, allowed, "checkRedis should allow after refreshing key conf")
}

func TestRefresh(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rate := int64(5)
	interval := 100 * time.Millisecond
	_, l, key1 := setup(t, ctx, rate, interval, 49*time.Millisecond)
	key2 := key1 + "-key2"

	for range 5 {
		require.True(t, l.Allow(ctx, key1), "should allow initial requests for key 1")
		require.True(t, l.Allow(ctx, key2), "should allow initial requests for key 2")
	}

	time.Sleep(l.batchDuration) // let incrementer run

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
	t.Parallel()
	ctx := t.Context()
	rate := int64(1)
	interval := 100 * time.Millisecond
	_, l, key := setup(t, ctx, rate, interval, 49*time.Millisecond)

	require.Eventually(t, func() bool { return !l.Allow(ctx, key) }, interval, 1*time.Microsecond)
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
	t.Parallel()
	ctx := t.Context()
	_, l, _ := setup(t, ctx, 10, time.Second, 49*time.Millisecond)
	// close manually
	l.Close()
	// Close is called automatically via t.Cleanup inside setup() and should not error or block even with us already having closed
}

func TestAllow_AsyncIncrementRace(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rate := int64(10)
	interval := 500 * time.Millisecond
	_, l, key := setup(t, ctx, rate, interval, 49*time.Millisecond)

	// Fire a burst of requests. More than the rate limit may be allowed initially
	// because of the async incrementer.
	allowedCount := 0
	for range 15 {
		if l.Allow(ctx, key) {
			allowedCount++
		}
	}

	l.logger.Info("initial allowed requests", zap.Int("count", allowedCount))
	assert.GreaterOrEqual(t, allowedCount, int(rate), "at least 'rate' should be allowed")

	// Wait for incrementer to catch up and mitigation to trigger.
	time.Sleep(l.batchDuration)

	// Now, subsequent requests should be denied.
	assert.False(t, l.Allow(ctx, key), "should not be allowed after incrementer catches up")
	assert.False(t, l.mitigationCache.Allow(ctx, key), "should be mitigated")
}

func BenchmarkAllow(b *testing.B) {
	ctx := b.Context()
	rate := int64(b.N + 1) // Ensure we don't hit the rate limit
	interval := 1 * time.Minute
	l, key := setupBench(b, ctx, rate, interval)

	for b.Loop() {
		l.Allow(ctx, key)
	}
}

func BenchmarkAllow_Contended(b *testing.B) {
	ctx := b.Context()
	rate := int64(10)
	interval := 1 * time.Minute
	l, key := setupBench(b, ctx, rate, interval)

	// Pre-fill to hit the rate limit
	for range 10 {
		l.Allow(ctx, key)
	}
	time.Sleep(l.batchDuration) // let incrementer run

	for b.Loop() {
		l.Allow(ctx, key)
	}
}

func BenchmarkWait(b *testing.B) {
	ctx := b.Context()
	rate := int64(1)
	interval := 1 * time.Minute
	l, key := setupBench(b, ctx, rate, interval)
	l.Allow(ctx, key)
	time.Sleep(l.batchDuration) // let incrementer run
	l.Allow(ctx, key)           // trigger mitigation
	time.Sleep(l.batchDuration)

	for b.Loop() {
		l.Wait(ctx, key)
	}
}

func BenchmarkAllow_RedisDown(b *testing.B) {
	ctx := b.Context()
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
