package slidingwindow

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitaminmoo/go-slidingwindow/internal/mitigation"
	"go.uber.org/zap"
)

func TestRateLimiterBasic(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	zl, err := zap.NewDevelopment()
	require.NoError(t, err)
	zl = zap.NewNop()

	cleanup := Init(ctx, *zl)
	defer cleanup()
	defer rdb.Close()

	key := fmt.Sprintf("%d", time.Now().UnixNano())
	rate := int64(10)
	interval := 100 * time.Millisecond
	limiter := NewRateLimiter(rdb, key, rate, interval)

	// avoid testing over the first interval wrap, which can cause more requests to
	// be allowed
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))

	allowed := 0
	for i := 0; i < 15; i++ {
		if limiter.Allow(ctx) {
			time.Sleep(2 * time.Millisecond) // due to async incrementer
			allowed++
		}
	}
	assert.Equal(t, 10, allowed)

	now := time.Now()
	limiter.Wait(ctx)
	assert.WithinDuration(t, time.Now(), now, interval)

	for i := 0; i < 10; i++ {
		now := time.Now()
		limiter.Wait(ctx)
		assert.WithinDuration(t, time.Now(), now, 11*time.Millisecond)
	}
}

func analyzeIntervals(t *testing.T, _ time.Duration, granularity time.Duration, rate int64, intervals map[time.Time]int64) {
	t.Helper()
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
		t.Logf("requests in interval %v: %d", i.Format("05.000"), intervals[i])
	}
	assert.GreaterOrEqual(t, intervals[minInterval.Add(granularity*0)], rate, "first interval should have at least rate requests")
	assert.Equal(t, intervals[minInterval.Add(granularity*1)], int64(0), "second interval should have zero requests")
}

func TestRateLimiterConcurrent(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	zl, err := zap.NewDevelopment()
	require.NoError(t, err)
	zl = zap.NewNop()

	cleanup := Init(ctx, *zl)
	defer cleanup()
	defer rdb.Close()

	key := fmt.Sprintf("%d", time.Now().UnixNano())
	interval := 500 * time.Millisecond
	granularity := 250 * time.Millisecond
	rate := int64(100)
	limiter := NewRateLimiter(rdb, key, rate, interval)

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

	// avoid testing over the first interval wrap, which can cause more requests to
	// be allowed
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))

	for i := 0; i < 10; i++ {
		wg.Add(1)
		// labels := pprof.Labels("name", "waiter", "number", fmt.Sprintf("%d", i))
		labels := pprof.Labels("name", "testWaiter")
		go pprof.Do(ctx, labels, func(context.Context) {
			defer wg.Done()
			for j := 0; int64(j) < rate; j++ {
				now := time.Now()
				limiter.Wait(ctx)
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

	analyzeIntervals(t, interval, granularity, rate, intervals)

	total := len(durations)
	assert.NotZero(t, total, "didn't record any requests")

	t.Logf("total sent: %d", len(durations))
}
