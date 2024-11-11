package slidingwindow

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRateLimiterBasic(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	zl, err := zap.NewDevelopment()
	require.NoError(t, err)
	// zl = zap.NewNop()

	cleanup := Init(rdb, *zl)
	defer cleanup()
	defer rdb.Close()

	key := fmt.Sprintf("%d", os.Getpid())
	rate := int64(10)
	interval := 100 * time.Millisecond
	limiter := NewRateLimiter(key, rate, interval)

	// avoid testing over the first interval wrap, which can cause more requests to
	// be allowed
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))

	var allows int
	for i := 0; i < 12; i++ {
		if limiter.Allow(ctx) {
			allows++
		}
	}
	assert.Equal(t, 10, allows, "should allow 10 requests")
	// Should be rate limited now
	assert.False(t, limiter.Allow(ctx))

	time.Sleep(interval)
	allows = 0
	for i := 0; i < 12; i++ {
		if limiter.Allow(ctx) {
			allows++
		}
	}
	assert.Equal(t, 10, allows, "should allow 10 requests")
}

func TestRateLimiterConcurrent(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	zl, err := zap.NewDevelopment()
	require.NoError(t, err)
	// zl = zap.NewNop()

	cleanup := Init(rdb, *zl)
	defer cleanup()
	defer rdb.Close()

	key := fmt.Sprintf("%d", os.Getpid())
	interval := 1000 * time.Millisecond
	granularity := 100 * time.Millisecond
	rate := int64(100)
	limiter := NewRateLimiter(key, rate, interval)

	var wg sync.WaitGroup

	var durations []time.Duration
	timesChan := make(chan time.Time, 1000)
	durationsChan := make(chan time.Duration, 1000)
	intervals := make(map[time.Time]int)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case ts := <-timesChan:
				intervals[ts.Truncate(granularity)]++
			case d := <-durationsChan:
				durations = append(durations, d)
			case <-done:
				return
			}
		}
	}()

	// avoid testing over the first interval wrap, which can cause more requests to
	// be allowed
	time.Sleep(time.Until(time.Now().Truncate(interval).Add(interval)))

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; int64(j) < rate; j++ {
				now := time.Now()
				limiter.Wait(ctx)
				timesChan <- time.Now()
				durationsChan <- time.Since(now)
			}
		}()
	}

	wg.Wait()
	done <- struct{}{}

	// Check that the durations are within the expected range
	buckets := make([]time.Duration, 0, 50)
	start := 100 * time.Microsecond
	end := time.Second
	factor := 1.5

	for d := start; d <= end; d = time.Duration(float64(d) * factor) {
		buckets = append(buckets, d)
	}
	stats := []int{}
	for _, d := range durations {
		for i, b := range buckets {
			if d <= b {
				if len(stats) <= i {
					stats = append(stats, 0)
				} else {
					stats[i]++
				}
			}
		}
	}

	for i, c := range stats {
		t.Logf("blocked <=% 15v: % 4d\n", buckets[i].Round(100*time.Microsecond), c)
	}

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

	t.Logf("allows: %d", allows.Load())
	t.Logf("redisIncrs: %d", redisIncrs.Load())
	t.Logf("redisExpireEXs: %d", redisExpireNXs.Load())
	t.Logf("denies: %d", denies.Load())
	t.Logf("redisGets: %d", redisGets.Load())
	t.Logf("mitigatedCacheMisses: %d", mitigatedCacheMisses.Load())
	t.Logf("mitigatedCacheHits: %d", mitigatedCacheHits.Load())
	t.Logf("mitigatedCacheWrites: %d", mitigatedCacheWrites.Load())
	t.Logf("redisSets: %d", redisSets.Load())
	t.Logf("redisMGets: %d", redisMGets.Load())
	t.Logf("redisErrors: %d", redisErrors.Load())
}
