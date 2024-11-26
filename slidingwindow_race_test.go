//go:build !race
// +build !race

package slidingwindow

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// the way this test overwrites the redis client causes a race
func TestFailureMode(t *testing.T) {
	ctx := context.Background()
	rate := int64(1)
	interval := 1 * time.Second
	l, key := setup(t, ctx, rate, interval)

	assert.True(t, l.Allow(ctx, key), "first request should be allowed")
	time.Sleep(50 * time.Millisecond)

	// kill redis in a way that fails instantly
	l.rdb = redis.NewClient(&redis.Options{
		DialTimeout: 100 * time.Millisecond,
		Addr:        "127.0.0.1:0",
	})
	assert.True(t, l.Allow(ctx, key), "should be allowed with redis down")
	l.failClosed = true
	assert.False(t, l.Allow(ctx, key), "should be denied with redis down")

	// kill redis in a way that fails with a timeout
	l.rdb = redis.NewClient(&redis.Options{
		DialTimeout: 100 * time.Millisecond,
		Addr:        "1.1.1.1:0",
	})
	assert.False(t, l.Allow(ctx, key), "should be denied with redis down")
	l.failClosed = false
	assert.True(t, l.Allow(ctx, key), "should be allowed with redis down")
}
