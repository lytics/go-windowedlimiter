package mitigation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mitKey struct {
	key string
}

func (mk mitKey) String() string {
	return mk.key
}

func TestMitigate_AllowsRequests(t *testing.T) {
	ctx := context.Background()
	key := mitKey{key: "test-allow"}
	period := 10 * time.Millisecond
	mc := New(
		func(context.Context, mitKey) bool { return true },
	)

	// Always allow requests
	mc.Trigger(ctx, key, period)

	// Should return immediately since the allowFn returns true
	err := mc.Wait(ctx, key)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMitigate_BlocksRequests(t *testing.T) {
	ctx := context.Background()
	key := mitKey{key: "test-block"}
	period := 10 * time.Millisecond
	mc := New(
		func(context.Context, mitKey) bool { return false },
	)

	// Never allow requests
	mc.Trigger(ctx, key, period)

	// Create a context with timeout to avoid hanging
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err := mc.Wait(ctxWithTimeout, key)
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

func TestMitigate_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	key := mitKey{key: "test-cancel"}
	period := 10 * time.Millisecond
	mc := New(
		func(context.Context, mitKey) bool { return false },
	)

	mc.Trigger(ctx, key, period)

	// Cancel the context immediately
	cancel()

	err := mc.Wait(ctx, key)
	if err == nil {
		t.Error("Expected cancelled error, got nil")
	}
}

func TestWait_NonExistentKey(t *testing.T) {
	ctx := context.Background()
	key := mitKey{key: "non-existent"}
	mc := New(
		func(context.Context, mitKey) bool { return false },
	)

	err := mc.Wait(ctx, key)
	if err != nil {
		t.Errorf("Expected no error for non-existent key, got %v", err)
	}
}

func TestMitigate_Expiration(t *testing.T) {
	ctx := context.Background()
	key := mitKey{key: "test-expiration"}
	period := 10 * time.Millisecond
	mc := New(
		func(context.Context, mitKey) bool { return true },
	)

	mc.Trigger(ctx, key, period)

	check := func() bool {
		_, exists := mc.cache.Load(key)
		return !exists
	}
	require.Eventually(t, check, 10*period, period)
}

func TestMitigate_MultipleWaiters(t *testing.T) {
	ctx := context.Background()
	key := mitKey{key: "test-multiple"}
	period := 100 * time.Millisecond
	num := 10

	var try int
	mc := New(
		func(context.Context, mitKey) bool {
			try++
			return try >= 5
		},
	)

	mc.Trigger(ctx, key, period)

	// Create multiple goroutines waiting on the same mitigation
	errs := make(chan error, num)
	var wg sync.WaitGroup
	for i := range num {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * time.Millisecond)
			errs <- mc.Wait(ctx, key)
		}(i)
	}
	wg.Wait()

	// Collect results
	for i := range num {
		err := <-errs
		if err != nil {
			t.Errorf("Expected no error for waiter %d, got %v", i, err)
		}
	}
}

func TestMitigate_PeriodReset(t *testing.T) {
	ctx := context.Background()
	key := mitKey{key: "test-reset"}
	period := 10 * time.Millisecond

	allowCalls := atomic.Int32{}
	mc := New(
		func(context.Context, mitKey) bool {
			allowCalls.Add(1)
			return allowCalls.Load() > 2 // Allow after 2 failures
		},
	)
	mc.Trigger(ctx, key, period)

	// Wait long enough for multiple periods
	time.Sleep(time.Duration(ttlMultiplier) * period)

	err := mc.Wait(ctx, key)
	if err != nil {
		t.Errorf("Expected successful wait after period reset, got %v", err)
	}

	if allowCalls.Load() <= 2 {
		t.Errorf("Expected multiple allow function calls, got %d", allowCalls.Load())
	}
}
