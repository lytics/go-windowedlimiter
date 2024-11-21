package mitigation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMitigate_AllowsRequests(t *testing.T) {
	ctx := context.Background()
	key := "test-allow"
	period := 10 * time.Millisecond

	// Always allow requests
	Trigger(ctx, key, period, func(context.Context) bool {
		return true
	})

	// Should return immediately since the allowFn returns true
	err := Wait(ctx, key)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMitigate_BlocksRequests(t *testing.T) {
	ctx := context.Background()
	key := "test-block"
	period := 10 * time.Millisecond

	// Never allow requests
	Trigger(ctx, key, period, func(context.Context) bool {
		return false
	})

	// Create a context with timeout to avoid hanging
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err := Wait(ctxWithTimeout, key)
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

func TestMitigate_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	key := "test-cancel"
	period := 10 * time.Millisecond

	Trigger(ctx, key, period, func(context.Context) bool {
		return false
	})

	// Cancel the context immediately
	cancel()

	err := Wait(ctx, key)
	if err == nil {
		t.Error("Expected cancelled error, got nil")
	}
}

func TestWait_NonExistentKey(t *testing.T) {
	ctx := context.Background()
	key := "non-existent"

	err := Wait(ctx, key)
	if err != nil {
		t.Errorf("Expected no error for non-existent key, got %v", err)
	}
}

func TestMitigate_Expiration(t *testing.T) {
	ctx := context.Background()
	key := "test-expiration"
	period := 10 * time.Millisecond

	Trigger(ctx, key, period, func(context.Context) bool {
		return true
	})

	check := func() bool {
		_, exists := mitigationCache.Load(key)
		return !exists
	}
	require.Eventually(t, check, 5*period, period)
}

func TestMitigate_MultipleWaiters(t *testing.T) {
	ctx := context.Background()
	key := "test-multiple"
	period := 100 * time.Millisecond
	num := 10

	var try int
	Trigger(ctx, key, period, func(context.Context) bool {
		try++
		return try >= 5
	})

	// Create multiple goroutines waiting on the same mitigation
	errs := make(chan error, num)
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * time.Millisecond)
			errs <- Wait(ctx, key)
		}(i)
	}
	wg.Wait()

	// Collect results
	for i := 0; i < num; i++ {
		err := <-errs
		if err != nil {
			t.Errorf("Expected no error for waiter %d, got %v", i, err)
		}
	}
}

func TestMitigate_PeriodReset(t *testing.T) {
	ctx := context.Background()
	key := "test-reset"
	period := 10 * time.Millisecond

	allowCalls := atomic.Int32{}
	Trigger(ctx, key, period, func(context.Context) bool {
		allowCalls.Add(1)
		return allowCalls.Load() > 2 // Allow after 2 failures
	})

	// Wait long enough for multiple periods
	time.Sleep(ttlMultiplier * period)

	err := Wait(ctx, key)
	if err != nil {
		t.Errorf("Expected successful wait after period reset, got %v", err)
	}

	if allowCalls.Load() <= 2 {
		t.Errorf("Expected multiple allow function calls, got %d", allowCalls.Load())
	}
}
