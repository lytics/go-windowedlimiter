package ringbalancer

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	rb := New()
	require.NotNil(t, rb, "New() returned nil")
}

func TestSubscribe(t *testing.T) {
	rb := New()
	entry := rb.Subscribe()

	require.NotNil(t, entry, "Subscribe() returned nil entry")
	require.NotNil(t, entry.C, "Subscribe() returned entry with nil channel")
	assert.Equal(t, 1, len(rb.entries), "Expected 1 entry")
}

func TestTickWithNoSubscribers(t *testing.T) {
	rb := New()
	err := rb.Tick()

	require.Error(t, err, "Expected error when ticking with no subscribers")
}

func TestTickDistribution(t *testing.T) {
	rb := New()

	num := 3
	counts := make([]int, num)
	var wg sync.WaitGroup
	wg.Add(num)
	for i := 0; i < num; i++ {
		// Count received ticks
		go func() {
			defer wg.Done()
			for range rb.Subscribe().C {
				counts[i]++
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)

	// Send 6 ticks
	for i := 0; i < num*2; i++ {
		err := rb.Tick()
		require.NoError(t, err, "Unexpected error on tick %d: %v\n%v", i, err, rb.String())
		time.Sleep(10 * time.Millisecond) // Give some time for processing
	}

	rb.Close()
	wg.Wait()

	// Each subscriber should have received 2 ticks
	for i, count := range counts {
		assert.Equal(t, 2, count, "Subscriber %d received wrong number of ticks", i)
	}
}

func TestClosedEntryRemoval(t *testing.T) {
	rb := New()

	entry1 := rb.Subscribe()
	entry2 := rb.Subscribe()

	assert.Len(t, rb.entries, 2, "Expected 2 entries")
	entry1.Close()
	assert.Len(t, rb.entries, 1, "Expected 1 entries")

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, entry2.Wait())
	}() // Drain the channel

	time.Sleep(10 * time.Millisecond)

	// Tick should remove the closed entry
	err := rb.Tick()
	require.NoError(t, err, "Unexpected error on tick")

	wg.Wait()
}

func TestConcurrentSubscribeAndTick(t *testing.T) {
	rb := New()

	num := 2
	counts := make([]int, num)
	var wgSub sync.WaitGroup
	wgSub.Add(num)
	// Concurrent subscribes
	for i := 0; i < num; i++ {
		go func() {
			defer wgSub.Done()
			for range rb.Subscribe().C {
				counts[i]++
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)

	var wgTick sync.WaitGroup
	wgTick.Add(num)
	// Concurrent ticks
	for i := 0; i < num; i++ {
		go func() {
			defer wgTick.Done()
			for j := 0; j < 10; j++ {
				err := rb.Tick()
				require.NoError(t, err)
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wgTick.Wait()
	rb.Close()
	wgSub.Wait()
}

func TestEntryCloseIdempotency(t *testing.T) {
	rb := New()
	entry := rb.Subscribe()

	// Should be able to close multiple times without panic
	entry.Close()
	entry.Close()
	entry.Close()
}

func TestCloseWhileWriting(t *testing.T) {
	rb := New()
	entry := rb.Subscribe()

	var wg sync.WaitGroup
	wg.Add(1)

	// Start goroutine that will close the entry while Tick is trying to write
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond) // Give time for Tick to start
		entry.Close()
	}()

	// Try to tick repeatedly - this should not panic even if the entry is closed
	for i := 0; i < 100; i++ {
		rb.Tick()
		time.Sleep(1 * time.Millisecond)
	}

	wg.Wait()
}
