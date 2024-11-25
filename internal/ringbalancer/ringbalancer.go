package ringbalancer

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// ErrDone to any active subscriber(s) when the Entry or Balancer is closed
var ErrDone = errors.New("done")

type Entry struct {
	C chan struct{} // C is the channel that will receive ticks to allow an unblock
	b *Balancer     // b is the balancer this entry is subscribed to (for access to the mutex)
}

// Close closes the entry and removes it from the balancer
func (re *Entry) Close() {
	re.b.mu.Lock()
	defer re.b.mu.Unlock()
	if len(re.b.entries) > 0 {
		i := slices.Index(re.b.entries, re)
		re.b.entries = slices.Delete(re.b.entries, i, i+1)
	}
	// we don't need to close as the GC will get it, and we won't panic if we don't even try
	// close(re.C)
	if re.b.i >= len(re.b.entries) {
		re.b.i = 0
	}
}

// Wait blocks until this subscriber receives a tick, or the entry is closed
// returns ErrDone if the entry is closed
func (re *Entry) Wait() error {
	_, ok := <-re.C
	if !ok {
		return ErrDone
	}
	return nil
}

// Balancer is a ring balancer that distributes ticks to subscribers in a round-robin fashion
type Balancer struct {
	entries []*Entry // entries is the list of subscribers
	i       int      // i is the current index in the ring
	mu      sync.Mutex
}

// New creates a new Balancer
func New() *Balancer {
	return &Balancer{}
}

// Close closes all subscribers and removes them from the balancer
func (rb *Balancer) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for _, e := range rb.entries {
		close(e.C)
	}
	rb.entries = []*Entry{}
	rb.i = 0
}

// String returns a string representation of the balancer for debugging
func (rb *Balancer) String() string {
	output := []string{}
	output = append(output, "Balancer{")
	for i, e := range rb.entries {
		output = append(output, fmt.Sprintf("  Entry %d: %+v", i, e))
	}
	output = append(output, fmt.Sprintf("  i: %d", rb.i))
	output = append(output, "}\n")
	return strings.Join(output, "\n")
}

// Subscribers returns the number of subscribers currently registered
func (rb *Balancer) Subscribers() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return len(rb.entries)
}

// Subscribe creates a new subscriber entry
func (rb *Balancer) Subscribe() *Entry {
	entry := &Entry{
		C: make(chan struct{}),
		b: rb,
	}
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.entries = append(rb.entries, entry)
	return entry
}

// Tick sends a tick to the next subscriber in the ring
// it will try each subscriber once before returning an error
func (rb *Balancer) Tick() error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	retries := 0
RETRY:
	retry := false

	if len(rb.entries) == 0 {
		return errors.New("no subscribers")
	}

	select {
	case rb.entries[rb.i].C <- struct{}{}:
	default:
		retry = true
	}

	rb.i++
	if rb.i >= len(rb.entries) {
		rb.i = 0
	}

	if retry {
		retries++
		if len(rb.entries) <= retries {
			return fmt.Errorf("send failed, no listenersd")
		}
		goto RETRY
	}
	return nil
}
