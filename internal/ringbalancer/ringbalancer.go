package ringbalancer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ErrDone to any active subscriber(s) when the Entry or Balancer is closed
var ErrDone = errors.New("done")

type Element[T any] struct {
	Value   T                     // Value is the user-supplied value being balanced
	cleanup func(element T) error // cleanup is called when the element is removed from the ring
	b       *Ring[T]              // b is the balancer this entry is subscribed to (for access to the mutex)
	prev    *Element[T]
	next    *Element[T]
}

// Remove closes the entry and removes it from the balancer
//
// Runs Element.Cleanup after removing the entry from the ring
func (re *Element[any]) Remove() error {
	re.b.mu.Lock()
	defer re.b.mu.Unlock()
	if re.next == re {
		// last entry in the ring
		re.next = nil
		re.prev = nil
		re.b.cur = nil
	} else {
		re.next.prev = re.prev
		re.prev.next = re.next
		if re.b.cur == re {
			re.b.cur = re.next
		}
	}
	if re.cleanup != nil {
		return re.cleanup(re.Value)
	}
	return nil
}

// Ring is a doubly linked ring, optimized equally for reads and registers
type Ring[T any] struct {
	cur *Element[T]
	mu  sync.Mutex
}

// New creates a new Balancer
func New[T any]() *Ring[T] {
	return &Ring[T]{}
}

// Close clears the ring of all Elements, running each Cleanup function while doing so
func (rb *Ring[T]) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.cur == nil {
		return
	}
	if rb.cur.prev == nil {
		// only one element
		_ = rb.cur.cleanup(rb.cur.Value)
		rb.cur = nil
		return
	}
	rb.cur.prev.next = nil
	for cur := rb.cur; cur != nil; cur = cur.next {
		_ = cur.cleanup(cur.Value)
	}
	rb.cur = nil
}

// Empty returns true if the ring is empty
func (rb *Ring[T]) Empty() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.cur == nil
}

// String returns a string representation of the balancer for debugging
func (rb *Ring[T]) String() string {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.cur == nil {
		return "Empty Ring"
	}

	var sb strings.Builder
	sb.WriteString("Current      | Previous     | Thing        | Next\n")
	sb.WriteString("------------ | ------------ | ------------ | ------------\n")

	cur := rb.cur
	for {
		prev := cur.prev
		next := cur.next

		sb.WriteString(fmt.Sprintf("%p | %p | %v | %p\n", cur, prev, cur.Value, next))

		cur = next
		if cur == rb.cur {
			break
		}
	}

	return sb.String()
}

// Register creates a new element in the ring in the slot before Ring.cur
func (rb *Ring[T]) Register(thing T, cleanup func(T) error) *Element[T] {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	element := &Element[T]{
		Value:   thing,
		cleanup: cleanup,
		b:       rb,
	}
	if rb.cur == nil {
		// empty ring, set this element as the only element
		element.next = element
		element.prev = element
		rb.cur = element
		return element
	}
	element.next = rb.cur
	element.prev = rb.cur.prev
	rb.cur.prev.next = element
	rb.cur.prev = element
	return element
}

// Next returns the current element and advances the ring to the next element.
func (rb *Ring[T]) Next() *Element[T] {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	cur := rb.cur
	if cur != nil {
		rb.cur = rb.cur.next
	}
	return cur
}
