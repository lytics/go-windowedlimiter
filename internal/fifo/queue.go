// Package fifo provides a linked-list based fifo queue optimized for pushing and shifting
package fifo

import (
	"sync"
)

type Element[T any] struct {
	Value   T                     // Value is the user-supplied value being balanced
	cleanup func(element T) error // cleanup is called when the element is removed from the queue
	next    *Element[T]
	mu      sync.Mutex
	removed bool
}

// Remove closes the entry and removes it from the queue
//
// Runs Element.Cleanup after removing the element from the queue
func (qe *Element[any]) Remove() error {
	qe.mu.Lock()
	defer qe.mu.Unlock()
	qe.removed = true
	if qe.cleanup != nil {
		return qe.cleanup(qe.Value)
	}
	return nil
}

// Queue is a linked-list based queue optimized for pushing and shifting
type Queue[T any] struct {
	cur  *Element[T]
	last *Element[T]
	mu   sync.Mutex
}

// New creates a new Balancer
func New[T any]() *Queue[T] {
	return &Queue[T]{}
}

// Close clears the ring of all Elements, running each Cleanup function while doing so
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.cur == nil {
		return
	}
	for cur := q.cur; cur != nil; cur = cur.next {
		cur.removed = true
		cur.next = nil
		_ = cur.cleanup(cur.Value)
	}
	q.cur = nil
}

// Push creates a new element at the end of the queue
func (q *Queue[T]) Push(thing T, cleanup func(T) error) *Element[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	element := &Element[T]{
		Value:   thing,
		cleanup: cleanup,
		mu:      sync.Mutex{},
	}
	if q.cur == nil {
		// empty ring, set this element as the only element
		q.cur = element
		q.last = element
		return element
	}
	q.last.mu.Lock()
	q.last.next = element
	q.last.mu.Unlock()
	q.last = element
	return element
}

// shift removes and returns the oldest element from the queue, optionally removing it
func (rb *Queue[T]) shift(remove bool) *Element[T] {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for cur := rb.cur; cur != nil; cur = cur.next {
		cur.mu.Lock()
		if cur.removed {
			cur.mu.Unlock()
			continue
		}
		if remove {
			rb.cur = cur.next
		}
		cur.mu.Unlock()
		return cur
	}
	return nil
}

// Shift removes and returns the oldest element from the queue
func (rb *Queue[T]) Shift() *Element[T] {
	return rb.shift(true)
}

// Peek returns the oldest element from the queue without removing it.
// Note that thread safety of the returned element is up to the caller
func (rb *Queue[T]) Peek() *Element[T] {
	return rb.shift(false)
}
