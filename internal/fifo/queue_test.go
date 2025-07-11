package fifo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	rb := New[chan struct{}]()
	require.NotNil(t, rb, "New() returned nil")
}

func TestPush(t *testing.T) {
	rb := New[int]()
	entry1 := rb.Push(1, nil)
	require.Equal(t, 1, entry1.Value)
	require.NotNil(t, entry1, "Push() returned nil entry")
	require.Nil(t, rb.cur.next, "Single entry should point to nil")
	require.Equal(t, entry1, rb.last, "last should point to only entry")
	require.Equal(t, entry1, rb.Peek(), "Peek() should return the only entry")
	require.Equal(t, entry1, rb.Shift(), "Shift() should return the only entry")
	require.Nil(t, rb.Shift(), "Shift() should return nil when empty")

	entry2 := rb.Push(2, nil)
	entry3 := rb.Push(3, nil)
	entry4 := rb.Push(4, nil)

	require.Equal(t, entry2, rb.cur, "last should point to newest entry")
	require.Equal(t, entry4, rb.last, "last should point to newest entry")

	entry3.Remove()
	require.Equal(t, entry2, rb.Shift(), "Shift() should return entry2")
	require.Equal(t, entry4, rb.Shift(), "Shift() should return entry4")

	entry4.Remove()
	require.Nil(t, rb.Shift(), "Shift() should return nil when empty")
}

func BenchmarkPush(b *testing.B) {
	rb := New[int]()
	for i := 0; b.Loop(); i++ {
		rb.Push(i, nil)
	}
}

func BenchmarkRemove(b *testing.B) {
	rb := New[int]()
	elements := make([]*Element[int], b.N)
	for i := 0; b.Loop(); i++ {
		elements[i] = rb.Push(i, nil)
	}

	for i := 0; b.Loop(); i++ {
		elements[i].Remove()
	}
}

func BenchmarkShiftEmpty(b *testing.B) {
	rb := New[int]()
	for b.Loop() {
		rb.Shift()
	}
}

func BenchmarkShift(b *testing.B) {
	rb := New[int]()
	for i := 0; b.Loop(); i++ {
		rb.Push(i, nil)
	}

	for b.Loop() {
		rb.Shift()
	}
}

func BenchmarkRemoveWithCleanup(b *testing.B) {
	rb := New[int]()
	cleanup := func(int) error { return nil }
	elements := make([]*Element[int], b.N)
	for i := 0; b.Loop(); i++ {
		elements[i] = rb.Push(i, cleanup)
	}

	for i := 0; b.Loop(); i++ {
		elements[i].Remove()
	}
}
