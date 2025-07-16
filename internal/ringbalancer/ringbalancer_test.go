package ringbalancer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	rb := New[chan struct{}]()
	require.NotNil(t, rb, "New() returned nil")
}

func TestRegister(t *testing.T) {
	rb := New[int]()
	entry1 := rb.Register(1, nil)
	require.Equal(t, 1, entry1.Value)
	require.NotNil(t, entry1, "Register() returned nil entry")
	require.Equal(t, 1, rb.cur.Value, "Expected cur to be the only entry")
	require.Equal(t, rb.cur.next.Value, rb.cur.prev.Value, "Single entry should point to itself")

	next := rb.Next()
	require.Equal(t, 1, next.Value, "Next() should return the only entry")
	next = rb.Next()
	require.Equal(t, 1, next.Value, "Next() should return the only entry")

	entry2 := rb.Register(2, nil)
	require.NotNil(t, entry2, "Register() returned nil entry")
	require.Equal(t, 2, entry2.Value)

	checkOrdering(t, rb, []int{1, 2})

	entry3 := rb.Register(3, nil)
	require.NotNil(t, entry3, "Register() returned nil entry")
	require.Equal(t, 3, entry3.Value)

	checkOrdering(t, rb, []int{1, 2, 3})

	entry4 := rb.Register(4, nil)
	require.NotNil(t, entry4, "Register() returned nil entry")
	require.Equal(t, 4, entry4.Value)

	checkOrdering(t, rb, []int{1, 2, 3, 4})

	require.NoError(t, entry1.Remove())
	require.Equal(t, 2, rb.cur.Value, "Expected cur to be entry2")
	checkOrdering(t, rb, []int{2, 3, 4})

	require.NoError(t, entry4.Remove())
	require.Equal(t, 2, rb.cur.Value, "Expected cur to be entry2")
	checkOrdering(t, rb, []int{2, 3})
}

func checkOrdering(t *testing.T, rb *Ring[int], expected []int) {
	t.Helper()
	n := rb.cur
	for _, val := range expected {
		require.Equal(t, val, n.Value)
		n = n.next
	}
	require.Equal(t, expected[0], n.Value)

	for i := range expected {
		next := rb.Next()
		require.Equal(t, expected[i], next.Value, "Next() should return the correct entry")
	}
}

func BenchmarkRegister(b *testing.B) {
	rb := New[int]()
	for i := 0; b.Loop(); i++ {
		rb.Register(i, nil)
	}
}

func BenchmarkRemove(b *testing.B) {
	rb := New[int]()
	elements := make([]*Element[int], b.N)
	for i := 0; b.Loop(); i++ {
		elements[i] = rb.Register(i, nil)
	}

	for i := 0; b.Loop(); i++ {
		require.NoError(b, elements[i].Remove())
	}
}

func BenchmarkNextEmpty(b *testing.B) {
	rb := New[int]()
	for b.Loop() {
		rb.Next()
	}
}

func BenchmarkNext(b *testing.B) {
	rb := New[int]()
	for i := range 100 { // Pre-populate with a reasonable number of elements
		rb.Register(i, nil)
	}

	for b.Loop() {
		rb.Next()
	}
}

func BenchmarkRemoveWithCleanup(b *testing.B) {
	rb := New[int]()
	cleanup := func(int) error { return nil }
	elements := make([]*Element[int], b.N)
	for i := 0; b.Loop(); i++ {
		elements[i] = rb.Register(i, cleanup)
	}

	for i := 0; b.Loop(); i++ {
		require.NoError(b, elements[i].Remove())
	}
}
