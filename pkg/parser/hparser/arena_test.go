// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package hparser

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestArenaAlloc(t *testing.T) {
	a := NewArena()

	// Allocate a simple struct.
	type point struct {
		X, Y int64
	}
	p := Alloc[point](a)
	require.NotNil(t, p)
	require.Equal(t, int64(0), p.X)
	require.Equal(t, int64(0), p.Y)
	p.X = 42
	p.Y = 99
	require.Equal(t, int64(42), p.X)
	require.Equal(t, int64(99), p.Y)

	// Allocate another — should be at a different address.
	p2 := Alloc[point](a)
	require.NotNil(t, p2)
	require.NotEqual(t, unsafe.Pointer(p), unsafe.Pointer(p2))
	require.Equal(t, int64(0), p2.X)

	// Original allocation should still be valid.
	require.Equal(t, int64(42), p.X)
}

func TestArenaAllocSlice(t *testing.T) {
	a := NewArena()

	s := AllocSlice[int64](a, 10)
	require.Len(t, s, 10)
	for i := range s {
		require.Equal(t, int64(0), s[i])
		s[i] = int64(i * i)
	}
	for i := range s {
		require.Equal(t, int64(i*i), s[i])
	}

	// Zero-length slice should return nil.
	s0 := AllocSlice[int64](a, 0)
	require.Nil(t, s0)
}

func TestArenaReset(t *testing.T) {
	a := NewArena()

	type big struct {
		data [1024]byte
	}

	// Fill the arena.
	for range 100 {
		Alloc[big](a)
	}

	// Reset and reuse.
	a.Reset()

	p := Alloc[big](a)
	require.NotNil(t, p)
	// After reset, the first allocation should start from the beginning.
	// The zero-initialization guarantees data is zeroed even on reuse.
	for _, b := range p.data {
		require.Equal(t, byte(0), b)
	}
}

func TestArenaBlockGrowth(t *testing.T) {
	a := NewArena()

	// Alloc currently uses heap (new) instead of arena bump-pointer
	// for GC safety (see Alloc comment). Verify that all pointers are
	// valid and distinct, which matters regardless of the backing allocator.
	type item struct {
		data [4096]byte
	}
	items := make([]*item, 0, 20)
	for range 20 {
		items = append(items, Alloc[item](a))
	}

	// All pointers should be valid and distinct.
	seen := make(map[unsafe.Pointer]bool)
	for _, it := range items {
		ptr := unsafe.Pointer(it)
		require.False(t, seen[ptr], "duplicate pointer from arena")
		seen[ptr] = true
	}
}

func TestArenaOversizedAlloc(t *testing.T) {
	a := NewArena()

	// Allocate something larger than defaultBlockSize.
	type huge struct {
		data [defaultBlockSize * 2]byte
	}
	h := Alloc[huge](a)
	require.NotNil(t, h)
	h.data[0] = 0xFF
	h.data[len(h.data)-1] = 0xFE
	require.Equal(t, byte(0xFF), h.data[0])
	require.Equal(t, byte(0xFE), h.data[len(h.data)-1])

	// Should still be able to allocate smaller items after.
	p := Alloc[int64](a)
	require.NotNil(t, p)
	*p = 12345
	require.Equal(t, int64(12345), *p)
}

func BenchmarkArenaAlloc(b *testing.B) {
	a := NewArena()
	type node struct {
		Left  *node
		Right *node
		Value int64
		Flags uint64
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%1000 == 0 {
			a.Reset()
		}
		n := Alloc[node](a)
		n.Value = int64(i)
	}
}

func BenchmarkHeapAlloc(b *testing.B) {
	type node struct {
		Left  *node
		Right *node
		Value int64
		Flags uint64
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := new(node)
		n.Value = int64(i)
	}
}
