// Copyright 2022 PingCAP, Inc.
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

package queue

import (
	"math/rand"
	"testing"

	"github.com/edwingeng/deque"
	"github.com/stretchr/testify/require"
)

const (
	iterTestSize = 10007
)

func TestChunkQueueIteratorPrevNext(t *testing.T) {
	t.Parallel()
	q := NewChunkQueue[int]()
	for i := 0; i < iterTestSize; i++ {
		q.Push(i)
	}

	var it *ChunkQueueIterator[int]
	i := 0
	for it = q.First(); it.Valid(); it.Next() {
		v := it.Value()
		require.Equal(t, i, it.Index())
		require.Equal(t, i, v)
		i++
	}
	i--
	for it = q.End(); it.Prev(); {
		v := it.Value()
		require.Equal(t, i, it.Index())
		require.Equal(t, i, v)
		i--
	}

	it = &ChunkQueueIterator[int]{0, nil}
	require.False(t, it.Prev())
	require.False(t, it.Next())
}

func BenchmarkIterate(b *testing.B) {
	b.Run("Iterate-ChunkQueue-by-iterator", func(b *testing.B) {
		q := NewChunkQueue[int]()
		n := b.N
		for i := 0; i < n; i++ {
			q.Push(i)
		}
		b.ResetTimer()

		i := 0
		for it := q.First(); it.Valid(); it.Next() {
			v := it.Value()
			if v != i {
				panic("not equal")
			}
			i++
		}
	})

	b.Run("Iterate-ChunkQueue-by-Peek", func(b *testing.B) {
		q := NewChunkQueue[int]()
		n := b.N
		for i := 0; i < n; i++ {
			q.Push(i)
		}
		b.ResetTimer()

		i := 0
		for i = 0; i < q.Len(); i++ {
			if q.Peek(i) != i {
				panic(q.Peek(i))
			}
		}
	})

	b.Run("Iterate-ChunkQueue-by-Range", func(b *testing.B) {
		q := NewChunkQueue[int]()
		n := b.N
		for i := 0; i < n; i++ {
			q.Push(i)
		}
		b.ResetTimer()

		q.RangeWithIndex(func(idx int, val int) bool {
			if val != idx {
				panic("not equal")
			}
			return true
		})
	})

	b.Run("Iterate-Slice-byLoop", func(b *testing.B) {
		n := b.N
		q := make([]int, n)
		for i := 0; i < n; i++ {
			q[i] = i
		}
		b.ResetTimer()

		for i := 0; i < len(q); i++ {
			if q[i] != i {
				panic("error")
			}
		}
	})

	b.Run("Iterate-3rdPartyDeque-byRange", func(b *testing.B) {
		q := deque.NewDeque()
		n := b.N

		for i := 0; i < n; i++ {
			q.Enqueue(i)
		}
		b.ResetTimer()

		q.Range(func(idx int, val deque.Elem) bool {
			if val.(int) != idx {
				panic("not equal")
			}
			return true
		})
	})

	b.Run("Iterate-3rdPartyDeque-byPeek", func(b *testing.B) {
		q := deque.NewDeque()
		n := b.N

		for i := 0; i < n; i++ {
			q.Enqueue(i)
		}
		b.ResetTimer()

		for i := 0; i < n; i++ {
			val := q.Peek(i)
			if val != i {
				panic("not equal")
			}
		}
	})
}

func TestChunkQueueGetIterator(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	q.PushMany(0, 1)
	var it *ChunkQueueIterator[int]
	it = q.GetIterator(-1)
	require.Nil(t, it)
	it = q.GetIterator(2)
	require.Nil(t, it)
	oldIt := q.GetIterator(1)
	q.PopMany(2)
	require.False(t, oldIt.Valid(), oldIt.Prev())
	require.True(t, q.Empty())

	for i := 0; i < iterTestSize; i++ {
		q.Push(i)
	}
	require.True(t, q.End().Index() < 0)
	require.False(t, q.Begin().Prev())
	require.Panics(t, func() {
		q.End().Set(1)
	})

	require.NotPanics(t, func() {
		for i := 0; i < iterTestSize; i++ {
			it = q.GetIterator(i)
			require.Equal(t, i, it.Index(), it.Value(), q.Peek(i))
		}
	})

	cnt := 0
	for !q.Empty() {
		n := rand.Intn(q.Len())
		if n == 0 {
			n = testCaseSize/20 + 1
		}
		it := q.Begin()
		require.True(t, it.Valid())

		q.PopMany(n)
		require.Equal(t, -1, it.Index())
		require.False(t, it.Valid())

		require.Nil(t, q.Begin().chunk.prev)
		cnt += n
		v := q.Begin().Value()
		if cnt >= iterTestSize {
			require.True(t, !it.Valid())
		} else {
			require.Equal(t, cnt, v)
		}
	}
}
