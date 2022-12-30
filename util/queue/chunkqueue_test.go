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
	"fmt"
	"math/rand"
	"testing"

	"github.com/edwingeng/deque"
	"github.com/labstack/gommon/random"
	"github.com/stretchr/testify/require"
)

const (
	testCaseSize = 10007
)

func TestChunkQueueCommon(t *testing.T) {
	t.Parallel()

	type ZeroSizeType struct{}
	require.NotNil(t, NewChunkQueue[ZeroSizeType]())

	q := NewChunkQueue[int]()

	// simple enqueue dequeue
	x := rand.Int()
	q.Push(x)
	require.Equal(t, q.Len(), 1)
	require.Equal(t, x, q.Peek(0))
	v, ok := q.Pop()
	require.Equal(t, v, x)
	require.True(t, ok)

	// PushMany & PopMany
	elements := make([]int, 0, testCaseSize)
	require.True(t, q.Empty())
	for i := 0; i < testCaseSize; i++ {
		elements = append(elements, i)
	}
	q.PushMany(elements...)
	require.Equal(t, testCaseSize, q.Len(), q.Len())
	vals, ok := q.PopMany(testCaseSize * 3 / 4)
	require.True(t, ok)
	for i, v := range vals {
		require.Equal(t, elements[i], v)
	}
	require.Equal(t, testCaseSize-testCaseSize*3/4, q.Len())
	// Clear
	q.Clear()
	require.True(t, q.Empty())

	// Element Access
	q.PushMany(elements...)
	require.Equal(t, testCaseSize, q.Len())
	require.False(t, q.Empty())
	for j := 0; j < testCaseSize; j++ {
		i := rand.Intn(testCaseSize)
		v := q.Peek(i)
		it := q.GetIterator(i)
		itv := it.Value()
		require.Equal(t, it.Index(), itv, v)

		it.Set(i + 1)
		require.Equal(t, it.Value(), v+1)
		q.Replace(i, i)
		require.Equal(t, it.Value(), v)
	}
	require.Panics(t, func() {
		_ = q.Peek(-1)
	})
	require.Panics(t, func() {
		q.Replace(testCaseSize, 0)
	})
	tail, ok := q.Tail()
	require.Equal(t, tail, testCaseSize-1)
	require.True(t, ok)

	// Pop one by one
	for i := 0; i < testCaseSize; i++ {
		h, ok := q.Head()
		require.Equal(t, i, h)
		require.True(t, ok)
		v, ok = q.Pop()
		require.True(t, ok)
		require.Equal(t, v, i)
	}

	// EmptyOperation
	require.True(t, q.Empty())
	require.Equal(t, 0, q.Len())
	_, ok = q.Pop()
	require.False(t, ok)
	_, ok = q.Head()
	require.False(t, ok)
	_, ok = q.Tail()
	require.False(t, ok)
}

func TestChunkQueueRandom(t *testing.T) {
	t.Parallel()

	getInt := func() int {
		return rand.Int()
	}
	doRandomTest[int](t, getInt)
	getFloat := func() float64 {
		return rand.Float64() + rand.Float64()
	}

	doRandomTest[float64](t, getFloat)

	getBool := func() bool {
		return rand.Int()%2 == 1
	}
	doRandomTest[bool](t, getBool)

	getString := func() string {
		return random.String(16)
	}
	doRandomTest[string](t, getString)

	type MyType struct {
		x int
		y string
	}
	getMyType := func() MyType {
		return MyType{1, random.String(64)}
	}

	doRandomTest[MyType](t, getMyType)

	getMyTypePtr := func() *MyType {
		return &MyType{1, random.String(64)}
	}

	doRandomTest[*MyType](t, getMyTypePtr)
}

func doRandomTest[T comparable](t *testing.T, getVal func() T) {
	const (
		opPushOne = iota
		opPushMany
		opPopOne
		opPopMany
		opPopAll
	)

	q := NewChunkQueue[T]()
	slice := make([]T, 0, 100)
	var val T
	for i := 0; i < 100; i++ {
		op := rand.Intn(4)
		if i == 99 {
			op = opPopAll
		}
		switch op {
		case opPushOne:
			val = getVal()
			q.Push(val)
			slice = append(slice, val)
		case opPushMany:
			n := rand.Intn(1024) + 1
			vals := make([]T, n)
			for j := 0; j < n; j++ {
				vals = append(vals, getVal())
			}
			q.PushMany(vals...)
			slice = append(slice, vals...)
		case opPopOne:
			if q.Empty() {
				require.Equal(t, 0, q.Len(), len(slice))
				_, ok := q.Pop()
				require.False(t, ok)
			} else {
				v, ok := q.Pop()
				require.True(t, ok)
				require.Equal(t, slice[0], v)
				slice = slice[1:]
			}
		case opPopMany:
			if q.Empty() {
				require.True(t, len(slice) == 0)
			} else {
				n := rand.Intn(q.Len()) + 1
				pops, ok := q.PopMany(n)
				require.True(t, ok)
				require.Equal(t, n, len(pops))
				popSlice := slice[0:n]
				slice = append(make([]T, 0, len(slice[n:])), slice[n:]...)

				for i := 0; i < len(pops); i++ {
					require.Equal(t, popSlice[i], pops[i])
				}
			}
		case opPopAll:
			pops := q.PopAll()
			require.Equal(t, len(pops), len(slice))
			for i := 0; i < len(pops); i++ {
				require.Equal(t, slice[i], pops[i])
			}
			slice = slice[:0]
			q.Clear()
		}

		require.Equal(t, q.Len(), len(slice))
		require.Nil(t, q.firstChunk().prev)
		require.Nil(t, q.lastChunk().next)
		freeSpace := q.Cap() - q.Len()
		if q.Empty() {
			require.True(t, freeSpace > 0 && freeSpace <= q.chunkLength)
		} else {
			require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		}
	}
}

func TestExpand(t *testing.T) {
	t.Parallel()
	q := NewChunkQueue[int]()

	for i := 0; i < testCaseSize; i++ {
		q.Push(1)
		require.Equal(t, 1, q.Len())
		freeSpace := q.Cap() - q.Len()
		require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		p, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, 1, p)
		require.True(t, q.Empty())
	}
}

func TestDequeueMany(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	x := testCaseSize
	for v := 0; v < x; v++ {
		q.Push(v)
	}
	f := 0
	for !q.Empty() {
		l := rand.Intn(q.Len()/5 + 1)
		if l == 0 {
			l = 1
		}
		vals, ok := q.PopMany(l)
		require.True(t, ok)
		for i := 0; i < l; i++ {
			require.Equal(t, f+i, vals[i])
		}
		f += len(vals)
		require.True(t, len(vals) > 0 && len(vals) <= l)

		freeSpace := q.Cap() - q.Len()
		if q.Empty() {
			require.True(t, freeSpace > 0 && freeSpace <= q.chunkLength)
		} else {
			require.True(t, freeSpace >= 0 && freeSpace < q.chunkLength)
		}
	}
	require.Equal(t, f, testCaseSize)
	require.True(t, q.Empty())
}

func TestRange(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	for i := 0; i < testCaseSize; i++ {
		q.Push(i)
	}

	var x int
	q.Range(func(v int) bool {
		if v >= 1000 {
			return false
		}
		x++
		return true
	})
	require.Equal(t, 1000, x)

	q.RangeWithIndex(func(i int, v int) bool {
		require.Equal(t, i, v)
		if i >= 1000 {
			return false
		}
		x++
		return true
	})
	require.Equal(t, 2000, x)

	q.RangeAndPop(func(v int) bool {
		return v < 1000
	})

	require.Equal(t, testCaseSize-1000, q.Len())

	process := 0
	q.RangeAndPop(func(v int) bool {
		process = v
		return true
	})
	require.Equal(t, testCaseSize-1, process)
	require.True(t, q.Empty())

	require.NotPanics(t, func() {
		q.RangeAndPop(func(v int) bool {
			return true
		})
	})
}

func TestRangeAndPop(t *testing.T) {
	t.Parallel()

	q := NewChunkQueue[int]()
	for i := 0; i < testCaseSize; i++ {
		q.Push(i)
	}

	var target int
	q.Range(func(v int) bool {
		if v >= 1000 {
			target = v
			return false
		}
		return true
	})
	require.Equal(t, 1000, target)

	q.RangeWithIndex(func(i int, v int) bool {
		require.Equal(t, i, v)
		q.Pop()
		return true
	})
	require.True(t, q.Empty())
}

func BenchmarkPush(b *testing.B) {
	b.Run("Push-ChunkQueue", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.Push(i)
		}
	})

	b.Run("Push-Slice", func(b *testing.B) {
		q := make([]int, 0, 1024)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q = append(q, i)
		}
	})

	b.Run("Push-3rdPartyDeque", func(b *testing.B) {
		q := deque.NewDeque()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.PushBack(i)
		}
	})
}

func TestChunkQueuePushMany(t *testing.T) {
	q := NewChunkQueueLeastCapacity[int](16)
	n := testCaseSize
	data := make([]int, 0, n)
	cnt := 0
	for i := 1; i < 10; i++ {
		q.PushMany(data[:n]...)
		cnt += n
		freeSpace := q.Cap() - q.Len()
		require.Equal(t, cnt, q.Len())
		require.True(t, freeSpace >= 0 && freeSpace <= q.chunkLength)
	}
}

func prepareSlice(n int) []int {
	data := make([]int, 0, n)
	for i := 0; i < n; i++ {
		data = append(data, i)
	}
	return data
}

func prepareChunkQueue(n int) *ChunkQueue[int] {
	q := NewChunkQueue[int]()
	for i := 0; i < n; i++ {
		q.Push(i)
	}
	return q
}

func BenchmarkPushMany(b *testing.B) {
	b.Run("PushMany-ChunkDeque", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](16)
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			q.PushMany(data[:n]...)
		}
	})

	b.Run("PushMany-ChunkDeque-OneByOne", func(b *testing.B) {
		q := NewChunkQueueLeastCapacity[int](16)
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			for _, v := range data {
				q.Push(v)
			}
		}
	})

	b.Run("PushMany-Slice", func(b *testing.B) {
		q := make([]int, 0, 16)
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			q = append(q, data[:n]...)
		}
	})

	b.Run("PushMany-3rdPartyDeque", func(b *testing.B) {
		q := deque.NewDeque()
		n := b.N
		data := prepareSlice(n)

		b.ResetTimer()
		for i := 1; i < 10; i++ {
			for _, v := range data {
				q.PushBack(v)
			}
		}
	})
}

func BenchmarkPopMany(b *testing.B) {
	b.Run("PopMany-ChunkDeque", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
		ls := []int{x / 5, x / 5, x / 5, x / 5, x - x/5*4}
		b.ResetTimer()
		for _, l := range ls {
			vals, ok := q.PopMany(l)
			if !ok || len(vals) != l {
				panic("error")
			}
		}
	})

	b.Run("PopMany-Slice", func(b *testing.B) {
		x := b.N
		q := prepareSlice(x)
		ls := []int{x / 5, x / 5, x / 5, x / 5, x - x/5*4}
		b.ResetTimer()
		for _, l := range ls {
			vals := q[:l]
			if len(vals) != l {
				panic("error")
			}
			q = append(make([]int, 0, len(q[l:])), q[l:]...)
		}
	})

	b.Run("PopMany-3rdPartyDeque", func(b *testing.B) {
		x := b.N
		q := deque.NewDeque()
		for i := 0; i < x; i++ {
			q.Enqueue(i)
		}
		ls := []int{x / 5, x / 5, x / 5, x / 5, x - x/5*4}
		b.ResetTimer()
		for _, l := range ls {
			if l > 0 {
				vals := q.DequeueMany(l)
				if len(vals) != l {
					fmt.Println(l, len(vals), vals[0])
					panic("error")
				}
			}
		}
	})
}

func BenchmarkChunkQueueLoopPop(b *testing.B) {
	b.Run("ChunkQueue-RangeAndPop", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
		b.ResetTimer()

		q.RangeAndPop(func(val int) bool {
			return val >= 0
		})

		require.True(b, q.Empty())
	})

	b.Run("ChunkQueue-IterateAndPop", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
		b.ResetTimer()

		for it := q.Begin(); it.Valid(); {
			if it.Value() >= 0 {
				it.Next()
				q.Pop()
			} else {
				break
			}
		}
		require.True(b, q.Empty())
	})

	b.Run("ChunkQueue-PeekAndPop", func(b *testing.B) {
		x := b.N
		q := prepareChunkQueue(x)
		b.ResetTimer()

		q.RangeAndPop(func(val int) bool {
			return val < 0
		})
		for i := 0; i < x; i++ {
			v, _ := q.Head()
			if v < 0 {
				break
			}
			q.Pop()
		}
		require.True(b, q.Empty())
	})
}
