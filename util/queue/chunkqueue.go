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
	"sync"
	"unsafe"
)

const (
	// defaultSizePerChunk set the default size of each chunk be 1024 bytes (1kB)
	defaultSizePerChunk = 1024
	// minimumChunkLen is the minimum length of each chunk
	minimumChunkLen = 16
	// defaultPitchArrayLen is the default length of the chunk pointers array
	defaultPitchArrayLen = 16
)

// ChunkQueue is a generic, efficient, iterable and GC-friendly queue.
// Attention, it's not thread-safe.
type ChunkQueue[T any] struct {
	// [head, tail) is the section of chunks in use
	head int
	tail int

	// size is number of elements in queue
	size int

	// chunks is an array to store chunk pointers
	chunks []*chunk[T]
	// chunkLength is the max number of elements stored in every chunk.
	chunkLength  int
	chunkPool    sync.Pool
	defaultValue T
}

func (q *ChunkQueue[T]) firstChunk() *chunk[T] {
	return q.chunks[q.head]
}

func (q *ChunkQueue[T]) lastChunk() *chunk[T] {
	return q.chunks[q.tail-1]
}

// NewChunkQueue creates a new ChunkQueue
func NewChunkQueue[T any]() *ChunkQueue[T] {
	return NewChunkQueueLeastCapacity[T](1)
}

// NewChunkQueueLeastCapacity creates a ChunkQueue with an argument minCapacity.
// It requests that the queue capacity be at least minCapacity. And it's similar
// to the cap argument when making a slice using make([]T, len, cap)
func NewChunkQueueLeastCapacity[T any](minCapacity int) *ChunkQueue[T] {
	elementSize := unsafe.Sizeof(*new(T))
	if elementSize == 0 {
		// To avoid divided by zero
		elementSize = 1
	}

	chunkLength := int(defaultSizePerChunk / elementSize)
	if chunkLength < minimumChunkLen {
		chunkLength = minimumChunkLen
	}

	q := &ChunkQueue[T]{
		head:        0,
		tail:        0,
		size:        0,
		chunkLength: chunkLength,
	}
	q.chunkPool = sync.Pool{
		New: func() any {
			return newChunk[T](q.chunkLength, q)
		},
	}

	q.chunks = make([]*chunk[T], defaultPitchArrayLen)
	q.addSpace(minCapacity)
	return q
}

// Len returns the number of elements in queue
func (q *ChunkQueue[T]) Len() int {
	return q.size
}

// Cap returns the capacity of the queue. The queue can hold more elements
// than that number by automatic expansion
func (q *ChunkQueue[T]) Cap() int {
	return q.chunkLength*(q.tail-q.head) - q.chunks[q.head].l
}

// Empty indicates whether the queue is empty
func (q *ChunkQueue[T]) Empty() bool {
	return q.size == 0
}

// Peek returns the value of a given index. It does NOT support modifying the value
func (q *ChunkQueue[T]) Peek(idx int) T {
	if idx < 0 || idx >= q.size {
		panic(fmt.Sprintf("[ChunkQueue]: index %d os out of index [0, %d)", idx, q.size))
	}
	// There may some space in the former part of the chunk. Added the bias and
	// index, we can get locate the element by division
	i := q.firstChunk().l + idx
	return q.chunks[q.head+i/q.chunkLength].data[i%q.chunkLength]
}

// Replace assigns a new value to a given index
func (q *ChunkQueue[T]) Replace(idx int, val T) {
	if idx < 0 || idx >= q.size {
		panic(fmt.Sprintf("[ChunkQueue]: index %d os out of index [0, %d)", idx, q.size))
	}
	// same with Peek()
	i := q.firstChunk().l + idx
	q.chunks[q.head+i/q.chunkLength].data[i%q.chunkLength] = val
}

// Head returns the value of the first element. This method is read-only
func (q *ChunkQueue[T]) Head() (T, bool) {
	if q.Empty() {
		return q.defaultValue, false
	}
	c := q.firstChunk()
	return c.data[c.l], true
}

// Tail returns the value of the last element. This method is only for reading
// the last element, not for modification
func (q *ChunkQueue[T]) Tail() (T, bool) {
	if q.Empty() {
		return q.defaultValue, false
	}
	c := q.lastChunk()
	return c.data[c.r-1], true
}

// extend extends the space by adding chunk(s) to the queue
func (q *ChunkQueue[T]) addSpace(n int) {
	if n <= 0 {
		panic("[ChunkQueue]: n should be greater than 0")
	}
	chunksNum := (n + q.chunkLength - 1) / q.chunkLength

	// reallocate the chunks array if no enough space in the tail
	if q.tail+chunksNum+1 >= len(q.chunks) {
		q.adjustChunksArray(chunksNum)
	}

	for i := 0; i < chunksNum; i++ {
		c := q.chunkPool.Get().(*chunk[T])
		c.queue = q
		q.chunks[q.tail] = c
		if q.tail > q.head {
			c.prev = q.chunks[q.tail-1]
			q.chunks[q.tail-1].next = c
		}
		q.tail++
	}
}

// adjustChunksArray extends/shrinks the chunk pointers array []*chunks, and
// eliminates the former spaces caused by popped chunks:
//  1. extend > 0: A positive expand represents an "extend" operation:
//     The value is the amount of space the array should have in tail. Expand the
//     chunks array until there is enough space.
//  2. extend < 0: A negative expand represents a "shrink" operation:
//     The value of a negative extend is oblivious. The new length of the array
//     []*chunks is max(defaultLength, tail - head + 1), which makes sure
func (q *ChunkQueue[T]) adjustChunksArray(extend int) {
	used := q.tail - q.head
	// adjust the array length. The new length should
	var newLen int
	switch {
	case extend > 0:
		newLen = len(q.chunks)
		// Expand the array if no enough space.
		for used+extend+1 >= newLen {
			newLen *= 2
		}
	case extend < 0:
		// for shrink, the new length is max(defaultLength, tail - head + 1)
		newLen = used + 1
		if newLen < defaultPitchArrayLen {
			newLen = defaultPitchArrayLen
		}
	}
	if newLen != len(q.chunks) {
		// If the length changed, allocate a new array and do copy
		newChunks := make([]*chunk[T], newLen)
		copy(newChunks[:used], q.chunks[q.head:q.tail])
		q.chunks = newChunks
	} else if q.head > 0 {
		// If the new array length remains the same, then there is no need to
		// create a new array, and only move the elements to front slots
		copy(q.chunks[:used], q.chunks[q.head:q.tail])
		for i := used; i < q.tail; i++ {
			q.chunks[i] = nil
		}
	}
	q.tail -= q.head
	q.head = 0
}

// Push enqueues an element to tail
func (q *ChunkQueue[T]) Push(v T) {
	c := q.lastChunk()
	if c.r == q.chunkLength {
		q.addSpace(1)
		c = q.lastChunk()
	}

	c.data[c.r] = v
	c.r++
	q.size++
}

// PushMany enqueues multiple elements at a time
func (q *ChunkQueue[T]) PushMany(vals ...T) {
	cnt, n := 0, len(vals)
	c := q.lastChunk()
	if q.Cap()-q.Len() < n {
		q.addSpace(n - (q.chunkLength - c.r))
	}

	if c.r == q.chunkLength {
		c = c.next
	}

	var addLen int
	for n > 0 {
		addLen = q.chunkLength - c.r
		if addLen > n {
			addLen = n
		}
		copy(c.data[c.r:c.r+addLen], vals[cnt:cnt+addLen])
		c.r += addLen
		q.size += addLen
		cnt += addLen
		c = c.next
		n -= addLen
	}
}

// Pop dequeues an element from head. The second return value is true on
// success, and false if the queue is empty and no element can be popped
func (q *ChunkQueue[T]) Pop() (T, bool) {
	if q.Empty() {
		return q.defaultValue, false
	}

	c := q.firstChunk()
	v := c.data[c.l]
	c.data[c.l] = q.defaultValue
	c.l++
	q.size--

	if c.l == q.chunkLength {
		q.popChunk()
	}
	return v, true
}

func (q *ChunkQueue[T]) popChunk() {
	c := q.firstChunk()
	if c.next == nil {
		q.addSpace(1)
	}
	q.chunks[q.head] = nil
	q.head++
	q.chunks[q.head].prev = nil

	c.reset()
	q.chunkPool.Put(c)
}

// PopAll dequeues all elements in the queue
func (q *ChunkQueue[T]) PopAll() []T {
	v, _ := q.PopMany(q.Len())
	return v
}

// PopMany dequeues n elements at a time. The second return value is true
// if n elements were popped out, and false otherwise.
func (q *ChunkQueue[T]) PopMany(n int) ([]T, bool) {
	if n < 0 {
		panic(fmt.Sprintf("negative pop number %v", n))
	}

	ok := n <= q.size
	if q.size < n {
		n = q.size
	}

	res := make([]T, n)
	cnt := 0
	for i := q.head; i < q.tail && cnt < n; i++ {
		c := q.chunks[i]
		popLen := c.len()
		if n-cnt < popLen {
			popLen = n - cnt
		}
		for j := 0; j < popLen; j++ {
			res[cnt+j] = c.data[c.l+j]
			c.data[c.l+j] = q.defaultValue
		}
		c.l += popLen
		cnt += popLen
		q.size -= popLen

		if c.l == q.chunkLength {
			q.popChunk()
		}
	}
	return res, ok
}

// Clear clears the queue and shrinks the chunks array
func (q *ChunkQueue[T]) Clear() {
	if !q.Empty() {
		emptyChunk := make([]T, q.chunkLength)
		for i := q.head; i < q.tail; i++ {
			q.size -= q.chunks[i].len()
			copy(q.chunks[i].data[:], emptyChunk[:])
			q.popChunk()
		}
	}
	// Shrink the chunks array
	q.Shrink()
}

// Shrink shrinks the space of the chunks array
func (q *ChunkQueue[T]) Shrink() {
	q.adjustChunksArray(-1)
}

// Range iterates the queue from head to the first element e that f(e) returns
// false, or to the end if f() is true for all elements.
func (q *ChunkQueue[T]) Range(f func(e T) bool) {
	var c *chunk[T]
	for i := q.head; i < q.tail; i++ {
		c = q.chunks[i]
		for j := c.l; j < c.r; j++ {
			if !f(c.data[j]) {
				return
			}
		}
	}
}

// RangeWithIndex iterates the queue with index from head. the first element e
// with index i that f(i, e) is false, or to tail if f() is true for all elements.
func (q *ChunkQueue[T]) RangeWithIndex(f func(idx int, e T) bool) {
	var c *chunk[T]
	idx := 0
	for i := q.head; i < q.tail; i++ {
		c = q.chunks[i]
		for j := c.l; j < c.r; j++ {
			if !f(idx, c.data[j]) {
				return
			}
			idx++
		}
	}
}

// RangeAndPop iterate the queue from head, and pop the element til the first
// element e that f(e) is false, or all elements if f(e) is true for all elements.
// This method is more convenient than Peek and Pop
func (q *ChunkQueue[T]) RangeAndPop(f func(e T) bool) {
	var c *chunk[T]

	for i := q.head; !q.Empty() && i < q.tail; i++ {
		c = q.chunks[i]
		for j := c.l; j < c.r; j++ {
			if f(c.data[j]) {
				c.data[c.l] = q.defaultValue
				c.l++
				q.size--
			} else {
				return
			}
		}
		if c.l == q.chunkLength {
			q.popChunk()
		}
	}
}
