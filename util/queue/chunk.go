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

type chunk[T any] struct {
	// [l, r) is a left open right closed interval,
	// that indicates invalid elements within the chunk
	l    int
	r    int
	data []T

	prev *chunk[T]
	next *chunk[T]

	// a pointer points to the queue
	queue *ChunkQueue[T]
}

func (c *chunk[T]) empty() bool {
	return c.l >= c.r
}

func (c *chunk[T]) len() int {
	return c.r - c.l
}

func (c *chunk[T]) reset() {
	c.l, c.r = 0, 0
	c.prev, c.next, c.queue = nil, nil, nil
}

func newChunk[T any](size int, q *ChunkQueue[T]) *chunk[T] {
	return &chunk[T]{
		data:  make([]T, size),
		queue: q,
	}
}
