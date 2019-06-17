// Copyright 2019 PingCAP, Inc.
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

package infoschema

type slowQueryBuffer struct {
	startPos int64
	endPos   int64
	fileName string
	buf      *ringBuffer
}

// ringBuffer is not safe for concurrent read/write, but it is safe to concurrent read.
type ringBuffer struct {
	data        []interface{}
	start, next int
	full        bool
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		data:  make([]interface{}, size),
		start: 0,
		next:  0,
	}
}

func (r *ringBuffer) write(d interface{}) {
	if r.start == r.next && r.full {
		r.start++
		r.start = r.start % len(r.data)
	}
	r.data[r.next] = d
	r.next++
	if r.next >= len(r.data) {
		r.next = 0
		r.full = true
	}
}

func (r *ringBuffer) readAtStart() interface{} {
	if r.isEmpty() {
		return nil
	}
	return r.data[r.start]
}

func (r *ringBuffer) readAtEnd() interface{} {
	if r.isEmpty() {
		return nil
	}
	end := r.next - 1
	if end < 0 {
		end = len(r.data) - 1
	}
	return r.data[end]
}

func (r *ringBuffer) isEmpty() bool {
	return r.next == 0 && !r.full
}

// iterate iterates all buffered data.
func (r *ringBuffer) iterate(fn func(d interface{}) bool) {
	if r.isEmpty() {
		return
	}
	end := r.next
	if end <= r.start {
		end = len(r.data)
	}
	for i := r.start; i < end; i++ {
		if fn(r.data[i]) {
			return
		}
	}
	if r.next > r.start {
		return
	}
	end = r.next
	for i := 0; i < end; i++ {
		if fn(r.data[i]) {
			return
		}
	}
}

func (r *ringBuffer) len() int {
	if r.isEmpty() {
		return 0
	}
	if r.next <= r.start {
		return len(r.data)
	}
	return r.next - r.start
}

func (r *ringBuffer) readAll() []interface{} {
	if r.isEmpty() {
		return nil
	}
	data := make([]interface{}, 0, r.len())
	r.iterate(func(d interface{}) bool {
		data = append(data, d)
		return false
	})
	return data
}

func (r *ringBuffer) resize(size int) {
	rb := newRingBuffer(size)
	r.iterate(func(d interface{}) bool {
		rb.write(d)
		return false
	})
	r.data = rb.data
	r.start = rb.start
	r.next = rb.next
	r.full = rb.full
}

func (r *ringBuffer) clear() {
	r.start = 0
	r.next = 0
	r.full = false
}
