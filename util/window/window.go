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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import "golang.org/x/exp/constraints"

// Bucket contains multiple float64 points.
type Bucket[T constraints.Integer | constraints.Float] struct {
	Points []T
	Count  int64
	next   *Bucket[T]
}

// Append appends the given value to the bucket.
func (b *Bucket[T]) Append(val T) {
	b.Points = append(b.Points, val)
	b.Count++
}

// Add adds the given value to the point.
func (b *Bucket[T]) Add(offset int, val T) {
	b.Points[offset] += val
	b.Count++
}

// Reset empties the bucket.
func (b *Bucket[T]) Reset() {
	b.Points = b.Points[:0]
	b.Count = 0
}

// Next returns the next bucket.
func (b *Bucket[T]) Next() *Bucket[T] {
	return b.next
}

// Window contains multiple buckets.
type Window[T constraints.Integer | constraints.Float] struct {
	buckets []Bucket[T]
	size    int
}

// Options contains the arguments for creating Window.
type Options struct {
	Size int
}

// NewWindow creates a new Window based on WindowOpts.
func NewWindow[T constraints.Integer | constraints.Float](opts Options) *Window[T] {
	buckets := make([]Bucket[T], opts.Size)
	for offset := range buckets {
		buckets[offset].Points = make([]T, 0)
		nextOffset := offset + 1
		if nextOffset == opts.Size {
			nextOffset = 0
		}
		buckets[offset].next = &buckets[nextOffset]
	}
	return &Window[T]{buckets: buckets, size: opts.Size}
}

// ResetWindow empties all buckets within the window.
func (w *Window[T]) ResetWindow() {
	for offset := range w.buckets {
		w.ResetBucket(offset)
	}
}

// ResetBucket empties the bucket based on the given offset.
func (w *Window[T]) ResetBucket(offset int) {
	w.buckets[offset%w.size].Reset()
}

// ResetBuckets empties the buckets based on the given offsets.
func (w *Window[T]) ResetBuckets(offset int, count int) {
	for i := 0; i < count; i++ {
		w.ResetBucket(offset + i)
	}
}

// Append appends the given value to the bucket where index equals the given offset.
func (w *Window[T]) Append(offset int, val T) {
	w.buckets[offset%w.size].Append(val)
}

// Add adds the given value to the latest point within bucket where index equals the given offset.
func (w *Window[T]) Add(offset int, val T) {
	offset %= w.size
	if w.buckets[offset].Count == 0 {
		w.buckets[offset].Append(val)
		return
	}
	w.buckets[offset].Add(0, val)
}

// Bucket returns the bucket where index equals the given offset.
func (w *Window[T]) Bucket(offset int) Bucket[T] {
	return w.buckets[offset%w.size]
}

// Size returns the size of the window.
func (w *Window[T]) Size() int {
	return w.size
}

// Iterator returns the count number buckets iterator from offset.
func (w *Window[T]) Iterator(offset int, count int) Iterator[T] {
	return Iterator[T]{
		count: count,
		cur:   &w.buckets[offset%w.size],
	}
}
