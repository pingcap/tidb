// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runaway

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestBatchFlusher[K comparable, V any](
	threshold int,
	mergeFn func(map[K]V, K, V),
	flushFn func(map[K]V),
) *batchFlusher[K, V] {
	return &batchFlusher[K, V]{
		name:      "test",
		buffer:    make(map[K]V, threshold),
		timer:     time.NewTimer(time.Hour),
		interval:  time.Hour,
		threshold: threshold,
		mergeFn:   mergeFn,
		flushFn:   flushFn,
	}
}

func TestBatchFlusherAdd(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		3,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) { flushCount.Add(1) },
	)
	re.Empty(flusher.buffer)

	flusher.add("a", 1)
	flusher.add("b", 2)
	re.Len(flusher.buffer, 2)
	re.False(flusher.flushed)
	re.Equal(int32(0), flushCount.Load())

	flusher.add("c", 3)
	re.Len(flusher.buffer, 0)
	re.True(flusher.flushed)
	re.Equal(int32(1), flushCount.Load())

	flusher.add("d", 4)
	re.Len(flusher.buffer, 1)
	re.False(flusher.flushed)
	re.Equal(int32(1), flushCount.Load())
}

func TestBatchFlusherMergeFn(t *testing.T) {
	re := require.New(t)

	var lastBuffer map[string]*Record
	flusher := newTestBatchFlusher(
		10,
		func(m map[string]*Record, k string, v *Record) {
			if existing, ok := m[k]; ok {
				existing.Repeats++
			} else {
				m[k] = v
			}
		},
		func(m map[string]*Record) { lastBuffer = m },
	)

	flusher.add("key1", &Record{SQLDigest: "d1", Repeats: 1})
	flusher.add("key1", &Record{SQLDigest: "d1", Repeats: 1})
	flusher.add("key1", &Record{SQLDigest: "d1", Repeats: 1})
	flusher.add("key2", &Record{SQLDigest: "d2", Repeats: 1})

	re.Len(flusher.buffer, 2)
	re.Equal(3, flusher.buffer["key1"].Repeats)
	re.Equal(1, flusher.buffer["key2"].Repeats)

	flusher.flush()
	re.Len(flusher.buffer, 0)
	re.Equal(3, lastBuffer["key1"].Repeats)
}

func TestBatchFlusherFlush(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		100,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) { flushCount.Add(1) },
	)
	re.Empty(flusher.buffer)

	flusher.add("a", 1)
	re.Len(flusher.buffer, 1)
	re.False(flusher.flushed)
	re.Equal(int32(0), flushCount.Load())

	flusher.flush()
	re.Len(flusher.buffer, 0)
	re.True(flusher.flushed)
	re.Equal(int32(1), flushCount.Load())

	flusher.add("b", 2)
	re.Len(flusher.buffer, 1)
	re.False(flusher.flushed)
	re.Equal(int32(1), flushCount.Load())
}

func TestBatchFlusherFlushEmpty(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		10,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) { flushCount.Add(1) },
	)

	flusher.flush()
	re.Equal(int32(0), flushCount.Load())

	flusher.add("a", 1)
	flusher.flush()
	re.Equal(int32(1), flushCount.Load())

	flusher.flush()
	re.Equal(int32(1), flushCount.Load())
}
