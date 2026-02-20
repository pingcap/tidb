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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func newTestBatchFlusher[K comparable, V any](
	threshold int,
	mergeFn func(map[K]V, K, V),
	flushFn func(map[K]V) error,
) *batchFlusher[K, V] {
	return &batchFlusher[K, V]{
		name:                "test",
		buffer:              make(map[K]V, threshold),
		ticker:              time.NewTicker(time.Hour),
		flushCh:             make(chan struct{}, 1),
		stopCh:              make(chan struct{}),
		done:                make(chan struct{}),
		mergeFn:             mergeFn,
		flushFn:             flushFn,
		batchSizeObserver:   metrics.RunawayFlusherBatchSizeHistogram.WithLabelValues("test"),
		durationObserver:    metrics.RunawayFlusherDurationHistogram.WithLabelValues("test"),
		intervalObserver:    metrics.RunawayFlusherIntervalHistogram.WithLabelValues("test"),
		flushSuccessCounter: metrics.RunawayFlusherCounter.WithLabelValues("test", metrics.LblOK),
		flushErrorCounter:   metrics.RunawayFlusherCounter.WithLabelValues("test", metrics.LblError),
		addCounter:          metrics.RunawayFlusherAddCounter.WithLabelValues("test"),
		dropCounter:         metrics.RunawayFlusherDropCounter.WithLabelValues("test"),
	}
}

func TestBatchFlusherAdd(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		3,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) error { flushCount.Add(1); return nil },
	)
	re.Equal(0, flusher.bufferLen())

	flusher.add("a", 1)
	flusher.add("b", 2)
	re.Equal(2, flusher.bufferLen())
	re.Equal(int32(0), flushCount.Load())

	// Threshold reached: notifyFlush is called but flush happens asynchronously in run().
	// Since run() is not started in this test, we manually flush.
	flusher.add("c", 3)
	// The buffer still has 3 items because run() is not consuming flushCh.
	// Manually flush to simulate what run() would do.
	flusher.flush()
	re.Equal(0, flusher.bufferLen())
	re.Equal(int32(1), flushCount.Load())

	flusher.add("d", 4)
	re.Equal(1, flusher.bufferLen())
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
		func(m map[string]*Record) error { lastBuffer = m; return nil },
	)

	flusher.add("key1", &Record{SQLDigest: "d1", Repeats: 1})
	flusher.add("key1", &Record{SQLDigest: "d1", Repeats: 1})
	flusher.add("key1", &Record{SQLDigest: "d1", Repeats: 1})
	flusher.add("key2", &Record{SQLDigest: "d2", Repeats: 1})

	re.Equal(2, flusher.bufferLen())

	flusher.flush()
	re.Equal(0, flusher.bufferLen())
	re.Equal(3, lastBuffer["key1"].Repeats)
	re.Equal(1, lastBuffer["key2"].Repeats)
}

func TestBatchFlusherFlush(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		100,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) error { flushCount.Add(1); return nil },
	)
	re.Equal(0, flusher.bufferLen())

	flusher.add("a", 1)
	re.Equal(1, flusher.bufferLen())
	re.Equal(int32(0), flushCount.Load())

	flusher.flush()
	re.Equal(0, flusher.bufferLen())
	re.Equal(int32(1), flushCount.Load())

	flusher.add("b", 2)
	re.Equal(1, flusher.bufferLen())
	re.Equal(int32(1), flushCount.Load())
}

func TestBatchFlusherFlushEmpty(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		10,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) error { flushCount.Add(1); return nil },
	)

	flusher.flush()
	re.Equal(int32(0), flushCount.Load())

	flusher.add("a", 1)
	flusher.flush()
	re.Equal(int32(1), flushCount.Load())

	flusher.flush()
	re.Equal(int32(1), flushCount.Load())
}

func TestBatchFlusherConcurrentAdd(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		1000, // high threshold so flush is not triggered during add
		func(m map[string]int, k string, v int) {
			if existing, ok := m[k]; ok {
				m[k] = existing + v
			} else {
				m[k] = v
			}
		},
		func(m map[string]int) error { flushCount.Add(1); return nil },
	)

	const numGoroutines = 100
	const addsPerGoroutine = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range addsPerGoroutine {
				key := "key" + string(rune('A'+id%26))
				flusher.add(key, j)
			}
		}(i)
	}
	wg.Wait()

	// All adds should have been captured without panic or race.
	re.Greater(flusher.bufferLen(), 0)
	re.LessOrEqual(flusher.bufferLen(), 26) // at most 26 unique keys

	flusher.flush()
	re.Equal(0, flusher.bufferLen())
	re.Equal(int32(1), flushCount.Load())
}

func TestBatchFlusherMaxBufferSize(t *testing.T) {
	re := require.New(t)

	mergeFn := func(m map[string]int, k string, v int) {
		if existing, ok := m[k]; ok {
			m[k] = existing + v
		} else {
			m[k] = v
		}
	}
	flusher := newTestBatchFlusher(
		maxBufferSize+100,
		mergeFn,
		func(m map[string]int) error { return nil },
	)

	// Pre-fill buffer to maxBufferSize.
	flusher.mu.Lock()
	for i := range maxBufferSize {
		flusher.buffer[fmt.Sprintf("prefill-%d", i)] = i
	}
	flusher.mu.Unlock()
	re.Equal(maxBufferSize, flusher.bufferLen())

	// New key should be dropped.
	flusher.add("new-key-1", 100)
	re.Equal(maxBufferSize, flusher.bufferLen())

	// Another new key should also be dropped.
	flusher.add("new-key-2", 200)
	re.Equal(maxBufferSize, flusher.bufferLen())

	// Existing key should still be merged.
	flusher.add("prefill-0", 42)
	re.Equal(maxBufferSize, flusher.bufferLen())
	flusher.mu.Lock()
	re.Equal(42, flusher.buffer["prefill-0"]) // merged: 0 + 42 = 42
	flusher.mu.Unlock()

	// After flush, new keys can be added again.
	flusher.flush()
	re.Equal(0, flusher.bufferLen())
	flusher.add("new-key-1", 100)
	re.Equal(1, flusher.bufferLen())
}

func TestBatchFlusherRunAndStop(t *testing.T) {
	re := require.New(t)

	var flushCount atomic.Int32
	flusher := newTestBatchFlusher(
		100,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) error { flushCount.Add(1); return nil },
	)

	go flusher.run()

	// Add some items and trigger flush via notifyFlush.
	flusher.add("a", 1)
	flusher.add("b", 2)
	flusher.notifyFlush()

	// Wait briefly for the flush goroutine to process.
	time.Sleep(50 * time.Millisecond)
	re.Equal(int32(1), flushCount.Load())
	re.Equal(0, flusher.bufferLen())

	// Add more items, then stop. stop() should flush remaining.
	flusher.add("c", 3)
	flusher.stop()

	re.Equal(int32(2), flushCount.Load())
	re.Equal(0, flusher.bufferLen())
}
