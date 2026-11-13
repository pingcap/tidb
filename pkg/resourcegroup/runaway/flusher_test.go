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
	"errors"
	"maps"
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
		threshold:           threshold,
		mergeFn:             mergeFn,
		flushFn:             flushFn,
		batchSizeObserver:   metrics.RunawayFlusherBatchSizeHistogram.WithLabelValues("test"),
		durationObserver:    metrics.RunawayFlusherDurationHistogram.WithLabelValues("test"),
		intervalObserver:    metrics.RunawayFlusherIntervalHistogram.WithLabelValues("test"),
		flushSuccessCounter: metrics.RunawayFlusherCounter.WithLabelValues("test", metrics.LblOK),
		flushErrorCounter:   metrics.RunawayFlusherCounter.WithLabelValues("test", metrics.LblError),
		addCounter:          metrics.RunawayFlusherAddCounter.WithLabelValues("test"),
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
	re.Empty(flusher.buffer)

	flusher.add("a", 1)
	flusher.add("b", 2)
	re.Len(flusher.buffer, 2)
	re.Equal(int32(0), flushCount.Load())

	flusher.add("c", 3)
	re.Len(flusher.buffer, 0)
	re.Equal(int32(1), flushCount.Load())

	flusher.add("d", 4)
	re.Len(flusher.buffer, 1)
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
		func(m map[string]int) error { flushCount.Add(1); return nil },
	)
	re.Empty(flusher.buffer)

	flusher.add("a", 1)
	re.Len(flusher.buffer, 1)
	re.Equal(int32(0), flushCount.Load())

	flusher.flush()
	re.Len(flusher.buffer, 0)
	re.Equal(int32(1), flushCount.Load())

	flusher.add("b", 2)
	re.Len(flusher.buffer, 1)
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

func TestBatchFlusherKeepsRecordsOnError(t *testing.T) {
	re := require.New(t)

	var flushed []map[string]int
	fail := true
	flushCount := 0
	flusher := newTestBatchFlusher(
		2,
		func(m map[string]int, k string, v int) { m[k] = v },
		func(m map[string]int) error {
			flushCount++
			if fail {
				return errors.New("mock flush error")
			}
			flushed = append(flushed, maps.Clone(m))
			return nil
		},
	)

	flusher.add("a", 1)
	flusher.add("b", 2)
	re.Equal(1, flushCount)
	re.Equal(map[string]int{"a": 1, "b": 2}, flusher.buffer)
	re.True(flusher.nextRetryTime.After(time.Now()))

	// No flush is tried before the backoff ends, neither by add nor by the ticker.
	flusher.add("c", 3)
	flusher.add("d", 4)
	flusher.add("e", 5)
	flusher.flush()
	re.Equal(1, flushCount)
	re.Len(flusher.buffer, 5)

	// Another failure doubles the backoff.
	flusher.nextRetryTime = time.Time{}
	flusher.flush()
	re.Equal(2, flushCount)
	re.Equal(2*minFlushRetryBackoff, flusher.retryBackoff)

	// The kept records are flushed in batches of at most threshold records.
	fail = false
	flusher.nextRetryTime = time.Time{}
	flusher.flush()
	re.Empty(flusher.buffer)
	re.Len(flushed, 3)
	all := make(map[string]int)
	for _, batch := range flushed {
		re.LessOrEqual(len(batch), 2)
		maps.Copy(all, batch)
	}
	re.Equal(map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, all)
	re.Zero(flusher.retryBackoff)
}

func TestMergeQuarantineRecordReplacesExpiredRecord(t *testing.T) {
	re := require.New(t)

	now := time.Now()
	m := make(map[string]*QuarantineRecord)
	first := &QuarantineRecord{ResourceGroupName: "rg", WatchText: "select 1", EndTime: now.Add(time.Hour)}
	mergeQuarantineRecord(m, first.getRecordKey(), first)
	mergeQuarantineRecord(m, first.getRecordKey(), &QuarantineRecord{ResourceGroupName: "rg", WatchText: "select 1", EndTime: now.Add(2 * time.Hour)})
	re.Same(first, m[first.getRecordKey()])

	first.EndTime = now.Add(-time.Second)
	renewed := &QuarantineRecord{ResourceGroupName: "rg", WatchText: "select 1", EndTime: now.Add(time.Hour)}
	mergeQuarantineRecord(m, renewed.getRecordKey(), renewed)
	re.Same(renewed, m[renewed.getRecordKey()])

	unlimited := &QuarantineRecord{ResourceGroupName: "rg", WatchText: "select 2"}
	mergeQuarantineRecord(m, unlimited.getRecordKey(), unlimited)
	mergeQuarantineRecord(m, unlimited.getRecordKey(), &QuarantineRecord{ResourceGroupName: "rg", WatchText: "select 2"})
	re.Same(unlimited, m[unlimited.getRecordKey()])
}
