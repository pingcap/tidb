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

// TestBatchFlusherMergeFn verifies that repeated adds for the same key accumulate Repeats
// and advance UpdateTime to the latest occurrence, and that the merged state is what gets flushed.
func TestBatchFlusherMergeFn(t *testing.T) {
	re := require.New(t)

	var lastBuffer map[recordKey]*Record
	flusher := newTestBatchFlusher(
		10,
		func(m map[recordKey]*Record, k recordKey, v *Record) {
			if existing, ok := m[k]; ok {
				existing.Repeats++
				if v.UpdateTime.After(existing.UpdateTime) {
					existing.UpdateTime = v.UpdateTime
				}
			} else {
				m[k] = v
			}
		},
		// capture the buffer passed to flushFn so we can assert its contents after flush.
		func(m map[recordKey]*Record) error { lastBuffer = m; return nil },
	)

	k1 := recordKey{ResourceGroupName: "rg", SQLDigest: "d1", PlanDigest: "p1", Match: "identify"}
	k2 := recordKey{ResourceGroupName: "rg", SQLDigest: "d2", PlanDigest: "p2", Match: "identify"}

	t0 := time.Now()
	// same key added three times with advancing timestamps — should be merged into one buffer entry.
	flusher.add(k1, &Record{SQLDigest: "d1", UpdateTime: t0, Repeats: 1})
	flusher.add(k1, &Record{SQLDigest: "d1", UpdateTime: t0.Add(time.Second), Repeats: 1})
	flusher.add(k1, &Record{SQLDigest: "d1", UpdateTime: t0.Add(2 * time.Second), Repeats: 1})
	// different key added once — must remain independent.
	flusher.add(k2, &Record{SQLDigest: "d2", UpdateTime: t0, Repeats: 1})

	// two distinct keys in buffer, not three.
	re.Len(flusher.buffer, 2)
	// three adds for k1 → Repeats=3.
	re.Equal(3, flusher.buffer[k1].Repeats)
	// UpdateTime must be the latest of the three timestamps, not the first.
	re.Equal(t0.Add(2*time.Second), flusher.buffer[k1].UpdateTime, "UpdateTime should advance to the latest occurrence")
	// k2 was added only once, Repeats stays 1.
	re.Equal(1, flusher.buffer[k2].Repeats)

	flusher.flush()
	// buffer is cleared after flush.
	re.Len(flusher.buffer, 0)
	// flushFn received the merged state, not three separate records.
	re.Equal(3, lastBuffer[k1].Repeats)
	re.Equal(t0.Add(2*time.Second), lastBuffer[k1].UpdateTime)
}

// TestGenRunawayQueriesStmtODKU verifies the generated SQL contains the ON DUPLICATE KEY UPDATE
// clause and that start_time/update_time/repeats land in the correct parameter positions.
func TestGenRunawayQueriesStmtODKU(t *testing.T) {
	re := require.New(t)

	// t0 = first-seen time (start_time, must never change on duplicate).
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// t1 = latest activity time (update_time, advanced by ODKU on duplicate).
	t1 := t0.Add(time.Minute)
	k := recordKey{ResourceGroupName: "rg1", SQLDigest: "sqldig", PlanDigest: "plandig", Match: "identify"}
	m := map[recordKey]*Record{
		k: {
			ResourceGroupName: "rg1",
			StartTime:         t0,
			UpdateTime:        t1,
			Match:             "identify",
			Action:            "cooldown",
			SampleText:        "select 1",
			SQLDigest:         "sqldig",
			PlanDigest:        "plandig",
			Source:            "127.0.0.1:4000",
			ExceedCause:       "cpu",
			Repeats:           3,
		},
	}

	sql, params := genRunawayQueriesStmt(m)

	// ODKU clause must be present for cluster-wide dedup to work.
	re.Contains(sql, "ON DUPLICATE KEY UPDATE repeats = repeats + VALUES(repeats), update_time = GREATEST(update_time, VALUES(update_time))")
	// update_time column must appear in the INSERT column list.
	re.Contains(sql, "update_time")
	// one row × 11 columns (was 10 before update_time was added).
	re.Len(params, 11)
	// positional checks — column order is: resource_group_name, start_time, update_time, …, repeats.
	re.Equal("rg1", params[0]) // resource_group_name
	re.Equal(t0, params[1])    // start_time — first-seen, must not be overwritten on duplicate
	re.Equal(t1, params[2])    // update_time — latest activity, propagated via ODKU
	re.Equal(3, params[10])    // repeats
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
