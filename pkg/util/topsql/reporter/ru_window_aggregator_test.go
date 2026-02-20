// Copyright 2026 PingCAP, Inc.
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

package reporter

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestRUWindowAggregatorReportGranularity(t *testing.T) {
	run := func(granularity uint64, expectedTS []uint64, expectedRU []float64) {
		agg := newRUWindowAggregator()
		key := stmtstats.RUKey{
			User:       "u1",
			SQLDigest:  stmtstats.BinaryDigest("sql1"),
			PlanDigest: stmtstats.BinaryDigest("plan1"),
		}
		agg.addBatchToBucket(1, stmtstats.RUIncrementMap{
			key: {TotalRU: 1, ExecCount: 1, ExecDuration: 10},
		})
		agg.addBatchToBucket(16, stmtstats.RUIncrementMap{
			key: {TotalRU: 2, ExecCount: 1, ExecDuration: 20},
		})
		agg.addBatchToBucket(31, stmtstats.RUIncrementMap{
			key: {TotalRU: 3, ExecCount: 1, ExecDuration: 30},
		})
		agg.addBatchToBucket(46, stmtstats.RUIncrementMap{
			key: {TotalRU: 4, ExecCount: 1, ExecDuration: 40},
		})

		records := agg.takeReportRecords(60, granularity, []byte("ks"))
		require.NotEmpty(t, records)

		rec := findRURecord(t, records, "u1", "sql1", "plan1")
		require.Len(t, rec.Items, len(expectedTS))
		for i := range expectedTS {
			require.Equal(t, expectedTS[i], rec.Items[i].TimestampSec)
			require.InDelta(t, expectedRU[i], rec.Items[i].TotalRu, 1e-9)
		}
	}

	run(15, []uint64{0, 15, 30, 45}, []float64{1, 2, 3, 4})
	run(30, []uint64{0, 30}, []float64{3, 7})
	run(60, []uint64{0}, []float64{10})
}

func TestRUWindowAggregatorCompactTo200(t *testing.T) {
	agg := newRUWindowAggregator()
	batch := make(stmtstats.RUIncrementMap, 250)
	for i := 0; i < 250; i++ {
		batch[stmtstats.RUKey{
			User:       fmt.Sprintf("u%03d", i),
			SQLDigest:  stmtstats.BinaryDigest("sql"),
			PlanDigest: stmtstats.BinaryDigest("plan"),
		}] = &stmtstats.RUIncrement{
			TotalRU:      float64(250 - i), // keep deterministic ranking
			ExecCount:    1,
			ExecDuration: 1,
		}
	}
	agg.addBatchToBucket(1, batch)
	agg.addBatchToBucket(16, stmtstats.RUIncrementMap{
		stmtstats.RUKey{
			User:       "next",
			SQLDigest:  stmtstats.BinaryDigest("sql"),
			PlanDigest: stmtstats.BinaryDigest("plan"),
		}: {TotalRU: 1},
	})

	bucket := agg.buckets[0]
	require.NotNil(t, bucket)
	require.Nil(t, bucket.collecting)
	require.NotNil(t, bucket.compactedCollecting)

	// Count normal users in compacted snapshot
	normalUsers := len(bucket.compactedCollecting.users)
	require.LessOrEqual(t, normalUsers, ruCompactedTopNUsers)
}

func TestRUWindowAggregatorTakeOncePerWindow(t *testing.T) {
	agg := newRUWindowAggregator()
	agg.addBatchToBucket(1, stmtstats.RUIncrementMap{
		stmtstats.RUKey{
			User:       "u1",
			SQLDigest:  stmtstats.BinaryDigest("sql1"),
			PlanDigest: stmtstats.BinaryDigest("plan1"),
		}: {TotalRU: 1, ExecCount: 1, ExecDuration: 1},
	})

	require.Nil(t, agg.takeReportRecords(59, 60, []byte("ks")))
	require.NotNil(t, agg.takeReportRecords(60, 60, []byte("ks")))
	require.Nil(t, agg.takeReportRecords(61, 60, []byte("ks")))
}

// Test gap 3: Concurrent pressure test for ruWindowAggregator.
// Verifies that under high goroutine contention, addBatchToBucket does not panic
// or lose structural integrity (records still produce valid reports).
func TestRUWindowAggregatorConcurrentPressure(t *testing.T) {
	const (
		numWriters       = 16
		batchesPerWriter = 100
		numUsers         = 50
	)
	agg := newRUWindowAggregator()

	// Concurrent writers: each goroutine sends batches for ts in [0..14] (first 15s bucket)
	done := make(chan int, numWriters)
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			dropped := 0
			for i := 0; i < batchesPerWriter; i++ {
				ts := uint64(i % 15)
				batch := make(stmtstats.RUIncrementMap, numUsers)
				for u := 0; u < numUsers; u++ {
					batch[stmtstats.RUKey{
						User:       fmt.Sprintf("u%d", u),
						SQLDigest:  stmtstats.BinaryDigest(fmt.Sprintf("sql%d_%d", writerID, i)),
						PlanDigest: stmtstats.BinaryDigest("plan"),
					}] = &stmtstats.RUIncrement{TotalRU: 1, ExecCount: 1, ExecDuration: 1}
				}
				agg.addBatchToBucket(ts, batch)
			}
			done <- dropped
		}(w)
	}

	totalDropped := 0
	for range numWriters {
		totalDropped += <-done
	}

	// Also inject data for 15..59 so we have a full 60s window
	for ts := uint64(15); ts < 60; ts += 15 {
		agg.addBatchToBucket(ts, stmtstats.RUIncrementMap{
			stmtstats.RUKey{
				User:       "u0",
				SQLDigest:  stmtstats.BinaryDigest("sql_filler"),
				PlanDigest: stmtstats.BinaryDigest("plan"),
			}: {TotalRU: 1, ExecCount: 1, ExecDuration: 1},
		})
	}

	records := agg.takeReportRecords(60, 60, []byte("ks"))
	require.NotNil(t, records, "concurrent writes should produce non-nil report")

	// Verify structural integrity: all records have valid items
	totalRU := 0.0
	for _, rec := range records {
		for _, item := range rec.Items {
			require.True(t, item.TotalRu >= 0, "negative RU in output")
			totalRU += item.TotalRu
		}
	}
	require.Greater(t, totalRU, 0.0, "total reported RU should be positive")
}

func findRURecord(t *testing.T, records []tipb.TopRURecord, user, sqlDigest, planDigest string) tipb.TopRURecord {
	t.Helper()
	for _, rec := range records {
		if rec.User == user && string(rec.SqlDigest) == sqlDigest && string(rec.PlanDigest) == planDigest {
			return rec
		}
	}
	t.Fatalf("record not found: user=%s sql=%s plan=%s", user, sqlDigest, planDigest)
	return tipb.TopRURecord{}
}
