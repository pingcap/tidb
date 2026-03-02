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

// makeRUBatch creates a RUIncrementMap with numUsers users and numSQLsPerUser SQLs per user (up to numUsers*numSQLsPerUser keys).
func makeRUBatch(numUsers, numSQLsPerUser int) stmtstats.RUIncrementMap {
	batch := make(stmtstats.RUIncrementMap, numUsers*numSQLsPerUser)
	for u := 0; u < numUsers; u++ {
		for s := 0; s < numSQLsPerUser; s++ {
			key := stmtstats.RUKey{
				User:       fmt.Sprintf("u%04d", u),
				SQLDigest:  stmtstats.BinaryDigest(fmt.Sprintf("sql%04d_%04d", u, s)),
				PlanDigest: stmtstats.BinaryDigest("plan"),
			}
			batch[key] = &stmtstats.RUIncrement{TotalRU: float64(numUsers*numSQLsPerUser - u*numSQLsPerUser - s), ExecCount: 1, ExecDuration: 1}
		}
	}
	return batch
}

// fillAggregatorForWindow fills the aggregator so that takeReportRecords(windowEnd, ...) has four 15s buckets (0,15,30,45) with data.
// Used by benchmarks to set up state before measuring takeReportRecords.
func fillAggregatorForWindow(agg *ruWindowAggregator, windowEnd uint64, numUsers, numSQLsPerUser int) {
	if windowEnd < ruReportWindowSeconds {
		return
	}
	batch := makeRUBatch(numUsers, numSQLsPerUser)
	// One batch per 15s bucket so we have 4 buckets for a 60s window.
	agg.addBatchToBucket(1, batch)
	agg.addBatchToBucket(16, batch)
	agg.addBatchToBucket(31, batch)
	agg.addBatchToBucket(46, batch)
}

// fillAggregatorSteadyState60s fills the aggregator to the 60s boundary steady state:
// 4 buckets (0,15,30,45) compacted to 200×200 each, and 1 bucket (60) still collecting at 400×400 cap.
// Adding a batch to ts=61 triggers rotateBucketsBefore(60), which compacts the four buckets.
// Used by BenchmarkTakeReportRecords_Large_60s_AtDesignLimit to measure takeReportRecords under design-limit load.
func fillAggregatorSteadyState60s(agg *ruWindowAggregator) {
	batch := makeRUBatch(maxPreTopNUsers, maxPreTopNSQLsPerUser)
	agg.addBatchToBucket(1, batch)
	agg.addBatchToBucket(16, batch)
	agg.addBatchToBucket(31, batch)
	agg.addBatchToBucket(46, batch)
	agg.addBatchToBucket(61, batch) // triggers rotation: 0,15,30,45 compact to 200×200; bucket 60 starts collecting 400×400
}

// fillAggregatorSteadyState60sAt10kKeys fills the aggregator to the same 4 compacted + 1 collecting shape
// but with 10k keys per batch (100×100), simulating maxRUKeysPerAggregate=10000 upstream.
// Used by BenchmarkTakeReportRecords_Large_60s_At10kKeys to compare with the 160k design-limit benchmark.
func fillAggregatorSteadyState60sAt10kKeys(agg *ruWindowAggregator) {
	const numUsers, numSQLsPerUser = 200, 200 // 10k keys
	batch := makeRUBatch(numUsers, numSQLsPerUser)
	agg.addBatchToBucket(1, batch)
	agg.addBatchToBucket(16, batch)
	agg.addBatchToBucket(31, batch)
	agg.addBatchToBucket(46, batch)
	agg.addBatchToBucket(61, batch) // triggers rotation; 4 buckets compact to 200×200 (with ≤100×100 data); bucket 60 has 10k keys
}

func TestRUWindowAggregatorReportGranularity(t *testing.T) {
	// Contract: the same four 15s buckets should be regrouped by item interval.
	// This verifies 15/30/60 aggregation boundaries on a fixed 60s closed window.
	run := func(interval uint64, expectedTS []uint64, expectedRU []float64) {
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

		records := agg.takeReportRecords(60, interval, []byte("ks"))
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
	// Build >200 users in one 15s bucket, then force rotation by writing to next bucket.
	// Contract: rotated bucket is compacted and normal users are capped by maxTopUsers.
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
	require.LessOrEqual(t, normalUsers, maxTopUsers)
}

func TestRUWindowAggregatorTakeOncePerWindow(t *testing.T) {
	// Contract: one aligned 60s window can be reported at most once.
	// 59 is still open, 60 closes [0,60), and later calls should not re-emit it.
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

	// Concurrent writers all hit the same 15s bucket to maximize lock contention.
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

	// Add filler points for later buckets so [0,60) is a complete reportable window.
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

	// We only assert structural integrity here, not exact totals, to avoid
	// coupling this stress test to internal compaction choices.
	totalRU := 0.0
	for _, rec := range records {
		for _, item := range rec.Items {
			require.True(t, item.TotalRu >= 0, "negative RU in output")
			totalRU += item.TotalRu
		}
	}
	require.Greater(t, totalRU, 0.0, "total reported RU should be positive")
}

func TestRUWindowAggregator_DropsLateDataAfterWindowReported(t *testing.T) {
	// A closed [0,60) window can be reported only once, and later writes to that window must be dropped.
	agg := newRUWindowAggregator()

	agg.addBatchToBucket(1, stmtstats.RUIncrementMap{
		{
			User:       "u1",
			SQLDigest:  stmtstats.BinaryDigest("sql-a"),
			PlanDigest: stmtstats.BinaryDigest("plan-a"),
		}: {TotalRU: 10, ExecCount: 1, ExecDuration: 10},
	})
	first := agg.takeReportRecords(60, 60, []byte("ks"))
	require.NotNil(t, first)
	require.NotNil(t, findRURecordByDigest(first, "u1", "sql-a", "plan-a"))

	// Late data for [0,60) should be ignored after reportEnd=60 was emitted.
	agg.addBatchToBucket(10, stmtstats.RUIncrementMap{
		{
			User:       "u1",
			SQLDigest:  stmtstats.BinaryDigest("sql-late"),
			PlanDigest: stmtstats.BinaryDigest("plan-late"),
		}: {TotalRU: 999, ExecCount: 1, ExecDuration: 1},
	})
	agg.addBatchToBucket(61, stmtstats.RUIncrementMap{
		{
			User:       "u1",
			SQLDigest:  stmtstats.BinaryDigest("sql-cur"),
			PlanDigest: stmtstats.BinaryDigest("plan-cur"),
		}: {TotalRU: 1, ExecCount: 1, ExecDuration: 1},
	})

	second := agg.takeReportRecords(120, 60, []byte("ks"))
	require.NotEmpty(t, second)
	require.Nil(t, findRURecordByDigest(second, "u1", "sql-late", "plan-late"))
	cur := findRURecordByDigest(second, "u1", "sql-cur", "plan-cur")
	require.NotNil(t, cur)
	require.Len(t, cur.Items, 1)
	require.InDelta(t, 1.0, cur.Items[0].TotalRu, 1e-9)
	// Also guard against "late data hidden in others".
	require.InDelta(t, 1.0, totalRUFromTopRURecords(second), 1e-9)
}

func TestRUWindowAggregator_FinalReportCappedTo100x100(t *testing.T) {
	// Final 60s output must enforce 100 users max and 100 SQL max per retained user.
	agg := newRUWindowAggregator()
	const (
		numUsers       = 120
		numSQLsPerUser = 120
	)
	batch := make(stmtstats.RUIncrementMap, numUsers*numSQLsPerUser)
	for u := 0; u < numUsers; u++ {
		for s := 0; s < numSQLsPerUser; s++ {
			batch[stmtstats.RUKey{
				User:       fmt.Sprintf("u%03d", u),
				SQLDigest:  stmtstats.BinaryDigest(fmt.Sprintf("sql_%03d_%03d", u, s)),
				PlanDigest: stmtstats.BinaryDigest("plan"),
			}] = &stmtstats.RUIncrement{
				// Keep deterministic ranking and avoid ties.
				TotalRU:      float64((numUsers-u)*1000000 + (numSQLsPerUser - s)),
				ExecCount:    1,
				ExecDuration: 1,
			}
		}
	}
	agg.addBatchToBucket(1, batch)

	records := agg.takeReportRecords(60, 60, []byte("ks"))
	require.NotEmpty(t, records)

	realUsers := map[string]int{}
	var othersUserTotalRU float64
	for i := range records {
		rec := records[i]
		if rec.User == keyRUOthersUser && len(rec.SqlDigest) == 0 && len(rec.PlanDigest) == 0 {
			othersUserTotalRU += sumTopRUItems(rec.Items)
			continue
		}
		if rec.User == keyRUOthersUser {
			continue
		}
		if len(rec.SqlDigest) > 0 || len(rec.PlanDigest) > 0 {
			realUsers[rec.User]++
		} else {
			if _, ok := realUsers[rec.User]; !ok {
				realUsers[rec.User] = 0
			}
		}
	}

	require.LessOrEqual(t, len(realUsers), ruReportTopNUsers)
	for user, sqlCount := range realUsers {
		require.LessOrEqual(t, sqlCount, ruReportTopNSQLsPerUser, "user=%s", user)
	}
	require.Greater(t, othersUserTotalRU, 0.0)
}

func TestRUWindowAggregator_RegroupSparseBuckets_NoPhantomPoints(t *testing.T) {
	cases := []struct {
		name       string
		interval   uint64
		expectedTS []uint64
		expectedRU []float64
	}{
		{
			name:       "30s",
			interval:   30,
			expectedTS: []uint64{0, 30},
			expectedRU: []float64{2, 3},
		},
		{
			name:       "60s",
			interval:   60,
			expectedTS: []uint64{0},
			expectedRU: []float64{5},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			agg := newRUWindowAggregator()
			key := stmtstats.RUKey{
				User:       "u-sparse",
				SQLDigest:  stmtstats.BinaryDigest("sql-sparse"),
				PlanDigest: stmtstats.BinaryDigest("plan-sparse"),
			}
			agg.addBatchToBucket(1, stmtstats.RUIncrementMap{
				key: {TotalRU: 2, ExecCount: 1, ExecDuration: 1},
			})
			agg.addBatchToBucket(31, stmtstats.RUIncrementMap{
				key: {TotalRU: 3, ExecCount: 1, ExecDuration: 1},
			})

			records := agg.takeReportRecords(60, tc.interval, []byte("ks"))
			require.Len(t, records, 1)
			rec := findRURecordByDigest(records, "u-sparse", "sql-sparse", "plan-sparse")
			require.NotNil(t, rec)
			require.Len(t, rec.Items, len(tc.expectedTS))
			for i := range tc.expectedTS {
				require.Equal(t, tc.expectedTS[i], rec.Items[i].TimestampSec)
				require.InDelta(t, tc.expectedRU[i], rec.Items[i].TotalRu, 1e-9)
			}
			require.InDelta(t, 5.0, totalRUFromTopRURecords(records), 1e-9)
		})
	}
}

func TestRUWindowAggregator_HotKeySurvivesUnderHighCardinality(t *testing.T) {
	// Under high-cardinality long tail, the hot key should remain visible and not be folded into others.
	agg := newRUWindowAggregator()
	const (
		lowKeyCount = 2000
		hotRU       = 1e9
	)
	hotKey := stmtstats.RUKey{
		User:       "u-hot",
		SQLDigest:  stmtstats.BinaryDigest("sql-hot"),
		PlanDigest: stmtstats.BinaryDigest("plan-hot"),
	}
	for _, ts := range []uint64{1, 16, 31, 46} {
		agg.addBatchToBucket(ts, stmtstats.RUIncrementMap{
			hotKey: {TotalRU: hotRU, ExecCount: 1, ExecDuration: 1},
		})
		for i := 0; i < lowKeyCount; i++ {
			agg.addBatchToBucket(ts, stmtstats.RUIncrementMap{
				{
					User:       "u-hot",
					SQLDigest:  stmtstats.BinaryDigest(fmt.Sprintf("sql-low-%04d", i)),
					PlanDigest: stmtstats.BinaryDigest("plan-low"),
				}: {TotalRU: 1, ExecCount: 1, ExecDuration: 1},
			})
		}
	}

	records := agg.takeReportRecords(60, 60, []byte("ks"))
	require.NotEmpty(t, records)

	hot := findRURecordByDigest(records, "u-hot", "sql-hot", "plan-hot")
	require.NotNil(t, hot)
	require.Len(t, hot.Items, 1)
	require.InDelta(t, hotRU*4, hot.Items[0].TotalRu, 1e-6)

	others := findRURecordByDigest(records, "u-hot", "", "")
	require.NotNil(t, others)
	othersTotal := sumTopRUItems(others.Items)
	require.Greater(t, othersTotal, 0.0)
	require.LessOrEqual(t, othersTotal, float64(lowKeyCount*4))

	require.InDelta(t, hotRU*4+float64(lowKeyCount*4), totalRUFromTopRURecords(records), 1e-3)
}

func sumTopRUItems(items []*tipb.TopRURecordItem) float64 {
	total := 0.0
	for _, item := range items {
		total += item.TotalRu
	}
	return total
}

func totalRUFromTopRURecords(records []tipb.TopRURecord) float64 {
	total := 0.0
	for _, rec := range records {
		total += sumTopRUItems(rec.Items)
	}
	return total
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

type ruBenchCase struct {
	name           string
	numUsers       int
	numSQLsPerUser int
	itemInterval   uint64
}

func benchmarkRUFillOnly(b *testing.B, c ruBenchCase) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, c.numUsers, c.numSQLsPerUser)
	}
}

func benchmarkRUFillAndTake(b *testing.B, c ruBenchCase) {
	b.Helper()
	keyspace := []byte("ks")
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, c.numUsers, c.numSQLsPerUser)
		_ = agg.takeReportRecords(60, c.itemInterval, keyspace)
	}
}

func benchmarkRUTakeOnly(b *testing.B, c ruBenchCase) {
	b.Helper()
	keyspace := []byte("ks")
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, c.numUsers, c.numSQLsPerUser)
		b.StartTimer()
		_ = agg.takeReportRecords(60, c.itemInterval, keyspace)
		b.StopTimer()
	}
}

// BenchmarkRUWindowAggregatorMatrix provides a unified benchmark matrix for
// comparing cost curves under different cardinalities and measurement modes.
// - fill-only: pre-aggregation pressure
// - take-only: report extraction pressure
// - fill+take: end-to-end periodic cost
func BenchmarkRUWindowAggregatorMatrix(b *testing.B) {
	cases := []ruBenchCase{
		{name: "200x200_60s", numUsers: 200, numSQLsPerUser: 200, itemInterval: 60},
		{name: "1000x500_60s", numUsers: 1000, numSQLsPerUser: 500, itemInterval: 60},
	}
	for _, c := range cases {
		c := c
		b.Run(c.name, func(b *testing.B) {
			b.Run("fill-only", func(b *testing.B) {
				benchmarkRUFillOnly(b, c)
			})
			b.Run("take-only", func(b *testing.B) {
				benchmarkRUTakeOnly(b, c)
			})
			b.Run("fill+take", func(b *testing.B) {
				benchmarkRUFillAndTake(b, c)
			})
		})
	}
}

// Benchmarks for takeReportRecords. Each iteration does fill + takeReportRecords
// (take consumes buckets, so we must re-fill).
// Use -bench=TakeReportRecords -benchmem to compare ns/op and B/op.

func BenchmarkTakeReportRecords_Small_60s(b *testing.B) {
	keyspace := []byte("ks")
	const numUsers, numSQLsPerUser = 10, 10
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, numUsers, numSQLsPerUser)
		_ = agg.takeReportRecords(60, 60, keyspace)
	}
}

func BenchmarkTakeReportRecords_Medium_60s(b *testing.B) {
	keyspace := []byte("ks")
	const numUsers, numSQLsPerUser = 100, 100
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, numUsers, numSQLsPerUser)
		_ = agg.takeReportRecords(60, 60, keyspace)
	}
}

func BenchmarkTakeReportRecords_Large_60s(b *testing.B) {
	keyspace := []byte("ks")
	const numUsers, numSQLsPerUser = 200, 200
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, numUsers, numSQLsPerUser)
		_ = agg.takeReportRecords(60, 60, keyspace)
	}
}

// BenchmarkTakeReportRecords_Large_60s_AtDesignLimit measures takeReportRecords(60, 60) when the
// aggregator is in the 60s design-limit shape: 1 bucket collecting at 400×400 cap and 4 buckets
// already compacted to 200×200. Run with -benchmem for B/op and allocs/op.
func BenchmarkTakeReportRecords_Large_60s_AtDesignLimit(b *testing.B) {
	keyspace := []byte("ks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorSteadyState60s(agg)
		_ = agg.takeReportRecords(60, 60, keyspace)
	}
}

// BenchmarkTakeReportRecords_Large_60s_At10kKeys measures takeReportRecords(60, 60) with 10k keys
// per bucket (4 compacted + 1 collecting), simulating maxRUKeysPerAggregate=10000. Compare with
// BenchmarkTakeReportRecords_Large_60s_AtDesignLimit (160k keys) for 10k vs 160k reporter load.
func BenchmarkTakeReportRecords_Large_60s_At10kKeys(b *testing.B) {
	keyspace := []byte("ks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorSteadyState60sAt10kKeys(agg)
		_ = agg.takeReportRecords(60, 60, keyspace)
	}
}

func BenchmarkTakeReportRecords_Medium_15s(b *testing.B) {
	keyspace := []byte("ks")
	const numUsers, numSQLsPerUser = 100, 100
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, numUsers, numSQLsPerUser)
		_ = agg.takeReportRecords(60, 15, keyspace)
	}
}

// BenchmarkFillAggregatorOnly_Medium measures only the cost to fill the aggregator (no takeReportRecords).
// takeReportRecords cost ≈ (BenchmarkTakeReportRecords_Medium_60s time) - (this benchmark time).
func BenchmarkFillAggregatorOnly_Medium(b *testing.B) {
	const numUsers, numSQLsPerUser = 100, 100
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, numUsers, numSQLsPerUser)
		_ = agg
	}
}

func BenchmarkTakeReportRecords_HotKey_15s(b *testing.B) {
	keyspace := []byte("ks")
	key := stmtstats.RUKey{
		User:       "u_hot",
		SQLDigest:  stmtstats.BinaryDigest("sql_hot"),
		PlanDigest: stmtstats.BinaryDigest("plan_hot"),
	}
	batch := stmtstats.RUIncrementMap{
		key: {TotalRU: 1, ExecCount: 1, ExecDuration: 1},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		agg.addBatchToBucket(1, batch)
		agg.addBatchToBucket(16, batch)
		agg.addBatchToBucket(31, batch)
		agg.addBatchToBucket(46, batch)
		_ = agg.takeReportRecords(60, 15, keyspace)
	}
}

func BenchmarkTakeReportRecords_OverUserCap_60s(b *testing.B) {
	keyspace := []byte("ks")
	const numUsers, numSQLsPerUser = 600, 10 // exceed maxPreTopNUsers (400)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, numUsers, numSQLsPerUser)
		_ = agg.takeReportRecords(60, 60, keyspace)
	}
}

func BenchmarkTakeReportRecords_OverSQLCapPerUser_60s(b *testing.B) {
	keyspace := []byte("ks")
	const numUsers, numSQLsPerUser = 10, 600 // exceed maxPreTopNSQLsPerUser (400)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		fillAggregatorForWindow(agg, 60, numUsers, numSQLsPerUser)
		_ = agg.takeReportRecords(60, 60, keyspace)
	}
}

func BenchmarkTakeReportRecords_LateTake_DropsOldWindows(b *testing.B) {
	keyspace := []byte("ks")
	const numUsers, numSQLsPerUser = 100, 100
	batch := makeRUBatch(numUsers, numSQLsPerUser)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := newRUWindowAggregator()
		// Setup: write 3 windows and report only the last window.
		for ts := uint64(1); ts < 180; ts += 15 {
			agg.addBatchToBucket(ts, batch)
		}
		_ = agg.takeReportRecords(180, 60, keyspace)
	}
}
